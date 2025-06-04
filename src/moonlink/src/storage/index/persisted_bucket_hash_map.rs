use crate::storage::storage_utils::{MooncakeDataFileRef, RecordLocation};
use futures::executor::block_on;
use memmap2::Mmap;
use std::collections::BinaryHeap;
use std::fmt;
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::io::Cursor;
use std::io::SeekFrom;
use std::path::PathBuf;
use std::sync::Arc;
use tokio::fs::File as AsyncFile;
use tokio::io::BufWriter as AsyncBufWriter;
use tokio_bitstream_io::{
    BigEndian as AsyncBigEndian, BitRead as AsyncBitRead, BitReader as AsyncBitReader,
    BitWrite as AsyncBitWrite, BitWriter as AsyncBitWriter,
};

// Constants
const HASH_BITS: u32 = 64;
const _MAX_BLOCK_SIZE: u32 = 2 * 1024 * 1024 * 1024; // 2GB
const _TARGET_NUM_FILES_PER_INDEX: u32 = 4000;
const _INVALID_FILE_ID: u32 = 0xFFFFFFFF;

fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E3779B97F4A7C15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
    z ^ (z >> 31)
}
/// Hash index
/// that maps a u64 to [seg_idx, row_idx]
///
/// Structure:
/// Buckets:
/// [entry_offset],[entry_offset]...[entry_offset]
///
/// Values
/// [lower_bit_hash, seg_idx, row_idx]
#[derive(Clone)]
pub struct GlobalIndex {
    pub(crate) files: Vec<MooncakeDataFileRef>,
    pub(crate) num_rows: u32,
    pub(crate) hash_bits: u32,
    pub(crate) hash_upper_bits: u32,
    pub(crate) hash_lower_bits: u32,
    pub(crate) seg_id_bits: u32,
    pub(crate) row_id_bits: u32,
    pub(crate) bucket_bits: u32,

    pub(crate) index_blocks: Vec<IndexBlock>,
}

// For GlobalIndex, there won't be two indices pointing to same sets of data files, so we use data files for hash and equal.
impl PartialEq for GlobalIndex {
    fn eq(&self, other: &Self) -> bool {
        self.files == other.files
    }
}

impl Eq for GlobalIndex {}

impl Hash for GlobalIndex {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.files.hash(state);
    }
}

#[derive(Clone)]
pub(crate) struct IndexBlock {
    pub(crate) bucket_start_idx: u32,
    pub(crate) bucket_end_idx: u32,
    pub(crate) bucket_start_offset: u64,
    pub(crate) file_path: String,
    data: Arc<Option<Mmap>>,
}

impl IndexBlock {
    pub(crate) async fn new(
        bucket_start_idx: u32,
        bucket_end_idx: u32,
        bucket_start_offset: u64,
        file_path: String,
    ) -> Self {
        let file = tokio::fs::File::open(file_path.clone()).await.unwrap();
        let data = unsafe { Mmap::map(&file).unwrap() };
        Self {
            bucket_start_idx,
            bucket_end_idx,
            bucket_start_offset,
            file_path,
            data: Arc::new(Some(data)),
        }
    }

    async fn _create_iterator<'a>(
        &'a self,
        metadata: &'a GlobalIndex,
        file_id_remap: &'a Vec<u32>,
    ) -> IndexBlockIterator<'a> {
        IndexBlockIterator::_new(self, metadata, file_id_remap).await
    }

    #[inline]
    async fn read_bucket(
        &self,
        bucket_idx: u32,
        reader: &mut AsyncBitReader<Cursor<&[u8]>, AsyncBigEndian>,
        metadata: &GlobalIndex,
    ) -> (u32, u32) {
        reader
            .seek_bits(SeekFrom::Start(
                (bucket_idx * metadata.bucket_bits) as u64 + self.bucket_start_offset,
            ))
            .await
            .unwrap();
        let start = reader.read::<u32>(metadata.bucket_bits).await.unwrap();
        let end = reader.read::<u32>(metadata.bucket_bits).await.unwrap();
        (start, end)
    }

    #[inline]
    async fn read_entry(
        &self,
        reader: &mut AsyncBitReader<Cursor<&[u8]>, AsyncBigEndian>,
        metadata: &GlobalIndex,
    ) -> (u64, usize, usize) {
        let hash = reader.read::<u64>(metadata.hash_lower_bits).await.unwrap();
        let seg_idx = reader.read::<u32>(metadata.seg_id_bits).await.unwrap();
        let row_idx = reader.read::<u32>(metadata.row_id_bits).await.unwrap();
        (hash, seg_idx as usize, row_idx as usize)
    }

    async fn read(
        &self,
        target_lower_hash: u64,
        bucket_idx: u32,
        metadata: &GlobalIndex,
    ) -> Vec<RecordLocation> {
        assert!(bucket_idx >= self.bucket_start_idx && bucket_idx < self.bucket_end_idx);
        let cursor = Cursor::new(self.data.as_ref().as_ref().unwrap().as_ref());
        let mut reader = AsyncBitReader::endian(cursor, AsyncBigEndian);
        let mut entry_reader = reader.clone();
        let (entry_start, entry_end) = self.read_bucket(bucket_idx, &mut reader, metadata).await;
        if entry_start != entry_end {
            let mut results = Vec::new();
            entry_reader
                .seek_bits(SeekFrom::Start(
                    entry_start as u64
                        * (metadata.hash_lower_bits + metadata.seg_id_bits + metadata.row_id_bits)
                            as u64,
                ))
                .await
                .unwrap();
            for _i in entry_start..entry_end {
                let (hash, seg_idx, row_idx) = self.read_entry(&mut entry_reader, metadata).await;
                if hash == target_lower_hash {
                    results.push(RecordLocation::DiskFile(
                        metadata.files[seg_idx].file_id(),
                        row_idx,
                    ));
                }
            }
            results
        } else {
            vec![]
        }
    }
}

impl GlobalIndex {
    pub async fn search(&self, value: &u64) -> Vec<RecordLocation> {
        let target_hash = splitmix64(*value);
        let lower_hash = target_hash & ((1 << self.hash_lower_bits) - 1);
        let bucket_idx = (target_hash >> self.hash_lower_bits) as u32;
        for block in self.index_blocks.iter() {
            if bucket_idx >= block.bucket_start_idx && bucket_idx < block.bucket_end_idx {
                return block.read(lower_hash, bucket_idx, self).await;
            }
        }
        vec![]
    }

    pub async fn _create_iterator<'a>(
        &'a self,
        file_id_remap: &'a Vec<u32>,
    ) -> GlobalIndexIterator<'a> {
        GlobalIndexIterator::_new(self, file_id_remap).await
    }
}

// ================================
// Builders
// ================================
struct IndexBlockBuilder {
    bucket_start_idx: u32,
    bucket_end_idx: u32,
    buckets: Vec<u32>,
    file_path: PathBuf,
    entry_writer: AsyncBitWriter<AsyncBufWriter<AsyncFile>, AsyncBigEndian>,
    current_bucket: u32,
    current_entry: u32,
}

/// TODO(hjiang): Error handle for all IO operations.
impl IndexBlockBuilder {
    pub async fn new(bucket_start_idx: u32, bucket_end_idx: u32, directory: PathBuf) -> Self {
        let file_name = format!("index_block_{}.bin", uuid::Uuid::new_v4());
        let file_path = directory.join(&file_name);

        let file = AsyncFile::create(&file_path).await.unwrap();
        let buf_writer = AsyncBufWriter::new(file);
        let entry_writer = AsyncBitWriter::endian(buf_writer, AsyncBigEndian);

        Self {
            bucket_start_idx,
            bucket_end_idx,
            buckets: vec![0; (bucket_end_idx - bucket_start_idx) as usize],
            file_path,
            entry_writer,
            current_bucket: bucket_start_idx,
            current_entry: 0,
        }
    }

    pub async fn write_entry(
        &mut self,
        hash: u64,
        seg_idx: usize,
        row_idx: usize,
        metadata: &GlobalIndex,
    ) {
        while (hash >> metadata.hash_lower_bits) != self.current_bucket as u64 {
            self.current_bucket += 1;
            self.buckets[self.current_bucket as usize] = self.current_entry;
        }
        self.entry_writer
            .write(
                metadata.hash_lower_bits,
                hash & ((1 << metadata.hash_lower_bits) - 1),
            )
            .await
            .unwrap();
        self.entry_writer
            .write(metadata.seg_id_bits, seg_idx as u32)
            .await
            .unwrap();
        self.entry_writer
            .write(metadata.row_id_bits, row_idx as u32)
            .await
            .unwrap();
        self.current_entry += 1;
    }

    pub async fn build(mut self, metadata: &GlobalIndex) -> IndexBlock {
        for i in self.current_bucket + 1..self.bucket_end_idx {
            self.buckets[i as usize] = self.current_entry;
        }
        let bucket_start_offset = (self.current_entry as u64)
            * (metadata.hash_lower_bits + metadata.seg_id_bits + metadata.row_id_bits) as u64;
        let buckets = std::mem::take(&mut self.buckets);
        for cur_bucket in buckets {
            self.entry_writer
                .write(metadata.bucket_bits, cur_bucket)
                .await
                .unwrap();
        }
        self.entry_writer.byte_align().await.unwrap();
        self.entry_writer.flush().await.unwrap();
        drop(self.entry_writer);
        IndexBlock::new(
            self.bucket_start_idx,
            self.bucket_end_idx,
            bucket_start_offset,
            self.file_path.to_str().unwrap().to_string(),
        )
        .await
    }
}

pub struct GlobalIndexBuilder {
    num_rows: u32,
    files: Vec<MooncakeDataFileRef>,
    directory: PathBuf,
}

impl Default for GlobalIndexBuilder {
    fn default() -> Self {
        Self::new()
    }
}

impl GlobalIndexBuilder {
    pub fn new() -> Self {
        Self {
            num_rows: 0,
            files: vec![],
            directory: PathBuf::new(),
        }
    }

    pub fn set_directory(&mut self, directory: PathBuf) -> &mut Self {
        self.directory = directory;
        self
    }

    pub fn set_files(&mut self, files: Vec<MooncakeDataFileRef>) -> &mut Self {
        self.files = files;
        self
    }

    // Util function to build global index.
    fn create_global_index(&mut self) -> (u32, GlobalIndex) {
        let num_rows = self.num_rows;
        let bucket_bits = 32 - num_rows.leading_zeros();
        let num_buckets = (num_rows / 4 + 2).next_power_of_two();
        let upper_bits = num_buckets.trailing_zeros();
        let lower_bits = 64 - upper_bits;
        let seg_id_bits = 32 - (self.files.len() as u32).trailing_zeros();
        let global_index = GlobalIndex {
            files: std::mem::take(&mut self.files),
            num_rows,
            hash_bits: HASH_BITS,
            hash_upper_bits: upper_bits,
            hash_lower_bits: lower_bits,
            seg_id_bits,
            row_id_bits: 32,
            bucket_bits,
            index_blocks: vec![],
        };
        (num_buckets, global_index)
    }

    // ================================
    // Build from flush
    // ================================
    pub async fn build_from_flush(mut self, mut entries: Vec<(u64, usize, usize)>) -> GlobalIndex {
        self.num_rows = entries.len() as u32;
        for entry in &mut entries {
            entry.0 = splitmix64(entry.0);
        }
        entries.sort_by_key(|entry| entry.0);
        self.build(entries.into_iter()).await
    }

    async fn build(mut self, iter: impl Iterator<Item = (u64, usize, usize)>) -> GlobalIndex {
        let (num_buckets, mut global_index) = self.create_global_index();
        let mut index_blocks = Vec::new();
        let mut index_block_builder =
            IndexBlockBuilder::new(0, num_buckets + 1, self.directory.clone()).await;
        for entry in iter {
            index_block_builder
                .write_entry(entry.0, entry.1, entry.2, &global_index)
                .await;
        }
        index_blocks.push(index_block_builder.build(&global_index).await);
        global_index.index_blocks = index_blocks;
        global_index
    }

    // ================================
    // Build from merge
    // ================================
    pub async fn _build_from_merge(mut self, indices: Vec<GlobalIndex>) -> GlobalIndex {
        self.num_rows = indices.iter().map(|index| index.num_rows).sum();
        self.files = indices
            .iter()
            .flat_map(|index| index.files.clone())
            .collect();
        let mut file_id_remaps = vec![];
        let mut file_id_after_remap = 0;
        for index in &indices {
            let mut file_id_remap = vec![_INVALID_FILE_ID; index.files.len()];
            for (_, item) in file_id_remap.iter_mut().enumerate().take(index.files.len()) {
                *item = file_id_after_remap;
                file_id_after_remap += 1;
            }
            file_id_remaps.push(file_id_remap);
        }
        let mut iters = Vec::with_capacity(indices.len());
        for (idx, index) in indices.iter().enumerate() {
            iters.push(index._create_iterator(&file_id_remaps[idx]).await);
        }
        let merge_iter = GlobalIndexMergingIterator::_new(iters).await;
        self._build_from_merging_iterator(merge_iter).await
    }

    async fn _build_from_merging_iterator(
        mut self,
        mut iter: GlobalIndexMergingIterator<'_>,
    ) -> GlobalIndex {
        let (num_buckets, mut global_index) = self.create_global_index();
        let mut index_blocks = Vec::new();
        let mut index_block_builder =
            IndexBlockBuilder::new(0, num_buckets + 1, self.directory.clone()).await;
        while let Some(entry) = iter._next().await {
            index_block_builder
                .write_entry(entry.0, entry.1, entry.2, &global_index)
                .await;
        }
        index_blocks.push(index_block_builder.build(&global_index).await);
        global_index.index_blocks = index_blocks;
        global_index
    }
}

// ================================
// Iterators for merging indices
// ================================
#[allow(dead_code)]
struct IndexBlockIterator<'a> {
    collection: &'a IndexBlock,
    metadata: &'a GlobalIndex,
    current_bucket: u32,
    current_bucket_entry_end: u32,
    current_entry: u32,
    current_upper_hash: u64,
    bucket_reader: AsyncBitReader<Cursor<&'a [u8]>, AsyncBigEndian>,
    entry_reader: AsyncBitReader<Cursor<&'a [u8]>, AsyncBigEndian>,
    file_id_remap: &'a Vec<u32>,
}

impl<'a> IndexBlockIterator<'a> {
    async fn _new(
        collection: &'a IndexBlock,
        metadata: &'a GlobalIndex,
        file_id_remap: &'a Vec<u32>,
    ) -> Self {
        let mut bucket_reader = AsyncBitReader::endian(
            Cursor::new(collection.data.as_ref().as_ref().unwrap().as_ref()),
            AsyncBigEndian,
        );
        let entry_reader = bucket_reader.clone();
        bucket_reader
            .seek_bits(SeekFrom::Start(collection.bucket_start_offset))
            .await
            .unwrap();
        let _ = bucket_reader
            .read::<u32>(metadata.bucket_bits)
            .await
            .unwrap();
        let current_bucket_entry_end = bucket_reader
            .read::<u32>(metadata.bucket_bits)
            .await
            .unwrap();
        Self {
            collection,
            metadata,
            bucket_reader,
            entry_reader,
            current_bucket: collection.bucket_start_idx,
            current_bucket_entry_end,
            current_entry: 0,
            current_upper_hash: 0,
            file_id_remap,
        }
    }

    async fn _next(&mut self) -> Option<(u64, usize, usize)> {
        loop {
            if self.current_bucket == self.collection.bucket_end_idx - 1 {
                return None;
            }
            while self.current_entry == self.current_bucket_entry_end {
                self.current_bucket += 1;
                if self.current_bucket == self.collection.bucket_end_idx - 1 {
                    return None;
                }
                self.current_bucket_entry_end = self
                    .bucket_reader
                    .read::<u32>(self.metadata.bucket_bits)
                    .await
                    .unwrap();
                self.current_upper_hash += 1 << self.metadata.hash_lower_bits;
            }
            let (lower_hash, seg_idx, row_idx) = self
                .collection
                .read_entry(&mut self.entry_reader, self.metadata)
                .await;
            self.current_entry += 1;
            if *self.file_id_remap.get(seg_idx).unwrap() != _INVALID_FILE_ID {
                return Some((lower_hash + self.current_upper_hash, seg_idx, row_idx));
            }
        }
    }
}

#[allow(dead_code)]
pub struct GlobalIndexIterator<'a> {
    index: &'a GlobalIndex,
    block_idx: usize,
    block_iter: Option<IndexBlockIterator<'a>>,
    file_id_remap: &'a Vec<u32>,
}

impl<'a> GlobalIndexIterator<'a> {
    pub async fn _new(index: &'a GlobalIndex, file_id_remap: &'a Vec<u32>) -> Self {
        let mut block_iter = None;
        let block_idx = 0;
        if !index.index_blocks.is_empty() {
            block_iter = Some(
                index.index_blocks[0]
                    ._create_iterator(index, file_id_remap)
                    .await,
            );
        }
        Self {
            index,
            block_idx,
            block_iter,
            file_id_remap,
        }
    }

    pub async fn _next(&mut self) -> Option<(u64, usize, usize)> {
        loop {
            if let Some(ref mut iter) = self.block_iter {
                if let Some(item) = iter._next().await {
                    return Some(item);
                }
            }
            self.block_idx += 1;
            if self.block_idx >= self.index.index_blocks.len() {
                return None;
            }
            self.block_iter = Some(
                self.index.index_blocks[self.block_idx]
                    ._create_iterator(self.index, self.file_id_remap)
                    .await,
            );
        }
    }
}

#[allow(dead_code)]
pub struct GlobalIndexMergingIterator<'a> {
    heap: BinaryHeap<HeapItem<'a>>,
}

#[allow(dead_code)]
struct HeapItem<'a> {
    value: (u64, usize, usize),
    iter: GlobalIndexIterator<'a>,
}

impl PartialEq for HeapItem<'_> {
    fn eq(&self, other: &Self) -> bool {
        self.value.0 == other.value.0
    }
}
impl Eq for HeapItem<'_> {}

impl PartialOrd for HeapItem<'_> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        Some(self.cmp(other))
    }
}
impl Ord for HeapItem<'_> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Reverse for min-heap
        other.value.0.cmp(&self.value.0)
    }
}

impl<'a> GlobalIndexMergingIterator<'a> {
    pub async fn _new(iterators: Vec<GlobalIndexIterator<'a>>) -> Self {
        let mut heap = BinaryHeap::new();
        for mut it in iterators {
            if let Some(value) = it._next().await {
                heap.push(HeapItem { value, iter: it });
            }
        }
        Self { heap }
    }

    pub async fn _next(&mut self) -> Option<(u64, usize, usize)> {
        if let Some(mut heap_item) = self.heap.pop() {
            let result = heap_item.value;
            if let Some(next_value) = heap_item.iter._next().await {
                self.heap.push(HeapItem {
                    value: next_value,
                    iter: heap_item.iter,
                });
            }
            Some(result)
        } else {
            None
        }
    }
}

// ================================
// Debug Helpers
// ================================
impl IndexBlock {
    fn fmt(&self, f: &mut fmt::Formatter<'_>, metadata: &GlobalIndex) -> fmt::Result {
        write!(
            f,
            "\nIndexBlock {{ \n   bucket_start_idx: {}, \n   bucket_end_idx: {},",
            self.bucket_start_idx, self.bucket_end_idx
        )?;
        let cursor = Cursor::new(self.data.as_ref().as_ref().unwrap().as_ref());
        let mut reader = AsyncBitReader::endian(cursor, AsyncBigEndian);
        write!(f, "\n   Buckets: ")?;
        let mut num = 0;
        block_on(reader.seek_bits(SeekFrom::Start(self.bucket_start_offset))).unwrap();
        for _i in 0..self.bucket_end_idx {
            num = block_on(reader.read::<u32>(metadata.bucket_bits)).unwrap();
            write!(f, "{} ", num)?;
        }
        write!(f, "\n   Entries: ")?;
        block_on(reader.seek_bits(SeekFrom::Start(0))).unwrap();
        for _i in 0..num {
            let (hash, seg_idx, row_idx) = block_on(self.read_entry(&mut reader, metadata));
            write!(f, "\n     {} {} {}", hash, seg_idx, row_idx)?;
        }
        write!(f, "\n}}")?;
        Ok(())
    }
}

impl Debug for GlobalIndex {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "GlobalIndex {{ files: {:?}, num_rows: {}, hash_bits: {}, hash_upper_bits: {}, hash_lower_bits: {}, seg_id_bits: {}, row_id_bits: {}, bucket_bits: {} ", self.files, self.num_rows, self.hash_bits, self.hash_upper_bits, self.hash_lower_bits, self.seg_id_bits, self.row_id_bits, self.bucket_bits)?;
        for block in &self.index_blocks {
            block.fmt(f, self)?;
        }
        write!(f, "}}")?;
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use std::vec;

    use super::*;

    use crate::storage::storage_utils::{create_data_file, FileId};

    #[tokio::test]
    async fn test_new() {
        let data_file = create_data_file(/*file_id=*/ 0, "a.parquet".to_string());
        let files = vec![data_file.clone()];
        let hash_entries = vec![
            (1, 0, 0),
            (2, 0, 1),
            (3, 0, 2),
            (4, 0, 3),
            (5, 0, 4),
            (16, 0, 5),
            (214141, 0, 6),
            (2141, 0, 7),
            (21141, 0, 8),
            (219511, 0, 9),
            (1421141, 0, 10),
            (1111111141, 0, 11),
            (99999, 0, 12),
        ];
        let mut builder = GlobalIndexBuilder::new();
        builder
            .set_files(files)
            .set_directory(tempfile::tempdir().unwrap().keep());
        let index = builder.build_from_flush(hash_entries.clone()).await;

        // Search for a non-existent key doesn't panic.
        assert!(index.search(/*hash=*/ &0).await.is_empty());

        let data_file_ids = [data_file.file_id()];
        for (hash, seg_idx, row_idx) in hash_entries.iter() {
            let expected_record_loc = RecordLocation::DiskFile(data_file_ids[*seg_idx], *row_idx);
            assert_eq!(index.search(hash).await, vec![expected_record_loc]);
        }

        let mut hash_entry_num = 0;
        let file_id_remap = vec![0; index.files.len()];
        for block in index.index_blocks.iter() {
            let mut index_block_iter = block._create_iterator(&index, &file_id_remap).await;
            while let Some((hash, seg_idx, row_idx)) = index_block_iter._next().await {
                println!("{} {} {}", hash, seg_idx, row_idx);
                hash_entry_num += 1;
            }
        }
        // Check all hash entries are stored and iterated through via index iterator.
        assert_eq!(hash_entry_num, hash_entries.len());
    }

    #[tokio::test]
    async fn test_merge() {
        let files = vec![
            create_data_file(/*file_id=*/ 1, "1.parquet".to_string()),
            create_data_file(/*file_id=*/ 2, "2.parquet".to_string()),
            create_data_file(/*file_id=*/ 3, "3.parquet".to_string()),
        ];
        let vec = (0..100).map(|i| (i as u64, i % 3, i)).collect::<Vec<_>>();
        let mut builder = GlobalIndexBuilder::new();
        builder
            .set_files(files)
            .set_directory(tempfile::tempdir().unwrap().keep());
        let index1 = builder.build_from_flush(vec).await;
        let files = vec![
            create_data_file(/*file_id=*/ 4, "4.parquet".to_string()),
            create_data_file(/*file_id=*/ 5, "5.parquet".to_string()),
        ];
        let vec = (100..200).map(|i| (i as u64, i % 2, i)).collect::<Vec<_>>();
        let mut builder = GlobalIndexBuilder::new();
        builder
            .set_files(files)
            .set_directory(tempfile::tempdir().unwrap().keep());
        let index2 = builder.build_from_flush(vec).await;
        let mut builder = GlobalIndexBuilder::new();
        builder.set_directory(tempfile::tempdir().unwrap().keep());
        let merged = builder._build_from_merge(vec![index1, index2]).await;

        for i in 0u64..100u64 {
            let ret = merged.search(&i).await;
            let record_location = ret.first().unwrap();
            let RecordLocation::DiskFile(FileId(file_id), _) = record_location else {
                panic!("No record location found for {}", i);
            };
            assert_eq!(*file_id, i % 3 + 1);
        }
    }
}
