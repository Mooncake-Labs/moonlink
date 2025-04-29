use crate::storage::storage_utils::{FileId, RecordLocation};
use bitstream_io::{BigEndian, BitRead, BitReader, BitWrite, BitWriter};
use memmap2::Mmap;
use std::collections::BinaryHeap;
use std::fmt;
use std::fmt::Debug;
use std::fs::File;
use std::io::BufWriter;
use std::io::Cursor;
use std::io::SeekFrom;
use std::path::PathBuf;
use std::sync::Arc;

// Constants
const HASH_BITS: u32 = 64;
const _MAX_BLOCK_SIZE: u32 = 2 * 1024 * 1024 * 1024; // 2GB
const _TARGET_NUM_FILES_PER_INDEX: u32 = 4000;
const INVALID_FILE_ID: u32 = 0xFFFFFFFF;

fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E3779B97F4A7C15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
    z ^ (z >> 31)
}
// Hash index
// that maps a u64 to [seg_idx, row_idx]
//
// Structure:
// Buckets:
// [entry_offset],[entry_offset]...[entry_offset]
//
// Values
// [lower_bit_hash, seg_idx, row_idx]
pub struct GlobalIndex {
    files: Vec<Arc<PathBuf>>,
    num_rows: u32,
    hash_bits: u32,
    hash_upper_bits: u32,
    hash_lower_bits: u32,
    seg_id_bits: u32,
    row_id_bits: u32,
    bucket_bits: u32,

    index_blocks: Vec<IndexBlock>,
}

struct IndexBlock {
    bucket_start_idx: u32,
    bucket_end_idx: u32,
    bucket_start_offset: u64,
    _file_name: String,
    data: Mmap,
}

impl IndexBlock {
    fn new(
        bucket_start_idx: u32,
        bucket_end_idx: u32,
        bucket_start_offset: u64,
        directory: &PathBuf,
        file_name: String,
    ) -> Self {
        let file = File::open(directory.join(&file_name)).unwrap();
        let data = unsafe { Mmap::map(&file).unwrap() };
        Self {
            bucket_start_idx,
            bucket_end_idx,
            _file_name: file_name,
            bucket_start_offset,
            data,
        }
    }

    fn iter<'a>(
        self: &'a Self,
        metadata: &'a GlobalIndex,
        file_id_remap: &'a Vec<u32>,
    ) -> IndexBlockIterator<'a> {
        IndexBlockIterator::new(self, metadata, file_id_remap)
    }

    #[inline]
    fn read_bucket(
        self: &Self,
        bucket_idx: u32,
        reader: &mut BitReader<Cursor<&[u8]>, BigEndian>,
        metadata: &GlobalIndex,
    ) -> (u32, u32) {
        reader
            .seek_bits(SeekFrom::Start(
                (bucket_idx * metadata.bucket_bits) as u64 + self.bucket_start_offset,
            ))
            .unwrap();
        let start = reader
            .read_unsigned_var::<u32>(metadata.bucket_bits)
            .unwrap();
        let end = reader
            .read_unsigned_var::<u32>(metadata.bucket_bits)
            .unwrap();
        (start, end)
    }

    #[inline]
    fn read_entry(
        self: &Self,
        reader: &mut BitReader<Cursor<&[u8]>, BigEndian>,
        metadata: &GlobalIndex,
    ) -> (u64, usize, usize) {
        let hash = reader
            .read_unsigned_var::<u64>(metadata.hash_lower_bits)
            .unwrap();
        let seg_idx = reader
            .read_unsigned_var::<u32>(metadata.seg_id_bits)
            .unwrap();
        let row_idx = reader
            .read_unsigned_var::<u32>(metadata.row_id_bits)
            .unwrap();
        (hash, seg_idx as usize, row_idx as usize)
    }

    fn read(
        self: &Self,
        target_lower_hash: u64,
        bucket_idx: u32,
        metadata: &GlobalIndex,
    ) -> Vec<RecordLocation> {
        assert!(bucket_idx >= self.bucket_start_idx && bucket_idx < self.bucket_end_idx);
        let cursor = Cursor::new(self.data.as_ref());
        let mut reader = BitReader::endian(cursor, BigEndian);
        let mut entry_reader = reader.clone();
        let (entry_start, entry_end) = self.read_bucket(bucket_idx, &mut reader, metadata);
        if entry_start != entry_end {
            let mut results = Vec::new();
            entry_reader
                .seek_bits(SeekFrom::Start(
                    entry_start as u64
                        * (metadata.hash_lower_bits + metadata.seg_id_bits + metadata.row_id_bits)
                            as u64,
                ))
                .unwrap();
            for _i in entry_start..entry_end {
                let (hash, seg_idx, row_idx) = self.read_entry(&mut entry_reader, metadata);
                if hash == target_lower_hash {
                    results.push(RecordLocation::DiskFile(
                        FileId(metadata.files[seg_idx].clone()),
                        row_idx as usize,
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
    pub fn search(&self, value: &u64) -> Vec<RecordLocation> {
        let target_hash = splitmix64(*value);
        let lower_hash = target_hash & ((1 << self.hash_lower_bits) - 1);
        let bucket_idx = (target_hash >> self.hash_lower_bits) as u32;
        for block in self.index_blocks.iter() {
            if bucket_idx >= block.bucket_start_idx && bucket_idx < block.bucket_end_idx {
                return block.read(lower_hash, bucket_idx, self);
            }
        }
        vec![]
    }

    pub fn _iter<'a>(&'a self, file_id_remap: &'a Vec<u32>) -> GlobalIndexIterator<'a> {
        GlobalIndexIterator::_new(self, file_id_remap)
    }
}

// ================================
// Builders
// ================================
struct IndexBlockBuilder {
    bucket_start_idx: u32,
    bucket_end_idx: u32,
    buckets: Vec<u32>,
    file_name: String,
    directory: PathBuf,
    entry_writer: BitWriter<BufWriter<File>, BigEndian>,
    current_bucket: u32,
    current_entry: u32,
}

impl IndexBlockBuilder {
    pub fn new(bucket_start_idx: u32, bucket_end_idx: u32, directory: PathBuf) -> Self {
        let file_name = format!("index_block_{}.bin", uuid::Uuid::new_v4());
        let file = File::create(&directory.join(&file_name)).unwrap();
        let buf_writer = BufWriter::new(file);
        let entry_writer = BitWriter::endian(buf_writer, BigEndian);
        Self {
            file_name,
            bucket_start_idx,
            bucket_end_idx,
            buckets: vec![0; (bucket_end_idx - bucket_start_idx) as usize],
            directory,
            entry_writer,
            current_bucket: bucket_start_idx,
            current_entry: 0,
        }
    }

    pub fn write_entry(
        &mut self,
        hash: u64,
        seg_idx: usize,
        row_idx: usize,
        metadata: &GlobalIndex,
    ) {
        while (hash >> metadata.hash_lower_bits) != self.current_bucket as u64 {
            self.current_bucket += 1;
            self.buckets[self.current_bucket as usize] = self.current_entry as u32;
        }
        self.entry_writer
            .write_unsigned_var(
                metadata.hash_lower_bits,
                hash & ((1 << metadata.hash_lower_bits) - 1),
            )
            .unwrap();
        self.entry_writer
            .write_unsigned_var(metadata.seg_id_bits, seg_idx as u32)
            .unwrap();
        self.entry_writer
            .write_unsigned_var(metadata.row_id_bits, row_idx as u32)
            .unwrap();
        self.current_entry += 1;
    }

    pub fn build(mut self, metadata: &GlobalIndex) -> IndexBlock {
        for i in self.current_bucket + 1..self.bucket_end_idx {
            self.buckets[i as usize] = self.current_entry as u32;
        }
        let bucket_start_offset = (self.current_entry as u64)
            * (metadata.hash_lower_bits + metadata.seg_id_bits + metadata.row_id_bits) as u64;
        for bucket in self.buckets {
            self.entry_writer
                .write_unsigned_var(metadata.bucket_bits, bucket)
                .unwrap();
        }
        self.entry_writer.byte_align().unwrap();
        drop(self.entry_writer);
        IndexBlock::new(
            self.bucket_start_idx,
            self.bucket_end_idx,
            bucket_start_offset,
            &self.directory,
            self.file_name.clone(),
        )
    }
}

pub struct GlobalIndexBuilder {
    num_rows: u32,
    files: Vec<Arc<PathBuf>>,
    directory: PathBuf,
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

    pub fn set_files(&mut self, files: Vec<Arc<PathBuf>>) -> &mut Self {
        self.files = files;
        self
    }

    pub fn build_from_flush(mut self, mut entries: Vec<(u64, usize, usize)>) -> GlobalIndex {
        self.num_rows = entries.len() as u32;
        for entry in &mut entries {
            entry.0 = splitmix64(entry.0);
        }
        entries.sort_by_key(|entry| entry.0);
        self.build(entries.into_iter())
    }

    pub fn _build_from_merge(mut self, indices: Vec<GlobalIndex>) -> GlobalIndex {
        self.num_rows = indices.iter().map(|index| index.num_rows).sum();
        self.files = indices
            .iter()
            .map(|index| index.files.clone())
            .flatten()
            .collect();
        let mut file_id_remaps = vec![];
        let mut file_id_after_remap = 0;
        for index in &indices {
            let mut file_id_remap = vec![INVALID_FILE_ID; index.files.len()];
            for i in 0..index.files.len() {
                file_id_remap[i] = file_id_after_remap;
                file_id_after_remap += 1;
            }
            file_id_remaps.push(file_id_remap);
        }
        let iters = indices
            .iter()
            .enumerate()
            .map(|(i, index)| index._iter(&file_id_remaps[i]))
            .collect::<Vec<_>>();
        let merge_iter = GlobalIndexMergingIterator::_new(iters);
        self.build(merge_iter)
    }

    fn build(self, iter: impl Iterator<Item = (u64, usize, usize)>) -> GlobalIndex {
        let num_rows = self.num_rows;
        let bucket_bits = 32 - num_rows.leading_zeros();
        let num_buckets = (num_rows / 4 + 2).next_power_of_two();
        let upper_bits = num_buckets.trailing_zeros();
        let lower_bits = 64 - upper_bits;
        let seg_id_bits = 32 - (self.files.len() as u32).trailing_zeros();
        let mut global_index = GlobalIndex {
            files: self.files,
            num_rows: num_rows as u32,
            hash_bits: HASH_BITS,
            hash_upper_bits: upper_bits,
            hash_lower_bits: lower_bits,
            seg_id_bits,
            row_id_bits: 32,
            bucket_bits: bucket_bits,
            index_blocks: vec![],
        };
        let mut index_blocks = Vec::new();
        let mut index_block_builder =
            IndexBlockBuilder::new(0, num_buckets + 1, self.directory.clone());
        for entry in iter {
            index_block_builder.write_entry(entry.0, entry.1, entry.2, &global_index);
        }
        index_blocks.push(index_block_builder.build(&global_index));
        global_index.index_blocks = index_blocks;
        global_index
    }
}

// ================================
// Iterators for merging indices
// ================================
struct IndexBlockIterator<'a> {
    collection: &'a IndexBlock,
    metadata: &'a GlobalIndex,
    current_bucket: u32,
    current_bucket_entry_end: u32,
    current_entry: u32,
    current_upper_hash: u64,
    bucket_reader: BitReader<Cursor<&'a [u8]>, BigEndian>,
    entry_reader: BitReader<Cursor<&'a [u8]>, BigEndian>,
    file_id_remap: &'a Vec<u32>,
}

impl<'a> IndexBlockIterator<'a> {
    fn new(
        collection: &'a IndexBlock,
        metadata: &'a GlobalIndex,
        file_id_remap: &'a Vec<u32>,
    ) -> Self {
        let mut bucket_reader = BitReader::endian(Cursor::new(collection.data.as_ref()), BigEndian);
        let entry_reader = bucket_reader.clone();
        bucket_reader
            .seek_bits(SeekFrom::Start(collection.bucket_start_offset))
            .unwrap();
        let _ = bucket_reader
            .read_unsigned_var::<u32>(metadata.bucket_bits)
            .unwrap();
        let current_bucket_entry_end = bucket_reader
            .read_unsigned_var::<u32>(metadata.bucket_bits)
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
}

impl<'a> Iterator for IndexBlockIterator<'a> {
    type Item = (u64, usize, usize);

    fn next(&mut self) -> Option<Self::Item> {
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
                    .read_unsigned_var::<u32>(self.metadata.bucket_bits)
                    .unwrap();
                self.current_upper_hash += 1 << self.metadata.hash_lower_bits;
            }
            let (lower_hash, seg_idx, row_idx) = self
                .collection
                .read_entry(&mut self.entry_reader, self.metadata);
            self.current_entry += 1;
            if *self.file_id_remap.get(seg_idx).unwrap() != INVALID_FILE_ID {
                return Some((lower_hash + self.current_upper_hash, seg_idx, row_idx));
            }
        }
    }
}

pub struct GlobalIndexIterator<'a> {
    index: &'a GlobalIndex,
    block_idx: usize,
    block_iter: Option<IndexBlockIterator<'a>>,
    file_id_remap: &'a Vec<u32>,
}

impl<'a> GlobalIndexIterator<'a> {
    pub fn _new(index: &'a GlobalIndex, file_id_remap: &'a Vec<u32>) -> Self {
        let mut block_iter = None;
        let block_idx = 0;
        if !index.index_blocks.is_empty() {
            block_iter = Some(index.index_blocks[0].iter(index, file_id_remap));
        }
        Self {
            index,
            block_idx,
            block_iter,
            file_id_remap,
        }
    }
}

impl<'a> Iterator for GlobalIndexIterator<'a> {
    type Item = (u64, usize, usize);

    fn next(&mut self) -> Option<Self::Item> {
        loop {
            if let Some(ref mut iter) = self.block_iter {
                if let Some(item) = iter.next() {
                    return Some(item);
                }
            }
            self.block_idx += 1;
            if self.block_idx >= self.index.index_blocks.len() {
                return None;
            }
            self.block_iter =
                Some(self.index.index_blocks[self.block_idx].iter(self.index, self.file_id_remap));
        }
    }
}

pub struct GlobalIndexMergingIterator<'a> {
    heap: BinaryHeap<HeapItem<'a>>,
}

struct HeapItem<'a> {
    value: (u64, usize, usize),
    iter: GlobalIndexIterator<'a>,
}

impl<'a> PartialEq for HeapItem<'a> {
    fn eq(&self, other: &Self) -> bool {
        self.value.0 == other.value.0
    }
}
impl<'a> Eq for HeapItem<'a> {}

impl<'a> PartialOrd for HeapItem<'a> {
    fn partial_cmp(&self, other: &Self) -> Option<std::cmp::Ordering> {
        // Reverse for min-heap
        other.value.0.partial_cmp(&self.value.0)
    }
}
impl<'a> Ord for HeapItem<'a> {
    fn cmp(&self, other: &Self) -> std::cmp::Ordering {
        // Reverse for min-heap
        other.value.0.cmp(&self.value.0)
    }
}

impl<'a> GlobalIndexMergingIterator<'a> {
    pub fn _new(iterators: Vec<GlobalIndexIterator<'a>>) -> Self {
        let mut heap = BinaryHeap::new();
        for mut it in iterators {
            if let Some(value) = it.next() {
                heap.push(HeapItem { value, iter: it });
            }
        }
        Self { heap }
    }
}

impl<'a> Iterator for GlobalIndexMergingIterator<'a> {
    type Item = (u64, usize, usize);

    fn next(&mut self) -> Option<Self::Item> {
        if let Some(mut heap_item) = self.heap.pop() {
            let result = heap_item.value;
            if let Some(next_value) = heap_item.iter.next() {
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
        let cursor = Cursor::new(self.data.as_ref());
        let mut reader = BitReader::endian(cursor, BigEndian);
        write!(f, "\n   Buckets: ")?;
        let mut num = 0;
        reader
            .seek_bits(SeekFrom::Start(self.bucket_start_offset))
            .unwrap();
        for _i in 0..self.bucket_end_idx {
            num = reader
                .read_unsigned_var::<u32>(metadata.bucket_bits)
                .unwrap();
            write!(f, "{} ", num)?;
        }
        write!(f, "\n   Entries: ")?;
        reader.seek_bits(SeekFrom::Start(0)).unwrap();
        for _i in 0..num {
            let (hash, seg_idx, row_idx) = self.read_entry(&mut reader, metadata);
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

    #[test]
    fn test_new() {
        let files = vec![Arc::new(PathBuf::from("test.parquet"))];
        let vec = vec![
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
            .set_directory(tempfile::tempdir().unwrap().into_path());
        let index = builder.build_from_flush(vec);
        println!("{:?}", index);
        println!("{:?}", index.search(&1));
        println!("{:?}", index.search(&2));

        let file_id_remap = vec![0; index.files.len()];
        for block in index.index_blocks.iter() {
            for (hash, seg_idx, row_idx) in block.iter(&index, &file_id_remap) {
                println!("{} {} {}", hash, seg_idx, row_idx);
            }
        }
    }

    #[test]
    fn stress_test() {
        #[cfg(profiling_enabled)]
        let guard = pprof::ProfilerGuard::new(100).unwrap(); // sample at 100 Hz

        #[cfg(debug_assertions)]
        let num_samples = 1000;
        #[cfg(not(debug_assertions))]
        let num_samples = 10000000;

        let files = vec![Arc::new(PathBuf::from("test.parquet"))];
        let vec = (0..10000000).map(|i| (i as u64, 0, i)).collect::<Vec<_>>();
        let mut builder = GlobalIndexBuilder::new();
        builder
            .set_files(files)
            .set_directory(tempfile::tempdir().unwrap().into_path());
        let index = builder.build_from_flush(vec);
        for i in 0..num_samples {
            if let Some(RecordLocation::DiskFile(FileId(_file_id), row_idx)) =
                index.search(&(i as u64)).get(0)
            {
                assert_eq!(*row_idx, i);
            } else {
                panic!("No record location found for {}", i);
            }
        }
        for i in 10000000..10000000 + num_samples {
            assert_eq!(index.search(&(i as u64)).len(), 0);
        }

        #[cfg(profiling_enabled)]
        if let Ok(report) = guard.report().build() {
            let file = std::fs::File::create("flamegraph.svg").unwrap();
            report.flamegraph(file).unwrap();
        }
    }
    #[test]
    fn test_merge() {
        let files = vec![
            Arc::new(PathBuf::from("1.parquet")),
            Arc::new(PathBuf::from("2.parquet")),
            Arc::new(PathBuf::from("3.parquet")),
        ];
        let vec = (0..100).map(|i| (i as u64, i % 3, i)).collect::<Vec<_>>();
        let mut builder = GlobalIndexBuilder::new();
        builder
            .set_files(files)
            .set_directory(tempfile::tempdir().unwrap().into_path());
        let index1 = builder.build_from_flush(vec);
        let files = vec![
            Arc::new(PathBuf::from("4.parquet")),
            Arc::new(PathBuf::from("5.parquet")),
        ];
        let vec = (100..200).map(|i| (i as u64, i % 2, i)).collect::<Vec<_>>();
        let mut builder = GlobalIndexBuilder::new();
        builder
            .set_files(files)
            .set_directory(tempfile::tempdir().unwrap().into_path());
        let index2 = builder.build_from_flush(vec);
        let mut builder = GlobalIndexBuilder::new();
        builder.set_directory(tempfile::tempdir().unwrap().into_path());
        let merged = builder._build_from_merge(vec![index1, index2]);

        for i in 0u64..100u64 {
            let ret = merged.search(&i);
            let record_location = ret.get(0).unwrap();
            let RecordLocation::DiskFile(FileId(file_id), _) = record_location else {
                panic!("No record location found for {}", i);
            };
            assert_eq!(file_id.to_string_lossy(), format!("{}.parquet", i % 3 + 1));
        }
    }
}
