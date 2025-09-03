use crate::create_data_file;
use crate::error::Result;
use crate::storage::async_bitwriter::BitWriter as AsyncBitWriter;
use crate::storage::storage_utils::{MooncakeDataFileRef, RecordLocation};
use crate::NonEvictableHandle;
use bitstream_io::{BigEndian, BitRead, BitReader};
use memmap2::Mmap;
use std::collections::{BinaryHeap, HashSet};
use std::fmt::Debug;
use std::hash::{Hash, Hasher};
use std::io::Cursor;
use std::io::SeekFrom;
use std::path::PathBuf;
use std::sync::Arc;
use std::{fmt, vec};
use tokio::fs::File as AsyncFile;
use tokio_bitstream_io::BigEndian as AsyncBigEndian;

// Constants
const HASH_BITS: u32 = 64;
const _MAX_BLOCK_SIZE: u32 = 2 * 1024 * 1024 * 1024; // 2GB
const _TARGET_NUM_FILES_PER_INDEX: u32 = 4000;
const INVALID_FILE_ID: u32 = 0xFFFFFFFF;

pub(super) fn splitmix64(mut x: u64) -> u64 {
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

/// It's guaranteed every file indice references to different set of data files.
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
    /// Local index file path.
    pub(crate) index_file: MooncakeDataFileRef,
    /// File size for the index block file, used to decide whether to trigger merge index blocks merge.
    pub(crate) file_size: u64,
    /// Mmapped-data.
    /// Synchronous IO is not needed because here we use mmap.
    data: Arc<Option<Mmap>>,
    /// Cache handle within object storage cache.
    pub(crate) cache_handle: Option<NonEvictableHandle>,
}

struct BucketEntry {
    upper_hash: u64,
    entry_start: u32,
    entry_end: u32,
}

impl IndexBlock {
    pub(crate) async fn new(
        bucket_start_idx: u32,
        bucket_end_idx: u32,
        bucket_start_offset: u64,
        index_file: MooncakeDataFileRef,
    ) -> Self {
        let file = tokio::fs::File::open(index_file.file_path()).await.unwrap();
        let file_metadata = file.metadata().await.unwrap();
        let file = file.into_std().await;
        let data = unsafe { Mmap::map(&file).unwrap() };
        Self {
            bucket_start_idx,
            bucket_end_idx,
            bucket_start_offset,
            index_file,
            file_size: file_metadata.len(),
            data: Arc::new(Some(data)),
            cache_handle: None,
        }
    }

    fn create_iterator<'a>(
        &'a self,
        metadata: &'a GlobalIndex,
        file_id_remap: &'a Vec<u32>,
    ) -> IndexBlockIterator<'a> {
        IndexBlockIterator::new(self, metadata, file_id_remap)
    }

    #[inline]
    fn read_buckets(
        &self,
        bucket_idxs: &[u32],
        reader: &mut BitReader<Cursor<&[u8]>, BigEndian>,
        metadata: &GlobalIndex,
    ) -> Vec<BucketEntry> {
        let mut results = Vec::new();
        for bucket_idx in bucket_idxs {
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
            if start != end {
                results.push(BucketEntry {
                    upper_hash: (*bucket_idx as u64) << metadata.hash_lower_bits,
                    entry_start: start,
                    entry_end: end,
                });
            }
        }
        results
    }

    #[inline]
    fn read_entry(
        &self,
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
        &self,
        value_and_hashes: &[(u64, u64)],
        mut bucket_idxs: Vec<u32>,
        metadata: &GlobalIndex,
    ) -> Vec<(u64, RecordLocation)> {
        let cursor = Cursor::new(self.data.as_ref().as_ref().unwrap().as_ref());
        let mut reader = BitReader::endian(cursor, BigEndian);
        let mut entry_reader = reader.clone();
        bucket_idxs.dedup();
        let entries = self.read_buckets(&bucket_idxs, &mut reader, metadata);
        let mut results = Vec::new();
        let mut lookup_iter = LookupIterator::new(self, metadata, &mut entry_reader, &entries);
        let mut i = 0;
        let mut lookup_entry = lookup_iter.next();
        while let Some((entry_hash, seg_idx, row_idx)) = lookup_entry {
            while i < value_and_hashes.len() && value_and_hashes[i].1 < entry_hash {
                i += 1;
            }
            if i < value_and_hashes.len() && value_and_hashes[i].1 == entry_hash {
                let value = value_and_hashes[i].0;
                results.push((
                    value,
                    RecordLocation::DiskFile(metadata.files[seg_idx].file_id(), row_idx),
                ));
            }
            lookup_entry = lookup_iter.next();
        }
        results
    }
}

pub struct LookupIterator<'a> {
    index: &'a IndexBlock,
    metadata: &'a GlobalIndex,
    entry_reader: &'a mut BitReader<Cursor<&'a [u8]>, BigEndian>,
    entries: &'a Vec<BucketEntry>,
    current_bucket: usize,
    current_entry: u32,
}

impl<'a> LookupIterator<'a> {
    fn new(
        index: &'a IndexBlock,
        metadata: &'a GlobalIndex,
        entry_reader: &'a mut BitReader<Cursor<&'a [u8]>, BigEndian>,
        entries: &'a Vec<BucketEntry>,
    ) -> Self {
        let mut ret = Self {
            index,
            metadata,
            entry_reader,
            entries,
            current_bucket: 0,
            current_entry: 0,
        };
        ret.seek_to_bucket_entry_start();
        ret
    }

    fn seek_to_bucket_entry_start(&mut self) {
        if self.current_bucket < self.entries.len() {
            self.current_entry = self.entries[self.current_bucket].entry_start;
            self.entry_reader
                .seek_bits(SeekFrom::Start(
                    self.current_entry as u64
                        * (self.metadata.hash_lower_bits
                            + self.metadata.seg_id_bits
                            + self.metadata.row_id_bits) as u64,
                ))
                .unwrap();
        }
    }

    fn next(&mut self) -> Option<(u64, usize, usize)> {
        loop {
            if self.current_bucket >= self.entries.len() {
                return None;
            }
            if self.current_entry < self.entries[self.current_bucket].entry_end {
                let (lower_hash, seg_idx, row_idx) =
                    self.index.read_entry(self.entry_reader, self.metadata);
                self.current_entry += 1;
                return Some((
                    lower_hash | self.entries[self.current_bucket].upper_hash,
                    seg_idx,
                    row_idx,
                ));
            }
            self.current_bucket += 1;
            self.seek_to_bucket_entry_start();
        }
    }
}

impl GlobalIndex {
    /// Get total index block files size.
    pub fn get_index_blocks_size(&self) -> u64 {
        self.index_blocks
            .iter()
            .map(|cur_index_block| cur_index_block.file_size)
            .sum()
    }
    pub async fn search_values(
        &self,
        value_and_hashes: &[(u64, u64)],
    ) -> Vec<(u64, RecordLocation)> {
        let mut results = Vec::new();
        let upper_hashes = value_and_hashes
            .iter()
            .map(|(_, hash)| (hash >> self.hash_lower_bits) as u32)
            .collect::<Vec<_>>();
        let mut start_idx = 0;
        for block in self.index_blocks.iter() {
            while upper_hashes[start_idx] < block.bucket_start_idx {
                start_idx += 1;
            }
            let mut end_idx = start_idx;
            while end_idx < upper_hashes.len() && upper_hashes[end_idx] < block.bucket_end_idx {
                end_idx += 1;
            }
            results.extend(block.read(
                &value_and_hashes[start_idx..end_idx],
                upper_hashes[start_idx..end_idx].to_vec(),
                self,
            ));
        }
        results
    }

    pub fn create_iterator<'a>(&'a self, file_id_remap: &'a Vec<u32>) -> GlobalIndexIterator<'a> {
        GlobalIndexIterator::new(self, file_id_remap)
    }

    pub fn prepare_hashes_for_lookup(values: impl Iterator<Item = u64>) -> Vec<(u64, u64)> {
        let mut ret = values
            .map(|value| (value, splitmix64(value)))
            .collect::<Vec<_>>();
        ret.sort_by_key(|(_, hash)| *hash);
        ret.dedup_by_key(|(_, hash)| *hash);
        ret
    }
}

// ================================
// Builders
// ================================
struct IndexBlockBuilder {
    bucket_start_idx: u32,
    bucket_end_idx: u32,
    buckets: Vec<u32>,
    index_file: MooncakeDataFileRef,
    entry_writer: AsyncBitWriter<AsyncFile, AsyncBigEndian>,
    current_bucket: u32,
    current_entry: u32,
}

impl IndexBlockBuilder {
    pub async fn new(
        bucket_start_idx: u32,
        bucket_end_idx: u32,
        file_id: u64,
        directory: PathBuf,
    ) -> Result<Self> {
        let file_name = format!("index_block_{}.bin", uuid::Uuid::now_v7());
        let file_path = directory.join(&file_name);

        let file = AsyncFile::create(&file_path).await?;
        let entry_writer = AsyncBitWriter::endian(file, AsyncBigEndian);
        let index_file = create_data_file(file_id, file_path.to_str().unwrap().to_string());

        Ok(Self {
            bucket_start_idx,
            bucket_end_idx,
            buckets: vec![0; (bucket_end_idx - bucket_start_idx) as usize],
            index_file,
            entry_writer,
            current_bucket: bucket_start_idx,
            current_entry: 0,
        })
    }

    /// Append current entry to the index block, and return whether buffer inside of bitwriter is full and should be flushed.
    pub fn write_entry(
        &mut self,
        hash: u64,
        seg_idx: usize,
        row_idx: usize,
        metadata: &GlobalIndex,
    ) -> bool {
        while (hash >> metadata.hash_lower_bits) != self.current_bucket as u64 {
            self.current_bucket += 1;
            self.buckets[self.current_bucket as usize] = self.current_entry;
        }
        let _ = self.entry_writer.write(
            metadata.hash_lower_bits,
            hash & ((1 << metadata.hash_lower_bits) - 1),
        );
        let _ = self
            .entry_writer
            .write(metadata.seg_id_bits, seg_idx as u32);
        let to_flush = self
            .entry_writer
            .write(metadata.row_id_bits, row_idx as u32);
        self.current_entry += 1;

        to_flush
    }

    /// Flush buffered entries written to disk.
    pub async fn flush(&mut self) -> Result<()> {
        self.entry_writer.flush().await?;
        Ok(())
    }

    pub async fn build(mut self, metadata: &GlobalIndex) -> Result<IndexBlock> {
        for i in self.current_bucket + 1..self.bucket_end_idx {
            self.buckets[i as usize] = self.current_entry;
        }
        let bucket_start_offset = (self.current_entry as u64)
            * (metadata.hash_lower_bits + metadata.seg_id_bits + metadata.row_id_bits) as u64;
        let buckets = std::mem::take(&mut self.buckets);
        for cur_bucket in buckets {
            let to_flush = self.entry_writer.write(metadata.bucket_bits, cur_bucket);
            if to_flush {
                self.entry_writer.flush().await?;
            }
        }
        self.entry_writer.byte_align();
        self.entry_writer.flush().await?;
        drop(self.entry_writer);

        Ok(IndexBlock::new(
            self.bucket_start_idx,
            self.bucket_end_idx,
            bucket_start_offset,
            self.index_file,
        )
        .await)
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

    // Util function for merge file indices, to get file id remap.
    fn create_file_id_remap_at_merge<'a>(
        file_indice_iter: impl Iterator<Item = &'a GlobalIndex>,
    ) -> Vec<Vec<u32>> {
        let mut file_id_remaps = vec![];
        let mut file_id_after_remap = 0;
        for index in file_indice_iter {
            let mut file_id_remap = vec![INVALID_FILE_ID; index.files.len()];
            for (_, item) in file_id_remap.iter_mut().enumerate().take(index.files.len()) {
                *item = file_id_after_remap;
                file_id_after_remap += 1;
            }
            file_id_remaps.push(file_id_remap);
        }
        file_id_remaps
    }

    // ================================
    // Build from flush
    // ================================
    pub async fn build_from_flush(
        mut self,
        mut entries: Vec<(u64, usize, usize)>,
        file_id: u64,
    ) -> Result<GlobalIndex> {
        self.num_rows = entries.len() as u32;
        for entry in &mut entries {
            entry.0 = splitmix64(entry.0);
        }
        entries.sort_unstable_by_key(|entry| entry.0);
        let global_index = self.build(entries.into_iter(), file_id).await?;
        Ok(global_index)
    }

    async fn build(
        mut self,
        iter: impl Iterator<Item = (u64, usize, usize)>,
        file_id: u64,
    ) -> Result<GlobalIndex> {
        let (num_buckets, mut global_index) = self.create_global_index();
        let mut index_blocks = Vec::new();
        let mut index_block_builder =
            IndexBlockBuilder::new(0, num_buckets + 1, file_id, self.directory.clone()).await?;
        for entry in iter {
            let to_flush =
                index_block_builder.write_entry(entry.0, entry.1, entry.2, &global_index);
            if to_flush {
                index_block_builder.flush().await?;
            }
        }
        index_blocks.push(index_block_builder.build(&global_index).await?);
        global_index.index_blocks = index_blocks;
        Ok(global_index)
    }

    // ================================
    // Build from merge
    // ================================
    #[allow(clippy::mutable_key_type)]
    pub async fn build_from_merge(
        mut self,
        indices: HashSet<GlobalIndex>,
        file_id: u64,
    ) -> Result<GlobalIndex> {
        self.num_rows = indices.iter().map(|index| index.num_rows).sum();
        self.files = indices
            .iter()
            .flat_map(|index| index.files.clone())
            .collect();
        let file_id_remaps = Self::create_file_id_remap_at_merge(indices.iter());
        let mut iters = Vec::with_capacity(indices.len());
        for (idx, index) in indices.iter().enumerate() {
            iters.push(index.create_iterator(&file_id_remaps[idx]));
        }
        let merge_iter = GlobalIndexMergingIterator::new(iters);
        self.build_from_merging_iterator(merge_iter, file_id).await
    }

    async fn build_from_merging_iterator(
        mut self,
        mut iter: GlobalIndexMergingIterator<'_>,
        file_id: u64,
    ) -> Result<GlobalIndex> {
        let (num_buckets, mut global_index) = self.create_global_index();
        let mut index_block_builder =
            IndexBlockBuilder::new(0, num_buckets + 1, file_id, self.directory.clone()).await?;
        while let Some(entry) = iter.next() {
            let to_flush =
                index_block_builder.write_entry(entry.0, entry.1, entry.2, &global_index);
            if to_flush {
                index_block_builder.flush().await?;
            }
        }

        let mut index_blocks = Vec::new();
        index_blocks.push(index_block_builder.build(&global_index).await?);
        global_index.index_blocks = index_blocks;
        Ok(global_index)
    }

    // ================================
    // Build from merge with predicate
    // ================================
    //
    // Different from [`build_from_merge`], it only merge items which could be found by [`get_remapped_record_location`].
    //
    // # Arguments
    //
    // * num_rows: number of rows after merge, which takes predicate into consideration.
    // * get_remapped_record_location: a predicate to decide whether a hash entry will be merged into the final file indice, and emits (seg-idx, row-idx) for selected entries.
    pub async fn build_from_merge_for_compaction<GetRemappedRecLoc, GetSegIdx>(
        mut self,
        num_rows: u32,
        file_id: u64,
        indices: Vec<GlobalIndex>,
        new_data_files: Vec<MooncakeDataFileRef>,
        get_remapped_record_location: GetRemappedRecLoc,
        get_seg_idx: GetSegIdx,
    ) -> Result<GlobalIndex>
    where
        GetRemappedRecLoc: FnMut(RecordLocation) -> Option<RecordLocation>,
        GetSegIdx: FnMut(RecordLocation) -> usize, /*seg_idx*/
    {
        // Assign data files before compaction, used to compose old record location and look it up with [`get_remapped_record_location`] and new record location after compaction.
        self.files = indices
            .iter()
            .flat_map(|index| index.files.clone())
            .collect();
        self.num_rows = num_rows;

        let file_id_remaps = Self::create_file_id_remap_at_merge(indices.iter());
        let mut iters = Vec::with_capacity(indices.len());
        for (idx, index) in indices.iter().enumerate() {
            iters.push(index.create_iterator(&file_id_remaps[idx]));
        }
        let merge_iter = GlobalIndexMergingIterator::new(iters);
        self.build_from_merging_iterator_with_predicate(
            file_id,
            merge_iter,
            new_data_files,
            get_remapped_record_location,
            get_seg_idx,
        )
        .await
    }

    async fn build_from_merging_iterator_with_predicate<GetRemappedRecLoc, GetSegIdx>(
        mut self,
        file_id: u64,
        mut iter: GlobalIndexMergingIterator<'_>,
        new_data_files: Vec<MooncakeDataFileRef>,
        mut get_remapped_record_location: GetRemappedRecLoc,
        mut get_seg_idx: GetSegIdx,
    ) -> Result<GlobalIndex>
    where
        GetRemappedRecLoc: FnMut(RecordLocation) -> Option<RecordLocation>,
        GetSegIdx: FnMut(RecordLocation) -> usize, /*seg_idx*/
    {
        let (num_buckets, mut global_index) = self.create_global_index();
        let mut index_block_builder =
            IndexBlockBuilder::new(0, num_buckets + 1, file_id, self.directory.clone()).await?;

        while let Some((hash, old_seg_idx, old_row_idx)) = iter.next() {
            let old_record_location =
                RecordLocation::DiskFile(global_index.files[old_seg_idx].file_id(), old_row_idx);
            if let Some(new_record_location) = get_remapped_record_location(old_record_location) {
                let new_row_idx = match new_record_location {
                    RecordLocation::DiskFile(_, offset) => offset,
                    _ => panic!("Expected DiskFile variant"),
                };
                let new_seg_idx = get_seg_idx(new_record_location);
                let to_flush =
                    index_block_builder.write_entry(hash, new_seg_idx, new_row_idx, &global_index);
                if to_flush {
                    index_block_builder.flush().await?;
                }
            }
            // The record doesn't exist in compacted data files, which means the corresponding row doesn't exist in the data file after compaction, simply ignore.
        }

        let mut index_blocks = Vec::new();
        index_blocks.push(index_block_builder.build(&global_index).await?);
        global_index.index_blocks = index_blocks;

        // Now all the (hash, seg_idx, row_idx) points to the new files passed in.
        global_index.files = new_data_files;

        Ok(global_index)
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
        let mut bucket_reader = BitReader::endian(
            Cursor::new(collection.data.as_ref().as_ref().unwrap().as_ref()),
            BigEndian,
        );
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

    fn next(
        &mut self,
    ) -> Option<(
        u64,   /*hash*/
        usize, /*seg_idx*/
        usize, /*row_idx*/
    )> {
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
        let seg_idx = self.file_id_remap.get(seg_idx).unwrap();
        assert_ne!(*seg_idx, INVALID_FILE_ID);
        Some((
            lower_hash + self.current_upper_hash,
            *seg_idx as usize,
            row_idx,
        ))
    }
}

pub struct GlobalIndexIterator<'a> {
    index: &'a GlobalIndex,
    block_idx: usize,
    block_iter: Option<IndexBlockIterator<'a>>,
    file_id_remap: &'a Vec<u32>,
}

impl<'a> GlobalIndexIterator<'a> {
    pub fn new(index: &'a GlobalIndex, file_id_remap: &'a Vec<u32>) -> Self {
        let mut block_iter = None;
        let block_idx = 0;
        if !index.index_blocks.is_empty() {
            block_iter = Some(index.index_blocks[0].create_iterator(index, file_id_remap));
        }
        Self {
            index,
            block_idx,
            block_iter,
            file_id_remap,
        }
    }

    pub fn next(
        &mut self,
    ) -> Option<(
        u64,   /*hash*/
        usize, /*seg_idx*/
        usize, /*row_idx*/
    )> {
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
            self.block_iter = Some(
                self.index.index_blocks[self.block_idx]
                    .create_iterator(self.index, self.file_id_remap),
            );
        }
    }
}

pub struct GlobalIndexMergingIterator<'a> {
    heap: BinaryHeap<HeapItem<'a>>,
}

struct HeapItem<'a> {
    value: (
        u64,   /*hash*/
        usize, /*seg_idx*/
        usize, /*row_idx*/
    ),
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
    pub fn new(iterators: Vec<GlobalIndexIterator<'a>>) -> Self {
        let mut heap = BinaryHeap::new();
        for mut it in iterators {
            if let Some(value) = it.next() {
                heap.push(HeapItem { value, iter: it });
            }
        }
        Self { heap }
    }

    pub fn next(&mut self) -> Option<(u64, usize, usize)> {
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
        let cursor = Cursor::new(self.data.as_ref().as_ref().unwrap().as_ref());
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
            write!(f, "{num} ")?;
        }
        write!(f, "\n   Entries: ")?;
        reader.seek_bits(SeekFrom::Start(0)).unwrap();
        for _i in 0..num {
            let (hash, seg_idx, row_idx) = self.read_entry(&mut reader, metadata);
            write!(f, "\n     {hash} {seg_idx} {row_idx}")?;
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
pub fn test_get_hashes_for_index(values: &[u64]) -> Vec<(u64, u64)> {
    GlobalIndex::prepare_hashes_for_lookup(values.iter().copied())
}

#[cfg(test)]
mod tests {
    use std::vec;

    use super::*;
    use tracing::debug;

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
        let index = builder
            .build_from_flush(hash_entries.clone(), /*file_id=*/ 1)
            .await
            .unwrap();

        // Search for a non-existent key doesn't panic.
        assert!(index
            .search_values(&test_get_hashes_for_index(&[0]))
            .await
            .is_empty());

        let data_file_ids = [data_file.file_id()];
        for (hash, seg_idx, row_idx) in hash_entries.iter() {
            let expected_record_loc = RecordLocation::DiskFile(data_file_ids[*seg_idx], *row_idx);
            assert_eq!(
                index
                    .search_values(&test_get_hashes_for_index(&[*hash]))
                    .await,
                vec![(*hash, expected_record_loc)]
            );
        }

        let mut hash_entry_num = 0;
        let file_id_remap = vec![0; index.files.len()];
        for block in index.index_blocks.iter() {
            let mut index_block_iter = block.create_iterator(&index, &file_id_remap);
            while let Some((hash, seg_idx, row_idx)) = index_block_iter.next() {
                debug!(?hash, seg_idx, row_idx, "index entry");
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
        let index1 = builder.build_from_flush(vec, /*file_id=*/ 4).await.unwrap();

        let files = vec![
            create_data_file(/*file_id=*/ 5, "4.parquet".to_string()),
            create_data_file(/*file_id=*/ 6, "5.parquet".to_string()),
        ];
        let vec = (100..200).map(|i| (i as u64, i % 2, i)).collect::<Vec<_>>();
        let mut builder = GlobalIndexBuilder::new();
        builder
            .set_files(files)
            .set_directory(tempfile::tempdir().unwrap().keep());
        let index2 = builder.build_from_flush(vec, /*file_id=*/ 7).await.unwrap();

        let mut builder = GlobalIndexBuilder::new();
        builder.set_directory(tempfile::tempdir().unwrap().keep());
        let merged = builder
            .build_from_merge(
                HashSet::<GlobalIndex>::from([index1, index2]),
                /*file_id=*/ 8,
            )
            .await
            .unwrap();

        let values = (0..200).collect::<Vec<_>>();
        let mut ret = merged
            .search_values(&test_get_hashes_for_index(&values))
            .await;
        ret.sort_by_key(|(value, _)| *value);
        assert_eq!(ret.len(), 200);
        for (value, pos) in ret.iter() {
            let RecordLocation::DiskFile(FileId(file_id), _) = pos else {
                panic!("No record location found for {value}");
            };
            // Check for the first file indice.
            // The second batch of data file ids starts with 1.
            if *value < 100 {
                assert_eq!(*file_id, *value % 3 + 1);
            }
            // Check for the second file indice.
            // The second batch of data file ids starts with 5.
            else {
                assert_eq!(*file_id, (*value - 100) % 2 + 5);
            }
        }
    }
}
