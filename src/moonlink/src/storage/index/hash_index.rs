use super::{Index, MemIndex, MooncakeIndex, ParquetFileIndex};
use std::path::PathBuf;
use bitstream_io::{BigEndian, BitRead, BitReader, BitWrite, BitWriter};

// Constants
const HASH_BITS: u32 = 64;
const TARGET_BLOCK_SIZE: u32 = 16 * 1024 * 1024; // 16MB
const TARGET_NUM_FILES_PER_INDEX: u32 = 4000;

// Hash index
// that maps a u64 to offsets
//
// Structure:
// Buckets:
// [entry_offset],[entry_offset]...[entry_offset]
//
// Values
// [lower_bit_hash, seg_idx, row_idx]
#[derive(Debug)]
pub struct GlobalIndex {
    files: Vec<PathBuf>,
    num_rows: u32,
    hash_bits: u32,
    hash_upper_bits: u32,
    hash_lower_bits: u32,
    seg_id_bits: u32,
    row_id_bits: u32,
    bucket_bits: u32,

    index_blocks: Vec<IndexBlock>,
}

#[derive(Debug)]
struct IndexBlock {
    bucket_start_idx: u32,
    bucket_end_idx: u32,
    file_start_offset: u64,
    file_end_offset: u64,

    data: Option<Vec<u8>>,
}

impl IndexBlock {

    fn new(buckets: Vec<u32>, entries: Vec<(u64, usize, usize)>, metadata: &GlobalIndex) -> Self {
        let total_size = buckets.len() * metadata.bucket_bits as usize + entries.len() * (metadata.hash_lower_bits as usize + metadata.seg_id_bits as usize + metadata.row_id_bits as usize);
        let mut data = vec![0; total_size/ 8 + 1];
        let mut writer = BitWriter::endian(&mut data, BigEndian);
        for bucket in &buckets {
            writer.write_unsigned_var(metadata.bucket_bits, *bucket).unwrap();
        }
        for entry in entries {
            writer.write_unsigned_var(metadata.hash_lower_bits, entry.0 & ((1 << metadata.hash_lower_bits) - 1)).unwrap();
            writer.write_unsigned_var(metadata.seg_id_bits, entry.1 as u32).unwrap();
            writer.write_unsigned_var(metadata.row_id_bits, entry.2 as u32).unwrap();
        }
        Self {bucket_start_idx: 0, bucket_end_idx: buckets.len() as u32, file_start_offset: 0, file_end_offset: total_size as u64, data: Some(data)}
    }

    #[inline]
    fn read_bucket(self: &Self, bucket_idx: u32, reader: &mut BitReader<&[u8], BigEndian>) -> (u32, u32) {
        reader.skip(bucket_idx * 32).unwrap();
        let start = reader.read::<32, u32>().unwrap();
        let end = reader.read::<32, u32>().unwrap();
        (start, end)
    }

    #[inline]
    fn read_entry(self: &Self, reader: &mut BitReader<&[u8], BigEndian>, metadata: &GlobalIndex) -> (u64, usize, usize) {
        let hash = reader.read_unsigned_var::<u64>(metadata.hash_lower_bits).unwrap();
        let seg_idx = reader.read_unsigned_var::<u32>(metadata.seg_id_bits).unwrap();
        let row_idx = reader.read_unsigned_var::<u32>(metadata.row_id_bits).unwrap();
        (hash, seg_idx as usize, row_idx as usize)
    }

    fn read(self: &Self, target_lower_hash: u64, bucket_idx: u32, metadata: &GlobalIndex) -> Vec<(u64, usize, usize)> {
        assert!(bucket_idx >= self.bucket_start_idx && bucket_idx < self.bucket_end_idx);
        let mut reader = BitReader::endian(self.data.as_ref().unwrap().as_slice(), BigEndian);
        let (entry_start, entry_end) = self.read_bucket(bucket_idx, &mut reader);
        if entry_start != entry_end {
            let mut results = Vec::new();
            reader.skip(entry_start * (metadata.hash_lower_bits + metadata.seg_id_bits + metadata.row_id_bits) + (self.bucket_end_idx- self.bucket_start_idx + 1) * metadata.bucket_bits).unwrap();
            for i in entry_start..entry_end {
                let (hash, seg_idx, row_idx) = self.read_entry(&mut reader, metadata);
                if hash == target_lower_hash {
                    results.push((hash, seg_idx as usize, row_idx as usize));
                }
            }
            results
        }
        else {
            vec![]
        }
    }
}

impl GlobalIndex {
    pub fn search(&self, value: u64) -> Vec<(u64, usize, usize)> {
        let target_hash = splitmix64(value);
        let lower_hash = target_hash & ((1 << self.hash_lower_bits) - 1);
        let bucket_idx = (target_hash >> self.hash_lower_bits) as u32;
        for block in self.index_blocks.iter() {
            if bucket_idx >= block.bucket_start_idx && bucket_idx < block.bucket_end_idx {
                return block.read(lower_hash, bucket_idx, self);
            }
        }
        vec![]
    }
}

pub struct GlobalIndexBuilder {
    entries: Vec<(u64, usize, usize)>,
    files: Vec<PathBuf>,
}

fn splitmix64(mut x: u64) -> u64 {
    x = x.wrapping_add(0x9E3779B97F4A7C15);
    let mut z = x;
    z = (z ^ (z >> 30)).wrapping_mul(0xBF58476D1CE4E5B9);
    z = (z ^ (z >> 27)).wrapping_mul(0x94D049BB133111EB);
    z ^ (z >> 31)
}

impl GlobalIndexBuilder {
    pub fn new(files: Vec<PathBuf>) -> Self {
        Self {entries: vec![], files: files}
    }

    pub fn add_entry(&mut self, hash: u64, seg_idx: usize, row_idx: usize) {
        self.entries.push((splitmix64(hash), seg_idx, row_idx));
    }

    pub fn build(mut self) -> GlobalIndex {
        self.entries.sort_by_key(|(hash, _, _)| *hash);
        let num_rows= self.entries.len() as u32;
        let bucket_bits = 32 - num_rows.leading_zeros();
        let num_buckets = (num_rows/4 + 1).next_power_of_two();
        let upper_bits = num_buckets.trailing_zeros();
        let lower_bits = 64 - upper_bits;
        let mut buckets = vec![0; num_buckets as usize + 1];
        let mut bucket_idx: usize = 0;
        buckets[0] = 0;
        for (i,(hash, _, _)) in self.entries.iter().enumerate() {
            while (hash >> lower_bits) != bucket_idx as u64 {
                bucket_idx += 1;
                buckets[bucket_idx] = i as u32;
            }
        }
        buckets[num_buckets as usize] = num_rows as u32;
        let mut global_index = GlobalIndex {
            files: self.files, 
            num_rows: num_rows as u32, 
            hash_bits: 64, 
            hash_upper_bits: upper_bits, 
            hash_lower_bits: lower_bits, 
            seg_id_bits: 16, 
            row_id_bits: 32, 
            bucket_bits: bucket_bits,
            index_blocks: vec![]};
        global_index.index_blocks.push(IndexBlock::new(buckets, self.entries, &global_index));
        global_index
    }
    
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new() {
        let files = vec![PathBuf::from("test.parquet")];
        let mut index = GlobalIndexBuilder::new(files);
        index.add_entry(1, 0, 0);
        index.add_entry(2, 0, 1);
        index.add_entry(3, 0, 2);
        index.add_entry(4, 0, 3);
        index.add_entry(5, 0, 4);
        index.add_entry(16, 0, 5);
        index.add_entry(214141, 0, 6);
        index.add_entry(2141, 0, 7);
        index.add_entry(21141, 0, 8);
        index.add_entry(219511, 0, 9);
        index.add_entry(1421141, 0, 10);
        index.add_entry(1111111141, 0, 11);
        index.add_entry(99999, 0, 12); 
        
        let index = index.build();
        println!("{:?}", index.search(1));
        println!("{:?}", index.search(2));
    }
}