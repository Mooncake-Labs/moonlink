use bincode::de::{read::Reader, Decode, Decoder};
use bincode::enc::{write::Writer, Encode, Encoder};
use bincode::error::DecodeError;
use bincode::error::EncodeError;
use more_asserts as ma;
use serde::{Deserialize, Serialize};

#[derive(Clone, Debug, PartialEq, Serialize, Deserialize)]
pub struct DeletionVector {
    pub data_file_number: u32,
    pub puffin_file_number: u32,
    pub offset: u32,
    pub size: u32,
}

#[derive(Clone, Debug, PartialEq, PartialOrd, Eq, Ord)]
pub struct PositionDelete {
    pub data_file_number: u32,
    pub data_file_row_number: u32,
}

#[derive(Clone, Debug, PartialEq)]
pub struct MooncakeTableMetadata {
    pub data_files: Vec<String>,
    pub puffin_files: Vec<String>,
    pub deletion_vectors: Vec<DeletionVector>,
    pub position_deletes: Vec<PositionDelete>,
}

impl Encode for MooncakeTableMetadata {
    fn encode<E: Encoder>(&self, encoder: &mut E) -> Result<(), EncodeError> {
        let writer = encoder.writer();

        // Write data filepaths offsets.
        write_usize(writer, self.data_files.len())?;
        let mut offset = 0;
        for data_file in &self.data_files {
            write_usize(writer, offset)?;
            offset = offset.saturating_add(data_file.len());
        }
        write_usize(writer, offset)?;

        // Write deletion vector puffin blob filepaths offsets.
        // Arrange all offsets together (instead of mixing with blob start offset and blob size), so decode side could directly operate on `uint32_t` pointers.
        write_usize(writer, self.puffin_files.len())?;
        let mut offset = 0;
        for puffin_file in &self.puffin_files {
            write_usize(writer, offset)?;
            offset = offset.saturating_add(puffin_file.len());
        }
        write_usize(writer, offset)?;

        // Used to check deletion vector ordering.
        let mut prev_data_file_number = 0;
        // Write deletion vector puffin blob information.
        write_usize(writer, self.deletion_vectors.len())?;
        for deletion_vector in &self.deletion_vectors {
            ma::assert_ge!(deletion_vector.data_file_number, prev_data_file_number);
            prev_data_file_number = deletion_vector.data_file_number;

            write_u32(writer, deletion_vector.data_file_number)?;
            write_u32(writer, deletion_vector.puffin_file_number)?;
            write_u32(writer, deletion_vector.offset)?;
            write_u32(writer, deletion_vector.size)?;
        }

        // Used to check positional deletes ordering.
        let mut prev_position_delete_data_file_number = 0;
        // Write positional deletion records.
        write_usize(writer, self.position_deletes.len())?;
        for position_delete in &self.position_deletes {
            ma::assert_ge!(
                position_delete.data_file_number,
                prev_position_delete_data_file_number
            );
            prev_position_delete_data_file_number = position_delete.data_file_number;

            write_u32(writer, position_delete.data_file_number)?;
            write_u32(writer, position_delete.data_file_row_number)?;
        }

        // Write data filepaths.
        for data_file in &self.data_files {
            writer.write(data_file.as_bytes())?;
        }

        // Write puffin filepaths.
        for puffin_file in &self.puffin_files {
            writer.write(puffin_file.as_bytes())?;
        }

        Ok(())
    }
}

impl<Context> Decode<Context> for MooncakeTableMetadata {
    fn decode<D: Decoder<Context = Context>>(decoder: &mut D) -> Result<Self, DecodeError> {
        let mut reader = decoder.reader();

        let data_files_len = read_usize(&mut reader)?;
        let mut data_file_offsets = Vec::with_capacity(data_files_len + 1);
        for _ in 0..=data_files_len {
            let data_file_offset = read_usize(&mut reader)?;
            data_file_offsets.push(data_file_offset);
        }

        let puffin_files_len = read_usize(&mut reader)?;
        let mut puffin_file_offsets = Vec::with_capacity(puffin_files_len + 1);
        for _ in 0..=puffin_files_len {
            let puffin_file_offset = read_usize(&mut reader)?;
            puffin_file_offsets.push(puffin_file_offset);
        }

        let deletion_vectors_len = read_usize(&mut reader)?;
        let mut deletion_vectors = Vec::with_capacity(deletion_vectors_len);
        for _ in 0..deletion_vectors_len {
            let data_file_number = read_u32(&mut reader)?;
            let puffin_file_number = read_u32(&mut reader)?;
            let offset = read_u32(&mut reader)?;
            let size = read_u32(&mut reader)?;
            deletion_vectors.push(DeletionVector {
                data_file_number,
                puffin_file_number,
                offset,
                size,
            });
        }

        let position_deletes_len = read_usize(&mut reader)?;
        let mut position_deletes = Vec::with_capacity(position_deletes_len);
        for _ in 0..position_deletes_len {
            let data_file_number = read_u32(&mut reader)?;
            let data_file_row_number = read_u32(&mut reader)?;
            position_deletes.push(PositionDelete {
                data_file_number,
                data_file_row_number,
            });
        }

        let mut data_files = Vec::with_capacity(data_files_len);
        for i in 0..data_files_len {
            let len = data_file_offsets[i + 1] - data_file_offsets[i];
            let mut bytes = vec![0u8; len];
            reader.read(&mut bytes)?;
            let data_file = String::from_utf8(bytes).unwrap();
            data_files.push(data_file);
        }

        let mut puffin_files = Vec::with_capacity(puffin_files_len);
        for i in 0..puffin_files_len {
            let len = puffin_file_offsets[i + 1] - puffin_file_offsets[i];
            let mut bytes = vec![0u8; len];
            reader.read(&mut bytes)?;
            let puffin_file = String::from_utf8(bytes).unwrap();
            puffin_files.push(puffin_file);
        }

        Ok(Self {
            data_files,
            puffin_files,
            deletion_vectors,
            position_deletes,
        })
    }
}

fn write_u32<W: Writer>(writer: &mut W, value: u32) -> Result<(), EncodeError> {
    writer.write(&value.to_ne_bytes())
}

fn write_usize<W: Writer>(writer: &mut W, value: usize) -> Result<(), EncodeError> {
    let value = u32::try_from(value).map_err(|_| EncodeError::Other("out of range"))?;
    write_u32(writer, value)
}

fn read_u32<R: Reader>(reader: &mut R) -> Result<u32, DecodeError> {
    let mut bytes = [0; 4];
    reader.read(&mut bytes)?;
    Ok(u32::from_ne_bytes(bytes))
}

fn read_usize<R: Reader>(reader: &mut R) -> Result<usize, DecodeError> {
    read_u32(reader).map(|value| value as usize)
}

#[cfg(test)]
mod tests {
    use super::*;
    use bincode::config;

    const BINCODE_CONFIG: config::Configuration = config::standard();

    /// Util function to create a puffin deletion blob.
    fn create_puffin_deletion_blob_1() -> (String /*puffin filepath*/, DeletionVector) {
        let deletion_blob = DeletionVector {
            data_file_number: 0,
            puffin_file_number: 0,
            offset: 4,
            size: 10,
        };
        let puffin_filepath = "/tmp/iceberg_test/1-puffin.bin".to_string();
        (puffin_filepath, deletion_blob)
    }
    fn create_puffin_deletion_blob_2() -> (String /*puffin filepath*/, DeletionVector) {
        let deletion_blob = DeletionVector {
            data_file_number: 0,
            puffin_file_number: 1,
            offset: 4,
            size: 20,
        };
        let puffin_filepath = "/tmp/iceberg_test/2-puffin.bin".to_string();
        (puffin_filepath, deletion_blob)
    }

    #[test]
    fn test_table_metadata_serde() {
        let (puffin_file_1, deletion_blob_1) = create_puffin_deletion_blob_1();
        let (puffin_file_2, deletion_blob_2) = create_puffin_deletion_blob_2();
        let table_metadata = MooncakeTableMetadata {
            data_files: vec![
                "/tmp/iceberg_test/data/1.parquet".to_string(),
                "/tmp/iceberg_test/data/2.parquet".to_string(),
                "/tmp/iceberg-rust/data/temp.parquet".to_string(), // associate file
            ],
            puffin_files: vec![puffin_file_1, puffin_file_2],
            deletion_vectors: vec![deletion_blob_1, deletion_blob_2],
            position_deletes: vec![PositionDelete {
                data_file_number: 2,
                data_file_row_number: 2,
            }],
        };
        let data = bincode::encode_to_vec(table_metadata.clone(), BINCODE_CONFIG).unwrap();

        let decoded_metadata: (MooncakeTableMetadata, usize) =
            bincode::decode_from_slice(&data, config::standard()).unwrap();
        assert_eq!(table_metadata, decoded_metadata.0);
    }
}
