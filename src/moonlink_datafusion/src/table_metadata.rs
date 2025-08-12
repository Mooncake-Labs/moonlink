use bincode::de::read::Reader;
use bincode::de::{Decode, Decoder};
use bincode::error::DecodeError;

#[derive(Debug)]
pub(crate) struct DeletionVector {
    pub(crate) data_file_number: u32,
    pub(crate) puffin_file_number: u32,
    pub(crate) offset: u32,
    pub(crate) size: u32,
}

#[derive(Debug)]
pub(crate) struct PositionDelete {
    pub(crate) data_file_number: u32,
    pub(crate) data_file_row_number: u32,
}

#[derive(Debug)]
pub(crate) struct MooncakeTableMetadata {
    pub(crate) data_files: Vec<String>,
    pub(crate) puffin_files: Vec<String>,
    pub(crate) deletion_vectors: Vec<DeletionVector>,
    pub(crate) position_deletes: Vec<PositionDelete>,
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

fn read_u32<R: Reader>(reader: &mut R) -> Result<u32, DecodeError> {
    let mut bytes = [0; 4];
    reader.read(&mut bytes)?;
    Ok(u32::from_ne_bytes(bytes))
}

fn read_usize<R: Reader>(reader: &mut R) -> Result<usize, DecodeError> {
    read_u32(reader).map(|value| value as usize)
}
