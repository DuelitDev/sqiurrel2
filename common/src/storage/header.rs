use super::codec::{Decoder, Encoder};
use super::error::{Result, StorageErr};
use std::io::{Read, Seek, SeekFrom, Write};

pub const MAGIC: u32 = 0x4c525153;
pub const VERSION: u8 = 2;
pub const HEADER_LEN: u8 = 64;

#[derive(Debug)]
pub struct FileHeader {
    pub flags: u16,
}

impl FileHeader {
    pub fn new() -> Self {
        Self { flags: 0 }
    }

    pub fn write_to(&self, w: &mut impl Write) -> Result<()> {
        let mut e = Encoder::new();
        e.u32(MAGIC);
        e.u8(VERSION);
        e.u8(HEADER_LEN);
        e.u16(self.flags);
        w.write_all(e.as_slice())?;
        w.write_all(&[0u8; 56])?;
        Ok(())
    }

    pub fn read_from(r: &mut impl Read) -> Result<Self> {
        let mut d = Decoder::new(r);
        if d.u32()? != MAGIC {
            return Err(StorageErr::Corrupted("magic mismatch".into()));
        } else if d.u8()? != VERSION {
            return Err(StorageErr::Corrupted("unsupported version".into()));
        } else if d.u8()? != HEADER_LEN {
            return Err(StorageErr::Corrupted("unexpected header length".into()));
        }
        let flags = d.u16()?;
        Ok(Self { flags })
    }

    pub fn flush_to(&self, w: &mut (impl Write + Seek)) -> Result<()> {
        w.seek(SeekFrom::Start(0))?;
        self.write_to(w)?;
        Ok(())
    }
}
