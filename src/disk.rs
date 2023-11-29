use std::{
    fs::{File, OpenOptions},
    os::unix::prelude::FileExt,
};

#[derive(Clone, Copy, PartialEq, Eq, PartialOrd, Ord)]
pub struct PageId(u64);

pub struct DiskManager {
    page_size: u64,
    heap_file: File,
}

impl DiskManager {
    pub fn from_file(file: File, page_size: u64) -> Self {
        Self {
            page_size,
            heap_file: file,
        }
    }
    pub fn from_path(path: impl AsRef<std::path::Path>, page_size: u64) -> std::io::Result<Self> {
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(path)?;
        Ok(Self::from_file(file, page_size))
    }
    pub(crate) fn read_at(&self, offset: u64, buf: &mut [u8]) -> std::io::Result<usize> {
        self.heap_file.read_at(buf, offset)
    }
    pub(crate) fn write_at(&self, offset: u64, buf: &[u8]) -> std::io::Result<usize> {
        self.heap_file.write_at(buf, offset)
    }
    pub fn read_page(&self, page_id: PageId, data: &mut [u8]) -> std::io::Result<usize> {
        let offset = self.page_size * page_id.0;
        self.read_at(offset, data)
    }
    pub fn write_page(&self, page_id: PageId, data: &[u8]) -> std::io::Result<usize> {
        let offset = self.page_size * page_id.0;
        self.write_at(offset, data)
    }
    pub fn sync(&self) -> std::io::Result<()> {
        self.heap_file.sync_all()
    }
}
