use std::path::PathBuf;

use crate::{errors::Errors, fio::IOManager};

use super::log_record::ReadLogRecord;

pub const DATA_FILE_NAME_SUFFIX: &str = ".data";

pub struct DataFile {
    file_id: u32,                   // 数据文件 id
    write_off: u64,                 // 当前写偏移
    io_manager: Box<dyn IOManager>, // IO 管理接口
}

impl DataFile {
    pub fn new(dir_path: PathBuf, file_id: u32) -> Result<DataFile, Errors> {
        todo!()
    }

    pub fn get_file_id(&self) -> u32 {
        self.file_id
    }

    pub fn get_write_off(&self) -> u64 {
        self.write_off
    }

    pub fn set_write_off(&mut self, offset: u64) {
        self.write_off = offset
    }

    pub fn read(&self, offset: u64) -> Result<ReadLogRecord, Errors> {
        todo!()
    }

    pub fn write(&self, buf: &[u8]) -> Result<usize, Errors> {
        todo!()
    }

    pub fn sync(&self) {
        todo!()
    }
}
