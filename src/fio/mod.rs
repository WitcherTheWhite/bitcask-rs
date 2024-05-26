pub mod file_io;
pub mod mmap;

use std::path::PathBuf;

use crate::{errors::Errors, options::IOType};

use self::{file_io::FileIO, mmap::MMapIO};

/// 抽象 IO 管理接口
pub trait IOManager: Sync + Send {
    /// 从 offset 开始读取对应的数据
    fn read(&self, buf: &mut [u8], offset: u64) -> Result<usize, Errors>;

    /// 写入字节流到文件中
    fn write(&self, buf: &[u8]) -> Result<usize, Errors>;

    /// 持久化数据
    fn sync(&self) -> Result<(), Errors>;

    /// 获取文件大小
    fn size(&self) -> u64;
}

/// 根据数据文件路径初始化 IOManager
pub fn new_io_manager(file_path: PathBuf, io_type: IOType) -> Box<dyn IOManager> {
    match io_type {
        IOType::FileIO => Box::new(FileIO::new(file_path).unwrap()),
        IOType::MMapIO => Box::new(MMapIO::new(file_path).unwrap()),
    }
}
