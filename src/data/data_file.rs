use std::path::PathBuf;

use bytes::{Buf, BytesMut};
use prost::{decode_length_delimiter, length_delimiter_len};

use crate::{
    data::log_record::{LogRecord, LogRecordType},
    errors::Errors,
    fio::{new_io_manager, IOManager},
};

use super::log_record::{max_log_record_header_size, ReadLogRecord};

pub const DATA_FILE_NAME_SUFFIX: &str = ".data";

/// 存储引擎数据文件实例
pub struct DataFile {
    file_id: u32,                   // 数据文件 id
    write_off: u64,                 // 当前写偏移
    io_manager: Box<dyn IOManager>, // IO 管理接口
}

impl DataFile {
    pub fn new(dir_path: PathBuf, file_id: u32) -> Result<DataFile, Errors> {
        let file_path = get_data_file_path(dir_path, file_id);
        let io_manager = new_io_manager(file_path)?;

        Ok(DataFile {
            file_id,
            write_off: 0,
            io_manager: Box::new(io_manager),
        })
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

    /// 从数据文件中读取 LogRecord
    pub fn read(&self, offset: u64) -> Result<ReadLogRecord, Errors> {
        // 先读出 header 部分的数据，header = LogRecord类型 + key长度 + value长度
        let mut header_buf = BytesMut::zeroed(max_log_record_header_size());
        self.io_manager.read(&mut header_buf, offset)?;
        let rec_type = header_buf.get_u8();
        let key_size = decode_length_delimiter(&mut header_buf).unwrap();
        let value_size = decode_length_delimiter(&mut header_buf).unwrap();
        if key_size == 0 && value_size == 0 {
            return Err(Errors::ReadDataFileEOF);
        }
        let header_size = length_delimiter_len(key_size) + length_delimiter_len(value_size) + 1;

        // 读取 key/value 数据和最后 4 字节 CRC 校验值
        let mut kv_buf = BytesMut::zeroed(key_size + value_size + 4);
        self.io_manager
            .read(&mut kv_buf, offset + header_size as u64)?;

        // 构造 LogRecord
        let log_record = LogRecord {
            key: kv_buf.get(..key_size).unwrap().to_vec(),
            value: kv_buf.get(key_size..kv_buf.len() - 4).unwrap().to_vec(),
            rec_type: LogRecordType::from_u8(rec_type),
        };

        // 校验 CRC 验证数据完整性
        kv_buf.advance(key_size + value_size);
        if kv_buf.get_u32() != log_record.get_crc() {
            return Err(Errors::InvalidLogRecordCrc);
        }

        Ok(ReadLogRecord {
            record: log_record,
            size: (header_size + kv_buf.len()) as u64,
        })
    }

    pub fn write(&mut self, buf: &[u8]) -> Result<usize, Errors> {
        let n_bytes = self.io_manager.write(buf)?;
        self.write_off += n_bytes as u64;
        Ok(n_bytes)
    }

    pub fn sync(&self) -> Result<(), Errors>{
        self.io_manager.sync()
    }
}

// 根据 dir_path 和 file_id 构建数据文件路径
fn get_data_file_path(dir_path: PathBuf, file_id: u32) -> PathBuf {
    let file_name = std::format!("{:09}", file_id) + DATA_FILE_NAME_SUFFIX;
    dir_path.join(file_name)
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_new_data_file() {
        let dir_path = std::env::temp_dir();

        let data_file_res1 = DataFile::new(dir_path.clone(), 0);
        assert!(data_file_res1.is_ok());
        let data_file1 = data_file_res1.unwrap();
        assert_eq!(data_file1.get_file_id(), 0);
        assert_eq!(data_file1.get_write_off(), 0);

        let data_file_res2 = DataFile::new(dir_path.clone(), 0);
        assert!(data_file_res2.is_ok());
        let data_file2 = data_file_res2.unwrap();
        assert_eq!(data_file2.get_file_id(), 0);
        assert_eq!(data_file2.get_write_off(), 0);

        let data_file_res3 = DataFile::new(dir_path.clone(), 666);
        assert!(data_file_res3.is_ok());
        let data_file3 = data_file_res3.unwrap();
        assert_eq!(data_file3.get_file_id(), 666);
        assert_eq!(data_file3.get_write_off(), 0);
    }

    #[test]
    fn test_data_file_write() {
        let dir_path = std::env::temp_dir();

        let data_file_res1 = DataFile::new(dir_path.clone(), 0);
        assert!(data_file_res1.is_ok());
        let mut data_file1 = data_file_res1.unwrap();
        assert_eq!(data_file1.get_file_id(), 0);
        assert_eq!(data_file1.get_write_off(), 0);

        let write_res1 = data_file1.write("hsy".as_bytes());
        assert!(write_res1.is_ok());
        assert_eq!(write_res1.unwrap(), 3);

        let write_res2 = data_file1.write("hahaha".as_bytes());
        assert!(write_res2.is_ok());
        assert_eq!(write_res2.unwrap(), 6);
    }

    #[test]
    fn test_data_file_sync() {
        let dir_path = std::env::temp_dir();

        let data_file_res1 = DataFile::new(dir_path.clone(), 0);
        assert!(data_file_res1.is_ok());
        let mut data_file1 = data_file_res1.unwrap();
        assert_eq!(data_file1.get_file_id(), 0);
        assert_eq!(data_file1.get_write_off(), 0);

        let write_res1 = data_file1.write("hsy".as_bytes());
        assert!(write_res1.is_ok());
        assert_eq!(write_res1.unwrap(), 3);

        let sync_res = data_file1.sync();
        assert!(sync_res.is_ok())
    }
}