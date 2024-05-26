use std::path::PathBuf;

use bytes::{Buf, BytesMut};
use prost::{decode_length_delimiter, length_delimiter_len};

use crate::{
    data::log_record::{LogRecord, LogRecordType},
    errors::Errors,
    fio::{new_io_manager, IOManager},
    options::IOType,
};

use super::log_record::{max_log_record_header_size, LogRecordPos, ReadLogRecord};

pub const DATA_FILE_NAME_SUFFIX: &str = ".data";
pub(crate) const HINT_FILE_NAME: &str = "hint-index";
pub(crate) const MERGE_FINISHED_FILE_NAME: &str = "merge-finished";
pub const SEQ_NO_FILE_NAME: &str = "seq-no";

/// 存储引擎数据文件实例
pub struct DataFile {
    file_id: u32,                   // 数据文件 id
    write_off: u64,                 // 当前写偏移
    io_manager: Box<dyn IOManager>, // IO 管理接口
}

impl DataFile {
    pub fn new(dir_path: PathBuf, file_id: u32, io_type: IOType) -> Result<DataFile, Errors> {
        let file_path = get_data_file_path(dir_path, file_id);
        let io_manager = new_io_manager(file_path, io_type);

        Ok(DataFile {
            file_id,
            write_off: 0,
            io_manager,
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

    pub fn file_size(&self) -> u64 {
        self.io_manager.size()
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
            size: (header_size + key_size + value_size + 4) as u64,
        })
    }

    pub fn write(&mut self, buf: &[u8]) -> Result<usize, Errors> {
        let n_bytes = self.io_manager.write(buf)?;
        self.write_off += n_bytes as u64;
        Ok(n_bytes)
    }

    pub fn sync(&self) -> Result<(), Errors> {
        self.io_manager.sync()
    }

    pub fn set_io_manager(&mut self, dir_path: PathBuf, io_type: IOType) {
        self.io_manager = new_io_manager(get_data_file_path(dir_path, self.file_id), io_type)
    }

    // 创建 hint 索引文件，用于启动时快速构建索引
    pub fn new_hint_file(dir_path: PathBuf) -> Result<DataFile, Errors> {
        let file_path = dir_path.join(HINT_FILE_NAME);
        let io_manager = new_io_manager(file_path, IOType::FileIO);

        Ok(DataFile {
            file_id: 0,
            write_off: 0,
            io_manager,
        })
    }

    // 写入 key 的索引信息
    pub fn write_hint_record(&mut self, key: Vec<u8>, pos: LogRecordPos) -> Result<(), Errors> {
        let hint_record = LogRecord {
            key,
            value: pos.encode(),
            rec_type: LogRecordType::NOAMAL,
        };
        let enc_record = hint_record.encode();
        self.write(&enc_record)?;

        Ok(())
    }

    // 标识 merge 完成的文件
    pub fn new_merge_finished_file(dir_path: PathBuf) -> Result<DataFile, Errors> {
        let file_path = dir_path.join(MERGE_FINISHED_FILE_NAME);
        let io_manager = new_io_manager(file_path, IOType::FileIO);

        Ok(DataFile {
            file_id: 0,
            write_off: 0,
            io_manager,
        })
    }

    /// 新建或打开存储事务序列号的文件
    pub fn new_seq_no_file(dir_path: PathBuf) -> Result<DataFile, Errors> {
        let file_path = dir_path.join(SEQ_NO_FILE_NAME);
        let io_manager = new_io_manager(file_path, IOType::FileIO);

        Ok(DataFile {
            file_id: 0,
            write_off: 0,
            io_manager,
        })
    }
}

// 根据 dir_path 和 file_id 构建数据文件路径
pub(crate) fn get_data_file_path(dir_path: PathBuf, file_id: u32) -> PathBuf {
    let file_name = std::format!("{:09}", file_id) + DATA_FILE_NAME_SUFFIX;
    dir_path.join(file_name)
}

#[cfg(test)]
mod tests {
    use std::fs::remove_file;

    use super::*;

    #[test]
    fn test_new_data_file() {
        let dir_path = std::env::temp_dir();

        let data_file_res1 = DataFile::new(dir_path.clone(), 0, IOType::FileIO);
        assert!(data_file_res1.is_ok());
        let data_file1 = data_file_res1.unwrap();
        assert_eq!(data_file1.get_file_id(), 0);
        assert_eq!(data_file1.get_write_off(), 0);
        let remove_res1 = remove_file(get_data_file_path(
            dir_path.clone(),
            data_file1.get_file_id(),
        ));
        assert!(remove_res1.is_ok());

        let data_file_res2 = DataFile::new(dir_path.clone(), 0, IOType::FileIO);
        assert!(data_file_res2.is_ok());
        let data_file2 = data_file_res2.unwrap();
        assert_eq!(data_file2.get_file_id(), 0);
        assert_eq!(data_file2.get_write_off(), 0);
        let remove_res2 = remove_file(get_data_file_path(
            dir_path.clone(),
            data_file2.get_file_id(),
        ));
        assert!(remove_res2.is_ok());

        let data_file_res3 = DataFile::new(dir_path.clone(), 1, IOType::FileIO);
        assert!(data_file_res3.is_ok());
        let data_file3 = data_file_res3.unwrap();
        assert_eq!(data_file3.get_file_id(), 1);
        assert_eq!(data_file3.get_write_off(), 0);
        let remove_res3 = remove_file(get_data_file_path(
            dir_path.clone(),
            data_file3.get_file_id(),
        ));
        assert!(remove_res3.is_ok());
    }

    #[test]
    fn test_data_file_write() {
        let dir_path = std::env::temp_dir();

        let data_file_res1 = DataFile::new(dir_path.clone(), 2, IOType::FileIO);
        assert!(data_file_res1.is_ok());
        let mut data_file1 = data_file_res1.unwrap();
        assert_eq!(data_file1.get_file_id(), 2);
        assert_eq!(data_file1.get_write_off(), 0);

        let write_res1 = data_file1.write("hsy".as_bytes());
        assert!(write_res1.is_ok());
        assert_eq!(write_res1.unwrap(), 3);

        let write_res2 = data_file1.write("hahaha".as_bytes());
        assert!(write_res2.is_ok());
        assert_eq!(write_res2.unwrap(), 6);

        let remove_res1 = remove_file(get_data_file_path(
            dir_path.clone(),
            data_file1.get_file_id(),
        ));
        assert!(remove_res1.is_ok());
    }

    #[test]
    fn test_data_file_sync() {
        let dir_path = std::env::temp_dir();

        let data_file_res1 = DataFile::new(dir_path.clone(), 3, IOType::FileIO);
        assert!(data_file_res1.is_ok());
        let mut data_file1 = data_file_res1.unwrap();
        assert_eq!(data_file1.get_file_id(), 3);
        assert_eq!(data_file1.get_write_off(), 0);

        let write_res1 = data_file1.write("hsy".as_bytes());
        assert!(write_res1.is_ok());
        assert_eq!(write_res1.unwrap(), 3);

        let sync_res = data_file1.sync();
        assert!(sync_res.is_ok());

        let remove_res1 = remove_file(get_data_file_path(
            dir_path.clone(),
            data_file1.get_file_id(),
        ));
        assert!(remove_res1.is_ok());
    }

    #[test]
    fn test_data_file_read_log_record() {
        let dir_path = std::env::temp_dir();
        let data_file_res1 = DataFile::new(dir_path.clone(), 4, IOType::FileIO);
        assert!(data_file_res1.is_ok());
        let mut data_file1 = data_file_res1.unwrap();
        assert_eq!(data_file1.get_file_id(), 4);
        assert_eq!(data_file1.get_write_off(), 0);

        // 从初始位置开始
        let rec1 = LogRecord {
            key: "name".as_bytes().to_vec(),
            value: "hsy".as_bytes().to_vec(),
            rec_type: LogRecordType::NOAMAL,
        };
        let write_res1 = data_file1.write(&rec1.encode());
        assert!(write_res1.is_ok());
        let read_res1 = data_file1.read(0);
        assert!(read_res1.is_ok());
        let read_res1 = read_res1.unwrap();
        let size1 = read_res1.size;
        let read_rec1 = read_res1.record;
        assert_eq!(size1, write_res1.unwrap() as u64);
        assert_eq!(read_rec1.key, rec1.key);
        assert_eq!(read_rec1.value, rec1.value);
        assert_eq!(read_rec1.rec_type, rec1.rec_type);

        // 新的位置开始
        let rec2 = LogRecord {
            key: "name".as_bytes().to_vec(),
            value: "james".as_bytes().to_vec(),
            rec_type: LogRecordType::NOAMAL,
        };
        let write_res2 = data_file1.write(&rec2.encode());
        assert!(write_res2.is_ok());

        let read_res2 = data_file1.read(size1);
        assert!(read_res2.is_ok());
        let read_res2 = read_res2.unwrap();
        let size2 = read_res2.size;
        let read_rec2 = read_res2.record;
        assert_eq!(size2, write_res2.unwrap() as u64);
        assert_eq!(read_rec2.key, rec2.key);
        assert_eq!(read_rec2.value, rec2.value);
        assert_eq!(read_rec2.rec_type, rec2.rec_type);

        // Deleted 数据
        let rec3 = LogRecord {
            key: "name".as_bytes().to_vec(),
            value: Default::default(),
            rec_type: LogRecordType::DELETED,
        };
        let write_res3 = data_file1.write(&rec3.encode());
        assert!(write_res3.is_ok());

        let read_res3 = data_file1.read(size1 + size2);
        assert!(read_res3.is_ok());
        let read_res3 = read_res3.unwrap();
        let size3 = read_res3.size;
        let read_rec3 = read_res3.record;
        assert_eq!(size3, write_res3.unwrap() as u64);
        assert_eq!(read_rec3.key, rec3.key);
        assert_eq!(read_rec3.value, rec3.value);
        assert_eq!(read_rec3.rec_type, rec3.rec_type);

        let remove_res1 = remove_file(get_data_file_path(
            dir_path.clone(),
            data_file1.get_file_id(),
        ));
        assert!(remove_res1.is_ok());
    }
}
