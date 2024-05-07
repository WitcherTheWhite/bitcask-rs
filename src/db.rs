use bytes::Bytes;
use parking_lot::RwLock;
use std::{collections::HashMap, sync::Arc};

use crate::{
    data::{
        data_file::DataFile,
        log_record::{LogRecord, LogRecordPos, LogRecordType},
    },
    errors::Errors,
    index::Indexer,
    options::Options,
};

/// bitcask 存储引擎实例
pub struct Engine {
    options: Arc<Options>,
    /// 当前活跃文件，用于写入新的数据
    active_file: Arc<RwLock<DataFile>>,
    /// 旧文件列表，保存文件 id 和 DafaFile 的映射关系
    older_files: Arc<RwLock<HashMap<u32, DataFile>>>,
    /// 数据内存索引
    index: Box<dyn Indexer>,
}

impl Engine {
    /// 存储 key/value 数据，key 不能为空
    pub fn put(&self, key: Bytes, value: Bytes) -> Result<(), Errors> {
        if key.is_empty() {
            return Err(Errors::KeyIsEmpty);
        }

        // 构造 LogRecord
        let log_record = LogRecord {
            key: key.to_vec(),
            value: value.to_vec(),
            rec_type: LogRecordType::NOAMAL,
        };

        let log_record_pos = self.append_log_record(log_record)?;

        // 更新内存索引
        let ok = self.index.put(key.to_vec(), log_record_pos);
        if !ok {
            return Err(Errors::FailedIndexUpdate);
        }

        Ok(())
    }

    /// 根据 key 获取数据
    pub fn get(&self, key: Bytes) -> Result<Bytes, Errors> {
        if key.is_empty() {
            return Err(Errors::KeyIsEmpty);
        }

        // 从内存索引中获取数据位置信息
        let pos = self.index.get(key.to_vec());
        if pos.is_none() {
            return Err(Errors::KeyIsNotFound);
        }
        let log_record_pos = pos.unwrap();

        // 从数据文件中读取 LogRecord
        let active_file = self.active_file.read();
        let older_files = self.older_files.read();
        let file_id = log_record_pos.file_id;
        let log_record = match file_id == active_file.get_file_id() {
            true => active_file.read(log_record_pos.offset)?,
            false => {
                let data_file = older_files.get(&file_id);
                if data_file.is_none() {
                    return Err(Errors::DataFileIsNotFound);
                }
                data_file.unwrap().read(log_record_pos.offset)?
            }
        };

        match log_record.rec_type {
            LogRecordType::DELETED => Err(Errors::KeyIsNotFound),
            _ => Ok(log_record.value.into()),
        }
    }

    // 追加写入数据到当前活跃文件中
    fn append_log_record(&self, log_record: LogRecord) -> Result<LogRecordPos, Errors> {
        let dir_path = self.options.dir_path.clone();

        // 编码写入数据
        let enc_record = log_record.encode();
        let record_len = enc_record.len() as u64;

        // 获取当前活跃文件
        let mut active_file = self.active_file.write();

        // 判断当前活跃文件是否达到阈值，是则持久化当前活跃文件
        // 并将其存储到旧文件列表，最后打开一个新的活跃文件
        if active_file.get_write_off() + record_len > self.options.data_file_size {
            active_file.sync();

            let mut older_files = self.older_files.write();
            let current_fid = active_file.get_file_id();
            let old_file = DataFile::new(dir_path.clone(), current_fid).unwrap();
            older_files.insert(current_fid, old_file);

            let new_file = DataFile::new(dir_path, current_fid + 1).unwrap();
            *active_file = new_file;
        }

        // 追加写入数据
        let write_off = active_file.get_write_off();
        active_file.write(&enc_record)?;

        // 根据配置项决定是否初始化
        if self.options.sync_writes {
            active_file.sync();
        }

        Ok(LogRecordPos {
            file_id: active_file.get_file_id(),
            offset: write_off,
        })
    }
}
