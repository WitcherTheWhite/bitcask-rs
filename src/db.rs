use bytes::Bytes;
use log::warn;
use parking_lot::RwLock;
use std::{
    collections::HashMap,
    fs::{create_dir_all, read_dir},
    path::PathBuf,
    sync::Arc,
};

use crate::{
    data::{
        data_file::{DataFile, DATA_FILE_NAME_SUFFIX},
        log_record::{LogRecord, LogRecordPos, LogRecordType},
    },
    errors::Errors,
    index::{self, Indexer},
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
    /// 数据文件列表，保存所有文件 id
    file_ids: Vec<u32>,
}

impl Engine {
    /// 打开 bitcask 存储引擎实例
    pub fn open(options: Options) -> Result<Self, Errors> {
        // 校验用户输入配置项
        if let Some(e) = check_options(&options) {
            return Err(e);
        }

        // 如果数据目录不存在则新建
        let dir_path = options.dir_path.clone();
        if !dir_path.is_dir() {
            if let Err(e) = create_dir_all(dir_path.clone()) {
                warn!("create database directory err: {}", e);
                return Err(Errors::FailedCreateDatabaseDir);
            }
        }

        // 加载数据文件
        let mut data_files = load_data_files(dir_path.clone())?;

        // 创建数据文件列表
        let mut file_ids = Vec::new();
        for data_file in data_files.iter() {
            file_ids.push(data_file.get_file_id())
        }

        // 将旧数据文件加入到 older_files
        let mut older_files = HashMap::new();
        if data_files.len() > 1 {
            for _ in 0..=data_files.len() - 2 {
                let data_file = data_files.pop().unwrap();
                older_files.insert(data_file.get_file_id(), data_file);
            }
        }

        // 获取当前活跃文件
        let active_file = match data_files.pop() {
            Some(file) => file,
            None => DataFile::new(dir_path.clone(), 0)?,
        };

        let mut engine = Engine {
            options: Arc::new(options.clone()),
            active_file: Arc::new(RwLock::new(active_file)),
            older_files: Arc::new(RwLock::new(older_files)),
            index: Box::new(index::new_indexer(options.index_type)),
            file_ids,
        };

        // 从数据文件中加载内存索引
        engine.load_index()?;

        Ok(engine)
    }

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
            true => active_file.read(log_record_pos.offset)?.record,
            false => {
                let data_file = older_files.get(&file_id);
                if data_file.is_none() {
                    return Err(Errors::DataFileIsNotFound);
                }
                data_file.unwrap().read(log_record_pos.offset)?.record
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

    // 从数据文件中加载内存索引
    fn load_index(&mut self) -> Result<(), Errors> {
        if self.file_ids.is_empty() {
            return Ok(());
        }

        let mut active_file = self.active_file.write();
        let older_files = self.older_files.read();

        // 读取所有数据文件并构建内存索引
        for (i, file_id) in self.file_ids.iter().enumerate() {
            let mut offset = 0;
            loop {
                let log_record_res = match *file_id == active_file.get_file_id() {
                    true => active_file.read(offset),
                    false => {
                        let data_file = older_files.get(file_id).unwrap();
                        data_file.read(offset)
                    }
                };

                // 读到文件末尾则继续读下个文件
                let (log_record, size) = match log_record_res {
                    Ok(r) => (r.record, r.size),
                    Err(e) => {
                        if e == Errors::ReadDataFileEOF {
                            break;
                        }
                        return Err(e);
                    }
                };

                // 构建内存索引
                let log_record_pos = LogRecordPos {
                    file_id: *file_id,
                    offset,
                };

                match log_record.rec_type {
                    LogRecordType::NOAMAL => self.index.put(log_record.key, log_record_pos),
                    LogRecordType::DELETED => self.index.delete(log_record.key),
                };

                offset += size;
            }

            // 如果是当前活跃文件，更新其 write_off
            if i == self.file_ids.len() - 1 {
                active_file.set_write_off(offset)
            }
        }

        Ok(())
    }
}

fn check_options(options: &Options) -> Option<Errors> {
    let dir_path = options.dir_path.to_str();
    if dir_path.is_none() || dir_path.unwrap().len() == 0 {
        return Some(Errors::DirPathIsEmpty);
    }

    if options.data_file_size <= 0 {
        return Some(Errors::DataFileSizeInvalid);
    }

    None
}

fn load_data_files(dir_path: PathBuf) -> Result<Vec<DataFile>, Errors> {
    let dir = read_dir(dir_path.clone());
    if dir.is_err() {
        return Err(Errors::FailedOpenDatabaseDir);
    }

    let mut file_ids = Vec::new();
    let mut data_files = Vec::new();
    for file in dir.unwrap() {
        if let Ok(entry) = file {
            let os_string = entry.file_name();
            let file_name = os_string.to_str().unwrap();

            // 数据文件名用 .data 作为后缀
            if file_name.ends_with(DATA_FILE_NAME_SUFFIX) {
                let split_names: Vec<&str> = file_name.split(".").collect();
                let file_id = match split_names[0].parse::<u32>() {
                    Ok(fid) => fid,
                    Err(_) => {
                        return Err(Errors::DataDirCorrupted);
                    }
                };
                file_ids.push(file_id)
            }
        }
    }

    if file_ids.is_empty() {
        return Ok(data_files);
    }

    file_ids.sort();
    for file_id in file_ids.iter() {
        let data_file = DataFile::new(dir_path.clone(), *file_id)?;
        data_files.push(data_file);
    }

    Ok(data_files)
}
