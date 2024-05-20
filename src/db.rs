use bytes::Bytes;
use log::warn;
use parking_lot::{Mutex, RwLock};
use std::{
    collections::HashMap,
    fs::{create_dir_all, read_dir},
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use crate::{
    batch::{log_record_key_with_seq, parse_log_record_key, NON_TXN_SEQ_NO},
    data::{
        data_file::{DataFile, DATA_FILE_NAME_SUFFIX},
        log_record::{LogRecord, LogRecordPos, LogRecordType, TransactionLogRecord},
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
    pub(crate) index: Box<dyn Indexer>,
    /// 数据文件列表，保存所有文件 id
    file_ids: Vec<u32>,
    /// 事务提交串行化
    pub(crate) batch_commit_lock: Mutex<()>,
    /// 事务序列号，全局递增
    pub(crate) seq_no: Arc<AtomicUsize>,
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
        data_files.reverse();
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
            batch_commit_lock: Mutex::new(()),
            seq_no: Arc::new(AtomicUsize::new(1)),
        };

        // 从数据文件中加载内存索引
        engine.load_index()?;

        Ok(engine)
    }

    /// 关闭存储引擎，释放相关资源
    pub fn close(&self) -> Result<(), Errors> {
        let read_guard = self.active_file.read();
        read_guard.sync()
    }

    /// 持久化当前活跃文件
    pub fn sync(&self) -> Result<(), Errors> {
        let read_guard = self.active_file.read();
        read_guard.sync()
    }

    /// 存储 key/value 数据，key 不能为空
    pub fn put(&self, key: Bytes, value: Bytes) -> Result<(), Errors> {
        if key.is_empty() {
            return Err(Errors::KeyIsEmpty);
        }

        // 构造 LogRecord 并写入当前活跃文件
        let log_record = LogRecord {
            key: log_record_key_with_seq(key.to_vec(), NON_TXN_SEQ_NO),
            value: value.to_vec(),
            rec_type: LogRecordType::NOAMAL,
        };
        let log_record_pos = self.append_log_record(log_record)?;

        // 更新内存索引
        match self.index.put(key.to_vec(), log_record_pos) {
            true => Ok(()),
            false => Err(Errors::FailedIndexUpdate),
        }
    }

    /// 根据 key 获取数据
    pub fn get(&self, key: Bytes) -> Result<Bytes, Errors> {
        if key.is_empty() {
            return Err(Errors::KeyIsEmpty);
        }

        let log_record_pos = self.get_log_record_pos(&key)?;

        // 从数据文件中读取 LogRecord
        self.get_value_by_position(log_record_pos)
    }

    // 根据 LogRecord 位置信息读取相应的 value
    pub(crate) fn get_value_by_position(&self, pos: LogRecordPos) -> Result<Bytes, Errors> {
        let active_file = self.active_file.read();
        let older_files = self.older_files.read();
        let file_id = pos.file_id;
        let log_record = match file_id == active_file.get_file_id() {
            true => active_file.read(pos.offset)?.record,
            false => {
                let data_file = older_files.get(&file_id);
                if data_file.is_none() {
                    return Err(Errors::DataFileIsNotFound);
                }
                data_file.unwrap().read(pos.offset)?.record
            }
        };

        match log_record.rec_type {
            LogRecordType::DELETED => Err(Errors::KeyIsNotFound),
            _ => Ok(log_record.value.into()),
        }
    }

    /// 根据 key 删除数据
    pub fn delete(&self, key: Bytes) -> Result<(), Errors> {
        if key.is_empty() {
            return Ok(());
        }

        let res = self.get_log_record_pos(&key);
        if res.is_err() {
            return Ok(());
        }

        // 构造 LogRecord，标识为删除值并写入当前活跃文件
        let log_record = LogRecord {
            key: log_record_key_with_seq(key.to_vec(), NON_TXN_SEQ_NO),
            value: Default::default(),
            rec_type: LogRecordType::DELETED,
        };
        self.append_log_record(log_record)?;

        // 更新内存索引
        match self.index.delete(key.to_vec()) {
            true => Ok(()),
            false => Err(Errors::FailedIndexUpdate),
        }
    }

    // 从内存索引中获取数据位置信息
    fn get_log_record_pos(&self, key: &Bytes) -> Result<LogRecordPos, Errors> {
        let pos = self.index.get(key.to_vec());
        if pos.is_none() {
            return Err(Errors::KeyIsNotFound);
        }
        Ok(pos.unwrap())
    }

    // 追加写入数据到当前活跃文件中
    pub(crate) fn append_log_record(&self, log_record: LogRecord) -> Result<LogRecordPos, Errors> {
        let dir_path = self.options.dir_path.clone();

        // 编码写入数据
        let enc_record = log_record.encode();
        let record_len = enc_record.len() as u64;

        // 获取当前活跃文件
        let mut active_file = self.active_file.write();

        // 判断当前活跃文件是否达到阈值，是则持久化当前活跃文件
        // 并将其存储到旧文件列表，最后打开一个新的活跃文件
        if active_file.get_write_off() + record_len > self.options.data_file_size {
            active_file.sync()?;

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

        // 根据配置项决定是否持久化
        if self.options.sync_writes {
            active_file.sync()?;
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

        // 暂存事务序列号和事务内所有数据的信息
        let mut txn_batch: HashMap<usize, Vec<TransactionLogRecord>> = HashMap::new();

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
                let (mut log_record, size) = match log_record_res {
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

                // 解析 key ,拿到实际 key 和事务序列号
                let (real_key, seq_no) = parse_log_record_key(log_record.key);

                // 更新事务序列号
                self.seq_no.fetch_max(seq_no + 1, Ordering::SeqCst);

                // 非事务数据直接更新索引，事务数据先暂存，读到 TXN_FIN_KEY 统一更新索引
                if seq_no == NON_TXN_SEQ_NO {
                    let ok = self.update_index(real_key, log_record.rec_type, log_record_pos);
                    if !ok {
                        return Err(Errors::FailedIndexUpdate);
                    }
                } else {
                    if log_record.rec_type == LogRecordType::TXNFINISHED {
                        let records = txn_batch.get(&seq_no).unwrap();
                        for txn_record in records.iter() {
                            self.update_index(
                                txn_record.record.key.clone(),
                                txn_record.record.rec_type,
                                txn_record.pos,
                            );
                        }
                        txn_batch.remove(&seq_no);
                    } else {
                        log_record.key = real_key;
                        txn_batch
                            .entry(seq_no)
                            .or_insert(Vec::new())
                            .push(TransactionLogRecord {
                                record: log_record,
                                pos: log_record_pos,
                            });
                    }
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

    // 更新内存索引
    pub(crate) fn update_index(
        &self,
        key: Vec<u8>,
        rec_type: LogRecordType,
        pos: LogRecordPos,
    ) -> bool {
        match rec_type {
            LogRecordType::NOAMAL => self.index.put(key, pos),
            LogRecordType::DELETED => self.index.delete(key),
            _ => true,
        }
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
