use bytes::Bytes;
use fs2::FileExt;
use log::warn;
use parking_lot::{Mutex, RwLock};
use std::{
    collections::HashMap,
    fs::{self, create_dir_all, read_dir, remove_file, File},
    path::PathBuf,
    sync::{
        atomic::{AtomicUsize, Ordering},
        Arc,
    },
};

use crate::{
    batch::{log_record_key_with_seq, parse_log_record_key, NON_TXN_SEQ_NO},
    data::{
        data_file::{DataFile, DATA_FILE_NAME_SUFFIX, MERGE_FINISHED_FILE_NAME, SEQ_NO_FILE_NAME},
        log_record::{LogRecord, LogRecordPos, LogRecordType, TransactionLogRecord},
    },
    errors::Errors,
    index::{self, Indexer},
    merge::load_merge_files,
    options::{IOType, IndexType, Options},
    util::file::{copy_dir, dir_disk_size},
};

const SEQ_NO_KEY: &str = "seq.no";
pub(crate) const FILE_LOCK_NAME: &str = "flock";

/// bitcask 存储引擎实例
pub struct Engine {
    /// 配置项
    pub(crate) options: Arc<Options>,
    /// 当前活跃文件，用于写入新的数据
    pub(crate) active_file: Arc<RwLock<DataFile>>,
    /// 旧文件列表，保存文件 id 和 DafaFile 的映射关系
    pub(crate) older_files: Arc<RwLock<HashMap<u32, DataFile>>>,
    /// 数据内存索引
    pub(crate) index: Box<dyn Indexer>,
    /// 数据文件列表，保存所有文件 id
    file_ids: Vec<u32>,
    /// 事务提交串行化
    pub(crate) batch_commit_lock: Mutex<()>,
    /// 事务序列号，全局递增
    pub(crate) seq_no: Arc<AtomicUsize>,
    /// 防止多个线程同时 merge
    pub(crate) merging_lock: Mutex<()>,
    /// 不存在则禁止 WriteBatch 使用
    pub(crate) seq_file_exists: bool,
    /// 是否是第一次初始化该目录
    pub(crate) is_initial: bool,
    /// 文件锁，保证单进程使用
    lock_file: File,
    /// 累计写入多少字节
    bytes_write: Arc<AtomicUsize>,
    /// 累计可以 merge 的数据量
    pub(crate) reclaim_size: Arc<AtomicUsize>,
}

/// 存储引擎相关统计信息
#[derive(Debug)]
pub struct Stat {
    /// key 的总数量
    pub key_num: usize,
    /// 数据文件的数量
    pub data_file_num: usize,
    /// 可以回收的数据量
    pub reclaim_size: usize,
    /// 占据磁盘空间大小
    pub disk_size: u64,
}

impl Engine {
    /// 打开 bitcask 存储引擎实例
    pub fn open(options: Options) -> Result<Self, Errors> {
        // 校验用户输入配置项
        if let Some(e) = check_options(&options) {
            return Err(e);
        }

        let mut is_initial = false;
        // 如果数据目录不存在则新建
        let dir_path = options.dir_path.clone();
        if !dir_path.is_dir() {
            is_initial = true;
            if let Err(e) = create_dir_all(dir_path.clone()) {
                warn!("create database directory err: {}", e);
                return Err(Errors::FailedCreateDatabaseDir);
            }
        }

        // 判断数据目录是否已经被使用了
        let lock_file = fs::OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .open(dir_path.join(FILE_LOCK_NAME))
            .unwrap();
        if let Err(_) = lock_file.try_lock_exclusive() {
            return Err(Errors::DatabaseIsUsing);
        }

        let entries = read_dir(dir_path.clone()).unwrap();
        if entries.count() == 0 {
            is_initial = true;
        }

        // 加载 merge 目录
        load_merge_files(dir_path.clone())?;

        // 加载数据文件
        let mut data_files = load_data_files(dir_path.clone(), options.mmap_at_startup)?;

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
            None => DataFile::new(dir_path.clone(), 0, IOType::FileIO)?,
        };

        let mut engine = Engine {
            options: Arc::new(options.clone()),
            active_file: Arc::new(RwLock::new(active_file)),
            older_files: Arc::new(RwLock::new(older_files)),
            index: index::new_indexer(options.index_type, dir_path.clone()),
            file_ids,
            batch_commit_lock: Mutex::new(()),
            seq_no: Arc::new(AtomicUsize::new(1)),
            merging_lock: Mutex::new(()),
            seq_file_exists: false,
            is_initial,
            lock_file,
            bytes_write: Arc::new(AtomicUsize::new(0)),
            reclaim_size: Arc::new(AtomicUsize::new(0)),
        };

        // b+树索引存放在磁盘上，不需要加载数据文件建立索引
        if engine.options.index_type != IndexType::BPlusTree {
            // 从 hint 文件中快速建立索引
            engine.load_index_from_hint_file()?;

            // 从数据文件中加载内存索引
            engine.load_index()?;

            // 重置 IO 类型
            engine.reset_io_type();
        }

        if engine.options.index_type == IndexType::BTree {
            let (exists, seq_no) = engine.load_seq_no();
            engine.seq_file_exists = exists;
            engine.seq_no.store(seq_no, Ordering::SeqCst);

            // 设置当前活跃文件的偏移
            let mut active_file = engine.active_file.write();
            let file_size = active_file.file_size();
            active_file.set_write_off(file_size);
        }

        Ok(engine)
    }

    /// 关闭存储引擎，释放相关资源
    pub fn close(&self) -> Result<(), Errors> {
        // 如果数据目录不存在则返回
        if !self.options.dir_path.is_dir() {
            return Ok(());
        }
        // 记录当前的事务序列号
        let mut seq_no_file = DataFile::new_seq_no_file(self.options.dir_path.clone())?;
        let seq_no = self.seq_no.load(Ordering::SeqCst);
        let record = LogRecord {
            key: SEQ_NO_KEY.as_bytes().to_vec(),
            value: seq_no.to_string().into_bytes(),
            rec_type: LogRecordType::NOAMAL,
        };
        seq_no_file.write(&record.encode())?;
        seq_no_file.sync()?;

        let read_guard = self.active_file.read();
        read_guard.sync()?;

        // 释放文件锁
        self.lock_file.unlock().unwrap();

        Ok(())
    }

    /// 持久化当前活跃文件
    pub fn sync(&self) -> Result<(), Errors> {
        let read_guard = self.active_file.read();
        read_guard.sync()
    }

    /// 获取统计信息
    pub fn stat(&self) -> Result<Stat, Errors> {
        let keys = self.list_keys();
        let older_files = self.older_files.read();
        Ok(Stat {
            key_num: keys.len(),
            data_file_num: older_files.len() + 1,
            reclaim_size: self.reclaim_size.load(Ordering::SeqCst),
            disk_size: dir_disk_size(self.options.dir_path.clone()),
        })
    }

    /// 备份数据目录
    pub fn backup(&self, dir_path: PathBuf) -> Result<(), Errors> {
        let exclude = [FILE_LOCK_NAME];
        if let Err(e) = copy_dir(self.options.dir_path.clone(), dir_path, &exclude) {
            log::error!("failed to copy dir: {}", e);
            return Err(Errors::FailedToCopyDir);
        }

        Ok(())
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
        if let Some(old_pos) = self.index.put(key.to_vec(), log_record_pos) {
            self.reclaim_size
                .fetch_add(old_pos.size as usize, Ordering::SeqCst);
        }

        Ok(())
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
        let pos = self.append_log_record(log_record)?;
        self.reclaim_size
            .fetch_add(pos.size as usize, Ordering::SeqCst);

        // 更新内存索引
        if let Some(old_pos) = self.index.delete(key.to_vec()) {
            self.reclaim_size
                .fetch_add(old_pos.size as usize, Ordering::SeqCst);
        }

        Ok(())
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
            let old_file = DataFile::new(dir_path.clone(), current_fid, IOType::FileIO)?;
            older_files.insert(current_fid, old_file);

            let new_file = DataFile::new(dir_path, current_fid + 1, IOType::FileIO)?;
            *active_file = new_file;
        }

        // 追加写入数据
        let write_off = active_file.get_write_off();
        active_file.write(&enc_record)?;

        let previous = self
            .bytes_write
            .fetch_add(enc_record.len(), Ordering::SeqCst);
        // 根据配置项决定是否持久化
        let mut need_sync = self.options.sync_writes;
        if !need_sync
            && self.options.bytes_per_sync > 0
            && previous + enc_record.len() > self.options.bytes_per_sync
        {
            need_sync = true;
        }

        if need_sync {
            active_file.sync()?;
            self.bytes_write.store(0, Ordering::SeqCst);
        }

        Ok(LogRecordPos {
            file_id: active_file.get_file_id(),
            offset: write_off,
            size: enc_record.len() as u32,
        })
    }

    // 从数据文件中加载内存索引
    fn load_index(&mut self) -> Result<(), Errors> {
        if self.file_ids.is_empty() {
            return Ok(());
        }

        // 拿到最近未参与 merge 的文件 id
        let mut non_merge_fid = 0;
        let merge_fin_file = self.options.dir_path.join(MERGE_FINISHED_FILE_NAME);
        if merge_fin_file.is_file() {
            let merge_fin_file = DataFile::new_merge_finished_file(self.options.dir_path.clone())?;
            let read_res = merge_fin_file.read(0)?;
            let v = String::from_utf8(read_res.record.value).unwrap();
            non_merge_fid = v.parse::<u32>().unwrap();
        }

        let mut active_file = self.active_file.write();
        let older_files = self.older_files.read();

        // 暂存事务序列号和事务内所有数据的信息
        let mut txn_batch: HashMap<usize, Vec<TransactionLogRecord>> = HashMap::new();

        // 读取所有数据文件并构建内存索引
        for (i, file_id) in self.file_ids.iter().enumerate() {
            // 如果文件 id 比 non_merge_fid 小，则跳过
            if *file_id < non_merge_fid {
                continue;
            }

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
                    size: size as u32,
                };

                // 解析 key ,拿到实际 key 和事务序列号
                let (real_key, seq_no) = parse_log_record_key(log_record.key);

                // 更新事务序列号
                self.seq_no.fetch_max(seq_no + 1, Ordering::SeqCst);

                // 非事务数据直接更新索引，事务数据先暂存，读到 TXN_FIN_KEY 统一更新索引
                if seq_no == NON_TXN_SEQ_NO {
                    self.update_index(real_key, log_record.rec_type, log_record_pos);
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

    // 启动时更新内存索引
    pub(crate) fn update_index(&self, key: Vec<u8>, rec_type: LogRecordType, pos: LogRecordPos) {
        if rec_type == LogRecordType::NOAMAL {
            if let Some(old_pos) = self.index.put(key.clone(), pos) {
                self.reclaim_size
                    .fetch_add(old_pos.size as usize, Ordering::SeqCst);
            }
        }
        if rec_type == LogRecordType::DELETED {
            let mut size = pos.size;
            if let Some(old_pos) = self.index.delete(key) {
                size += old_pos.size;
            }
            self.reclaim_size.fetch_add(size as usize, Ordering::SeqCst);
        }
    }

    // 加载事务序列号
    fn load_seq_no(&self) -> (bool, usize) {
        let file_path = self.options.dir_path.join(SEQ_NO_FILE_NAME);
        if !file_path.is_file() {
            return (false, 0);
        }

        let seq_no_file = DataFile::new_seq_no_file(self.options.dir_path.clone()).unwrap();
        let record = match seq_no_file.read(0) {
            Ok(res) => res.record,
            Err(e) => panic!("failed to read seq no: {}", e),
        };
        let v = String::from_utf8(record.value).unwrap();
        let seq_no = v.parse::<usize>().unwrap();

        // 加载后删除文件，避免追加写入
        remove_file(file_path).unwrap();

        (true, seq_no)
    }

    fn reset_io_type(&self) {
        let mut active_file = self.active_file.write();
        active_file.set_io_manager(self.options.dir_path.clone(), IOType::FileIO);
        let mut older_files = self.older_files.write();
        for (_, file) in older_files.iter_mut() {
            file.set_io_manager(self.options.dir_path.clone(), IOType::FileIO);
        }
    }
}

impl Drop for Engine {
    fn drop(&mut self) {
        if let Err(e) = self.close() {
            log::error!("error while closing engine: {}", e);
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

    if options.data_file_merge_ratio < 0 as f32 || options.data_file_merge_ratio > 1 as f32 {
        return Some(Errors::InvalidMergeRatio);
    }

    None
}

fn load_data_files(dir_path: PathBuf, use_mmap: bool) -> Result<Vec<DataFile>, Errors> {
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

    let mut io_type = IOType::FileIO;
    if use_mmap {
        io_type = IOType::MMapIO;
    }
    file_ids.sort();
    for file_id in file_ids.iter() {
        let data_file = DataFile::new(dir_path.clone(), *file_id, io_type)?;
        data_files.push(data_file);
    }

    Ok(data_files)
}
