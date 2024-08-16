use super::file;
use crate::config::{self, Config};
use crate::entry::entry::{self, Entry, EntryFile, EntryParseResult};
use crate::entry::hint::{Hint, HintFile};
use crate::util::lock;
use crate::OpError;
use chrono::Utc;
use log::*;
use std::collections::hash_map::Iter;
use std::collections::HashMap;
use std::fs::{self, File};
use std::io::{BufReader, Write};
use std::path::{Path, PathBuf};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::mpsc;
use std::sync::mpsc::Receiver;
use std::sync::{Arc, Mutex, RwLock};

pub trait Op: Send + Sync + 'static {
    fn get(&self, key: &Vec<u8>) -> Result<Vec<u8>, OpError>;
    fn get_entry(&self, key: &Vec<u8>) -> Result<Entry, OpError>;
    fn set(&mut self, key: &Vec<u8>, value: &Vec<u8>, timestamp: u64);
    fn delete(&mut self, key: &Vec<u8>);
    fn len(&self) -> usize;
    fn keys(&self) -> Vec<Vec<u8>>;
    fn compaction(&mut self);
}

const ACTIVE_FILE_SEQ: u16 = 0;
type StFile = Arc<RwLock<BufReader<File>>>; // storage File
type NotifyResult = i32;

pub struct Store<K>
where
    // K: OpKeydir,
    K: OpKeydir + Send + Sync + 'static,
{
    active_file: Arc<RwLock<File>>,
    config: Arc<config::Config>,
    keydir: Arc<RwLock<K>>,
    files: Arc<RwLock<HashMap<u16, StFile>>>,
    file_size: AtomicUsize, // 当前写入文件大小
    merge_file_num: AtomicUsize,
    updated_key_num: AtomicUsize, // 更新key数量
    sender: mpsc::Sender<NotifyResult>,
    receiver: Arc<Mutex<mpsc::Receiver<NotifyResult>>>,
}

pub fn new_store(
    config: Arc<Config>,
    sender: mpsc::Sender<NotifyResult>,
    receiver: mpsc::Receiver<NotifyResult>,
) -> Store<Keydir> {
    let keydir = Keydir::new();
    let mut s = Store::new(keydir, config, sender, receiver);
    s.start();
    s
}

fn get_active_data(filepath: PathBuf) -> Arc<RwLock<File>> {
    let fd = file::new(&filepath).unwrap();
    Arc::new(RwLock::new(fd))
}

impl<K> Store<K>
where
    K: OpKeydir + Send + Sync + 'static,
{
    pub fn new(
        keydir: K,
        conf: Arc<Config>,
        sender: mpsc::Sender<NotifyResult>,
        receiver: mpsc::Receiver<NotifyResult>,
    ) -> Store<K> {
        // let conf = config::Config::new();
        let active_file = get_active_data(conf.get_active_filepath());
        let mut s = Store {
            active_file,
            config: conf,
            keydir: Arc::new(RwLock::new(keydir)),
            files: Arc::new(RwLock::new(HashMap::new())),
            file_size: AtomicUsize::new(0),
            merge_file_num: AtomicUsize::new(0),
            updated_key_num: AtomicUsize::new(0),
            sender,
            receiver: Arc::new(Mutex::new(receiver)),
        };
        s.notify();
        s
    }

    fn get_filesize(&self) -> usize {
        self.file_size.load(Ordering::SeqCst)
    }

    // 从当前目录里读取相关文件，如果未找到任务数据文件
    pub fn start(&mut self) {
        // 1. load all hits to keydir
        self.load_hint_file();

        // 2. load all datafiles
        self.rebuild_from_datafile();
        self.load_active_file();
    }

    // merge

    fn rebuild_from_datafile(&mut self) {
        let current_max_file_seq = self.config.get_next_datafile_seq();
        let mut keydir = Keydir::new();

        for idx in 1..current_max_file_seq {
            let mut count = 0;

            // read index from hint file
            let the_file = self.config.get_filepath_by_seq(idx);
            if !the_file.exists() {
                debug!("data file {:?}  not found!", the_file);
                continue;
            } else {
                // 判断是否已通过 hint 文件合并
                let hint_filename = self.config.get_hint_filepath_by_seq(idx);
                if hint_filename.exists() {
                    continue;
                }
            }
            let file = file::open(&the_file).unwrap();
            let entry_file = EntryFile::new(file);
            let entry_result: Vec<EntryParseResult> = entry_file.into();

            for entry in entry_result {
                let metadata = Metadata {
                    file_id: idx,
                    value_sz: entry.entry.size() as u64,
                    value_pos: entry.value_pos,
                    tstamp: entry.entry.timestamp,
                };

                // debug!("{:?} {:?}", entry.entry, metadata);
                // log replay
                if entry.entry.is_expired() || entry.entry.is_removed() {
                    info!("expired or deleted {:?}", entry.entry);
                    keydir.remove(&entry.entry.key);
                } else {
                    keydir.set(&entry.entry.key, metadata);
                    count += 1;
                }
            }

            // register datafile fd
            let fd = file::open_reader(&the_file).unwrap();
            let mut file = self.files.write().unwrap();
            file.insert(idx, Arc::new(RwLock::new(fd)));

            debug!("加载序号 {}  datafile 文件，共找到条目 {}", idx, count);
        }

        debug!("共加载数据文件条目 {:?}", keydir.data.len());

        // merge all archived datafile to keydir
        let mut keydir_global = self.keydir.write().unwrap();
        keydir_global.extend(keydir.data);
    }
    fn load_hint_file(&mut self) {
        let current_max_file_seq = self.config.get_next_datafile_seq();
        let mut total: usize = 0;
        for idx in 1..current_max_file_seq {
            // read index from hint file
            let hint_filename = self.config.get_hint_filepath_by_seq(idx);
            let archived_filename = self.config.get_filepath_by_seq(idx);
            if !hint_filename.exists() || !archived_filename.exists() {
                debug!("hits file {:?}  not found!", hint_filename);
                continue;
            }
            let hint_file_path = file::open(&hint_filename).unwrap();

            let the_file = HintFile::new(hint_file_path);
            let result: Vec<Hint> = HintFile::into(the_file);
            let len = result.len();

            for hint in result {
                let metadata = Metadata {
                    file_id: idx,
                    value_sz: hint.value_size,
                    value_pos: hint.value_pos,
                    tstamp: hint.timestamp,
                };
                debug!("索引加载hint: {:?}", hint);
                debug!("{:?}", metadata);

                // update keydir
                let mut keydir = self.keydir.write().unwrap();
                keydir.set(&hint.key, metadata);
            }

            debug!("加载序号 {}  hint文件，共计 {}", current_max_file_seq, len);

            // register datafile fd
            let fd = file::open_reader(&archived_filename).unwrap();
            let mut file = self.files.write().unwrap();
            file.insert(idx, Arc::new(RwLock::new(fd)));

            total += len;
        }

        debug!("从hint文件加载共计 {} 项", total);
    }

    fn load_active_file(&mut self) {
        // 读取磁盘 active file, 主要实现从 data 文件实现索引重建
        let op_file = Arc::clone(&self.active_file);
        let active_file = op_file.write().unwrap();
        let the_file = active_file.try_clone().unwrap();
        let entry_file = EntryFile::new(the_file);
        let entry_result: Vec<EntryParseResult> = entry_file.into();

        // 临时 keydir
        let mut keydir = Keydir::new();
        for entry in entry_result {
            let metadata = Metadata {
                file_id: ACTIVE_FILE_SEQ,
                value_sz: entry.entry.size() as u64,
                value_pos: entry.value_pos,
                tstamp: entry.entry.timestamp,
            };

            // debug!("{:?} {:?}", entry.entry, metadata);
            // log replay
            if entry.entry.is_expired() || entry.entry.is_removed() {
                debug!("expired or deleted {:?}", entry.entry);
                keydir.remove(&entry.entry.key);
            } else {
                keydir.set(&entry.entry.key, metadata);
            }
        }

        debug!("found {} items from active file", keydir.len());

        // merge temp keydir
        let mut self_keydir = self.keydir.write().unwrap();
        self_keydir.extend(keydir.data);
    }

    fn get_fd(&self, seq: u16) -> Result<(Option<Arc<RwLock<File>>>, Option<StFile>), OpError> {
        if seq == ACTIVE_FILE_SEQ {
            return Ok((Some(Arc::clone(&self.active_file)), None));
        }

        // match self.files.get(&seq)
        let files = self.files.read().unwrap();
        match files.get(&seq) {
            Some(v) => Ok((None, Some(Arc::clone(v)))),
            None => {
                debug!("unregister fd for seq {}", seq);
                Err(OpError::KeyNotFound)
            }
        }
    }

    fn active_file_archive(&mut self) {
        // 归档文件
        self.archive_file();

        // merge archived datafiles in a new thread
        self.merge_file_num.fetch_add(1, Ordering::SeqCst);
        debug!(
            "{} : {}",
            self.merge_file_num.load(Ordering::SeqCst),
            self.config.get_merge_file_num()
        );
        if self.merge_file_num.load(Ordering::SeqCst) >= self.config.get_merge_file_num() {
            self.merge_file_num.store(0, Ordering::SeqCst);

            // Avoiding duplicate merges caused by generating new files too quickly
            if let Ok(_) = lock::Locker::acquire() {
                self.sender.send(0).unwrap();
            } else {
                debug!("发现 lock 文件，正在合并中...");
            }
        }
    }

    // rename active file to datafiles
    fn archive_file(&mut self) {
        let active_filepath = self.config.get_active_filepath();

        let archive_file_seq = self.config.get_next_datafile_seq();
        let archive_filepath = self.config.get_filepath_by_seq(archive_file_seq);

        // write lock and flush buffer body to disk
        {
            let mut files = self.files.write().unwrap();
            // 1. rename active file name to archive file
            let mut fd = self.active_file.write().unwrap();
            fd.flush().unwrap();

            std::fs::rename(&active_filepath, &archive_filepath).unwrap();

            // 2. reopen the file in read-only mode
            let file = file::open_reader(&archive_filepath).unwrap();
            files.insert(archive_file_seq, Arc::new(RwLock::new(file)));

            // update keydir
            let mut keydir = self.keydir.write().unwrap();
            keydir.update_key(archive_file_seq);

            debug!("archive active file => {:?}", archive_filepath);
        }

        // renew active file
        let archive_file_fd = file::new(&active_filepath).unwrap();
        self.active_file = Arc::new(RwLock::new(archive_file_fd));
        debug!("renew active file {:?}", active_filepath);
    }

    fn notify(&mut self) {
        let active_file = Arc::clone(&self.active_file);
        let files = Arc::clone(&self.files);
        let config = Arc::clone(&self.config);
        // let self = &self.clone();
        let receiver: Arc<Mutex<Receiver<NotifyResult>>> = Arc::clone(&self.receiver);
        let keydir = Arc::clone(&self.keydir);

        std::thread::spawn(move || {
            fn get_fd(
                active_file: Arc<RwLock<File>>,
                files: Arc<RwLock<HashMap<u16, StFile>>>,
                seq: u16,
            ) -> Result<(Option<Arc<RwLock<File>>>, Option<StFile>), OpError> {
                if seq == ACTIVE_FILE_SEQ {
                    return Ok((Some(active_file), None));
                }

                let files = files.read().unwrap();
                match files.get(&seq) {
                    Some(v) => Ok((None, Some(Arc::clone(v)))),
                    None => {
                        debug!("unregister fd for seq {}", seq);
                        Err(OpError::KeyNotFound)
                    }
                }
            }

            for i in receiver.lock().unwrap().iter() {
                println!("notify thread iter ======================= {:?}", i);
                debug!("\n\n=== COMPACTION BEGIN ===");

                let merge_dir = config.merge_dir();
                if merge_dir.exists() {
                    debug!("当前已处于工作状态 {:?}", merge_dir);
                    return;
                }
                // 创建临时合并目录
                fs::create_dir_all(merge_dir).unwrap();

                let mut merge_file_seq: u16 = 1;
                let mut merge_total_size = 0;
                let mut merge_keydir = Keydir::new();

                // merge file
                let merge_filepath = config.get_merge_filepath_by_seq(merge_file_seq);
                let mut merge_file_fd = file::new_writer(&merge_filepath).unwrap();
                debug!("create merge file: {:?}", merge_filepath);

                // hint file
                let merge_hint_filepath = config.get_merge_hint_filepath_by_seq(merge_file_seq);
                debug!("create merge hint file: {:?}", merge_hint_filepath);
                let mut merge_hint_file_fd = file::new_writer(&merge_hint_filepath).unwrap();

                let active_file_seq = config.get_next_datafile_seq();

                // 新文件pos
                let mut offset = 0;

                debug!("archive_file_seq= {:?}", active_file_seq);
                for (key, metadata) in keydir
                    .read()
                    .unwrap()
                    .iter()
                    .filter(|(_, metadata)| metadata.file_id > 0)
                {
                    let active_file = Arc::clone(&active_file);
                    let files = Arc::clone(&files);
                    let old_file_option = get_fd(active_file, files, metadata.file_id).unwrap();
                    // 从原来的文件读取最新值
                    let bytes_result = match old_file_option {
                        (None, Some(archive_file)) => {
                            file::read_reader(&archive_file, metadata.value_pos, metadata.value_sz)
                        }
                        _ => unreachable!(), // 理论上不可能到达这里
                    };

                    match bytes_result {
                        Ok(bytes) => {
                            let body_size = bytes.len();
                            //  splits a new file
                            if merge_total_size + body_size > config.file_max_size() {
                                // 1. close current merge file fd
                                merge_file_fd.flush().unwrap();
                                merge_hint_file_fd.flush().unwrap();

                                // 2.1 create a new merge file and hint file
                                merge_file_seq += 1;
                                let merge_filepath =
                                    config.get_merge_filepath_by_seq(merge_file_seq);
                                merge_file_fd = file::new_writer(&merge_filepath).unwrap();
                                debug!("[data]create merge file: {:?}", merge_filepath);

                                let merge_hint_filepath =
                                    config.get_merge_hint_filepath_by_seq(merge_file_seq);
                                merge_hint_file_fd =
                                    file::new_writer(&merge_hint_filepath).unwrap();
                                debug!("[hint]create merge hint file: {:?}", merge_hint_filepath);

                                // 3. reset all variable for next iter
                                merge_total_size = 0;
                                offset = 0;
                            }

                            // write merge file
                            merge_file_fd.write_all(bytes.as_slice()).unwrap();

                            // write hint file
                            let entry = match Entry::try_from(bytes.clone()) {
                                Ok(entry) => entry,
                                Err(e) => {
                                    debug!("wrong {:?}", bytes);
                                    debug!("metadata {:?}", metadata);
                                    panic!("wrong {:?}", e)
                                }
                            };
                            let hint_entry = Hint {
                                timestamp: metadata.tstamp,
                                key_size: entry.key_size,
                                value_size: entry.size() as u64, // 整个entry 大小,
                                value_pos: offset,               // 整个entry的读取位置
                                key: entry.key,
                            };
                            debug!("write_hit {:?}", hint_entry);
                            let hint_bytes: Vec<u8> = hint_entry.into();
                            merge_hint_file_fd.write_all(&hint_bytes).unwrap();

                            // update keydir index
                            let merge_metadata = Metadata {
                                file_id: merge_file_seq,     // 新 file_seq
                                value_pos: offset,           // 在新文件offset
                                value_sz: metadata.value_sz, // entry 本身大小不变
                                tstamp: metadata.tstamp,
                            };

                            debug!("old_medatdata {:?}", metadata);
                            debug!("hit_metadata  {:?}", merge_metadata);

                            merge_keydir.data.insert(key.clone(), merge_metadata);

                            // position for next write
                            offset += metadata.value_sz;
                            merge_total_size += body_size;
                        }
                        Err(_) => error!("err {:?}", metadata.file_id),
                    }
                }

                // 刷新写盘
                merge_file_fd.flush().unwrap();
                merge_hint_file_fd.flush().unwrap();

                // move
                if merge_keydir.data.len() > 0 {
                    // delete old datafiles
                    let mut files = files.write().unwrap();
                    for i in files.keys() {
                        if *i >= active_file_seq {
                            continue;
                        }
                        let delete_file = config.get_filepath_by_seq(*i);
                        debug!("deleted old data file: {:?}", delete_file);
                        fs::remove_file(delete_file).unwrap();
                    }
                    // 加锁操作
                    // rename merge files
                    for i in 1..(merge_file_seq + 1) {
                        // merge file
                        let from = config.get_merge_filepath_by_seq(i);
                        let to = config.get_filepath_by_seq(i);
                        let src_path = Path::new(&from);
                        let dst_path = Path::new(&to);

                        debug!("{:?} => {:?} File moved successfully!", from, to);
                        std::fs::rename(src_path, dst_path).unwrap();

                        // hint file
                        {
                            let from = config.get_merge_hint_filepath_by_seq(i);
                            let to = config.get_hint_filepath_by_seq(i);
                            let src_path = Path::new(&from);
                            let dst_path = Path::new(&to);

                            std::fs::rename(src_path, dst_path).unwrap();
                            debug!("{:?} => {:?} File moved successfully!", from, to);
                        }

                        // 重新打开所有文件句柄，并注册[file_id:fd]
                        let fd = file::open_reader(&dst_path.to_path_buf()).unwrap();
                        files.insert(i, Arc::new(RwLock::new(fd)));
                    }

                    // update keydir index
                    let mut keydir = keydir.write().unwrap();
                    keydir.extend(merge_keydir.data);
                } else {
                    // remove empty file
                    fs::remove_file(merge_filepath).unwrap();
                }

                // 删除临时合并文件
                config.merge_cleanup();

                debug!("\n=== COMPACTION END ===\n");
            }
        });
    }
}

impl<K: OpKeydir> Op for Store<K> {
    // get
    fn get(&self, key: &Vec<u8>) -> Result<Vec<u8>, OpError> {
        let self_keydir = self.keydir.read().unwrap();
        if let Ok(metadata) = self_keydir.get(key) {
            debug!("key:{:?}  {:?}", key, metadata);
            let op_file_option = self.get_fd(metadata.file_id).unwrap();

            let bytes_result = match op_file_option {
                (Some(active_file), None) => {
                    file::read(&active_file, metadata.value_pos, metadata.value_sz)
                }
                (None, Some(archive_file)) => {
                    file::read_reader(&archive_file, metadata.value_pos, metadata.value_sz)
                }
                _ => unreachable!(), // 理论上不可能到达这里
            };

            // debug!(
            //     "file_id: {},value_pos: {}, value_sz: {}",
            //     metadata.file_id, metadata.value_pos, metadata.value_sz
            // );
            // let bytes = file::read(&op_file, metadata.value_pos, metadata.value_sz);
            match bytes_result {
                Ok(bytes) => match Entry::try_from(bytes) {
                    Ok(entry) => {
                        debug!("{:?}", entry);
                        if !entry.is_valid() || entry.is_expired() || entry.is_removed() {
                            // remove item from key
                            // self.keydir.remove(key);
                            return Err(OpError::ValueInvalid);
                        } else {
                            return Ok(entry.value);
                        }
                    }
                    Err(e) => {
                        error!("parse Entry object failed! {:?}", e);
                        return Err(OpError::ValueInvalid);
                    }
                },
                Err(e) => {
                    error!("read error: {:?}", e);
                    return Err(OpError::ValueInvalid);
                }
            }
        }
        Err(OpError::KeyNotFound)
    }

    fn get_entry(&self, key: &Vec<u8>) -> Result<Entry, OpError> {
        let self_keydir = self.keydir.read().unwrap();
        if let Ok(metadata) = self_keydir.get(key) {
            debug!("key:{:?}  {:?}", key, metadata);
            let op_file_option = self.get_fd(metadata.file_id).unwrap();

            let bytes_result = match op_file_option {
                (Some(active_file), None) => {
                    file::read(&active_file, metadata.value_pos, metadata.value_sz)
                }
                (None, Some(archive_file)) => {
                    file::read_reader(&archive_file, metadata.value_pos, metadata.value_sz)
                }
                _ => unreachable!(), // 理论上不可能到达这里
            };

            // debug!(
            //     "file_id: {},value_pos: {}, value_sz: {}",
            //     metadata.file_id, metadata.value_pos, metadata.value_sz
            // );
            // let bytes = file::read(&op_file, metadata.value_pos, metadata.value_sz);
            match bytes_result {
                Ok(bytes) => match Entry::try_from(bytes) {
                    Ok(entry) => {
                        debug!("{:?}", entry);
                        if entry.is_valid() {
                            return Ok(entry);
                        } else {
                            return Err(OpError::ValueInvalid);
                        }
                    }
                    Err(e) => {
                        error!("parse Entry object failed! {:?}", e);
                        return Err(OpError::ValueInvalid);
                    }
                },
                Err(e) => {
                    error!("read error: {:?}", e);
                    return Err(OpError::ValueInvalid);
                }
            }
        }
        Err(OpError::KeyNotFound)
    }

    // set/put
    fn set(&mut self, key: &Vec<u8>, value: &Vec<u8>, timestamp: u64) {
        debug!(
            "set key:{:?}, value:{:?}, timestamp: {}",
            key, value, timestamp
        );
        let entry = entry::Entry::new(key.clone(), value.clone(), timestamp);

        // 文件大小分割
        if self.get_filesize() + entry.size() > self.config.file_max_size() {
            self.active_file_archive();
            self.file_size.store(0, Ordering::SeqCst);
        } else {
            self.file_size.fetch_add(entry.size(), Ordering::SeqCst);
        }

        // 2. 写入当前活跃文件
        let (entry_pos, entry_size) = file::append(&self.active_file, entry);
        self.updated_key_num.fetch_add(1, Ordering::SeqCst);
        let config_sync_keys_num = self.config.get_sync_keys_num() as usize;
        if config_sync_keys_num > 0
            && self.updated_key_num.load(Ordering::SeqCst) >= config_sync_keys_num
        {
            // file flush
            let mut active_file_fd = self.active_file.write().unwrap();
            active_file_fd.flush().unwrap();
        }

        // 3. update keydir
        let metadata = Metadata {
            file_id: ACTIVE_FILE_SEQ,
            value_sz: entry_size,
            value_pos: entry_pos,
            tstamp: Utc::now().timestamp() as u64,
        };
        let mut self_keydir = self.keydir.write().unwrap();
        self_keydir.set(key, metadata);
    }

    // delete
    fn delete(&mut self, key: &Vec<u8>) {
        // 先检查是否存在，否则直接返回
        // self.set(key, &b"".to_vec());
        let value: Vec<u8> = vec![];
        let entry = entry::Entry::new(key.clone(), value, 0).set_removed();
        debug!("delete {:?}", entry);
        self.file_size.fetch_add(entry.size(), Ordering::SeqCst);
        let (_, _) = file::append(&self.active_file, entry);
        let config_sync_keys_num = self.config.get_sync_keys_num() as usize;
        if config_sync_keys_num > 0
            && self.updated_key_num.load(Ordering::SeqCst) >= config_sync_keys_num
        {
            // file flush
            let mut active_file_fd = self.active_file.write().unwrap();
            active_file_fd.flush().unwrap();
        }
        // delete index from keydir
        let mut self_keydir = self.keydir.write().unwrap();
        self_keydir.remove(key);
    }

    // len
    fn len(&self) -> usize {
        self.keydir.read().unwrap().len()
    }

    fn keys(&self) -> Vec<Vec<u8>> {
        let keydir = self.keydir.read().unwrap(); // 读锁定
        keydir.keys().into_iter().cloned().collect()
    }

    fn compaction(&mut self) {
        debug!("\n\n=== COMPACTION BEGIN ===");

        let merge_dir = self.config.merge_dir();
        if merge_dir.exists() {
            debug!("当前已处于工作状态 {:?}", merge_dir);
            return;
        }
        // 创建临时合并目录
        fs::create_dir_all(merge_dir).unwrap();

        let mut merge_file_seq: u16 = 1;
        let mut merge_total_size = 0;
        let mut merge_keydir = Keydir::new();

        // merge file
        let merge_filepath = self.config.get_merge_filepath_by_seq(merge_file_seq);
        let mut merge_file_fd = file::new_writer(&merge_filepath).unwrap();
        debug!("create merge file: {:?}", merge_filepath);

        // hint file
        let merge_hint_filepath = self.config.get_merge_hint_filepath_by_seq(merge_file_seq);
        debug!("create merge hint file: {:?}", merge_hint_filepath);
        let mut merge_hint_file_fd = file::new_writer(&merge_hint_filepath).unwrap();

        let active_file_seq = self.config.get_next_datafile_seq();

        // 新文件pos
        let mut offset = 0;
        debug!("archive_file_seq= {:?}", active_file_seq);
        for (key, metadata) in self
            .keydir
            .read()
            .unwrap()
            .iter()
            .filter(|(_, metadata)| metadata.file_id > 0)
        {
            let old_file_option = self.get_fd(metadata.file_id).unwrap();
            // 从原来的文件读取最新值
            let bytes_result = match old_file_option {
                // (Some(active_file), None) => {
                //     // 合并操作，永远不会使用 active_file
                //     file::read(&active_file, metadata.value_pos, metadata.value_sz)
                // }
                (None, Some(archive_file)) => {
                    file::read_reader(&archive_file, metadata.value_pos, metadata.value_sz)
                }
                _ => unreachable!(), // 理论上不可能到达这里
            };

            match bytes_result {
                Ok(bytes) => {
                    let body_size = bytes.len();
                    //  splits a new file
                    if merge_total_size + body_size > self.config.file_max_size() {
                        // 1. close current merge file fd
                        merge_file_fd.flush().unwrap();
                        merge_hint_file_fd.flush().unwrap();

                        // 2.1 create a new merge file and hint file
                        merge_file_seq += 1;
                        let merge_filepath = self.config.get_merge_filepath_by_seq(merge_file_seq);
                        merge_file_fd = file::new_writer(&merge_filepath).unwrap();
                        debug!("[data]create merge file: {:?}", merge_filepath);

                        let merge_hint_filepath =
                            self.config.get_merge_hint_filepath_by_seq(merge_file_seq);
                        merge_hint_file_fd = file::new_writer(&merge_hint_filepath).unwrap();
                        debug!("[hint]create merge hint file: {:?}", merge_hint_filepath);

                        // 3. reset all variable for next iter
                        merge_total_size = 0;
                        offset = 0;
                    }

                    // write merge file
                    merge_file_fd.write_all(bytes.as_slice()).unwrap();

                    // write hint file
                    let entry = match Entry::try_from(bytes.clone()) {
                        Ok(entry) => entry,
                        Err(e) => {
                            debug!("wrong {:?}", bytes);
                            debug!("metadata {:?}", metadata);
                            panic!("wrong {:?}", e)
                        }
                    };
                    let hint_entry = Hint {
                        timestamp: metadata.tstamp,
                        key_size: entry.key_size,
                        value_size: entry.size() as u64, // 整个entry 大小,
                        value_pos: offset,               // 整个entry的读取位置
                        key: entry.key,
                    };
                    debug!("write_hit {:?}", hint_entry);
                    let hint_bytes: Vec<u8> = hint_entry.into();
                    merge_hint_file_fd.write_all(&hint_bytes).unwrap();

                    // update keydir index
                    let merge_metadata = Metadata {
                        file_id: merge_file_seq,     // 新 file_seq
                        value_pos: offset,           // 在新文件offset
                        value_sz: metadata.value_sz, // entry 本身大小不变
                        tstamp: metadata.tstamp,
                    };

                    debug!("old_medatdata {:?}", metadata);
                    debug!("hit_metadata  {:?}", merge_metadata);

                    merge_keydir.data.insert(key.clone(), merge_metadata);

                    // position for next write
                    offset += metadata.value_sz;
                    merge_total_size += body_size;
                }
                Err(_) => error!("err {:?}", metadata.file_id),
            }
        }

        // 刷新写盘
        merge_file_fd.flush().unwrap();
        merge_hint_file_fd.flush().unwrap();

        // move
        if merge_keydir.data.len() > 0 {
            // delete old datafiles
            let mut files = self.files.write().unwrap();
            for i in files.keys() {
                if *i >= active_file_seq {
                    continue;
                }
                let delete_file = self.config.get_filepath_by_seq(*i);
                debug!("deleted old data file: {:?}", delete_file);
                fs::remove_file(delete_file).unwrap();
            }
            // 加锁操作
            // rename merge files
            for i in 1..(merge_file_seq + 1) {
                // merge file
                let from = self.config.get_merge_filepath_by_seq(i);
                let to = self.config.get_filepath_by_seq(i);
                let src_path = Path::new(&from);
                let dst_path = Path::new(&to);

                debug!("{:?} => {:?} File moved successfully!", from, to);
                std::fs::rename(src_path, dst_path).unwrap();

                // hint file
                {
                    let from = self.config.get_merge_hint_filepath_by_seq(i);
                    let to = self.config.get_hint_filepath_by_seq(i);
                    let src_path = Path::new(&from);
                    let dst_path = Path::new(&to);

                    std::fs::rename(src_path, dst_path).unwrap();
                    debug!("{:?} => {:?} File moved successfully!", from, to);
                }

                // 重新打开所有文件句柄，并注册[file_id:fd]
                let fd = file::open_reader(&dst_path.to_path_buf()).unwrap();
                files.insert(i, Arc::new(RwLock::new(fd)));
            }

            // update keydir index
            let mut keydir = self.keydir.write().unwrap();
            keydir.extend(merge_keydir.data);
        } else {
            // remove empty file
            fs::remove_file(merge_filepath).unwrap();
        }

        // 删除临时合并文件
        self.config.merge_cleanup();

        debug!("\n=== COMPACTION END ===\n");
    }
}

// --- keydir
pub struct Keydir {
    // data: Arc<RwLock<HashMap<String, Metadata>>>,
    data: HashMap<Vec<u8>, Metadata>,
}

pub trait OpKeydir: Sync + Send {
    fn new() -> Keydir;
    fn get(&self, key: &Vec<u8>) -> Result<Metadata, OpError>;

    fn set(&mut self, key: &Vec<u8>, metadata: Metadata);
    fn remove(&mut self, key: &Vec<u8>);
    fn len(&self) -> usize;

    fn extend<I>(&mut self, iter: I)
    where
        I: IntoIterator<Item = (Vec<u8>, Metadata)>;

    fn iter(&self) -> Iter<Vec<u8>, Metadata>;

    fn update_key(&mut self, file_id: u16);
    fn keys(&self) -> Vec<&Vec<u8>>;
}

impl OpKeydir for Keydir {
    fn new() -> Keydir {
        Keydir {
            data: HashMap::new(),
        }
    }

    fn iter(&self) -> Iter<Vec<u8>, Metadata> {
        self.data.iter()
    }
    fn get(&self, key: &Vec<u8>) -> Result<Metadata, OpError> {
        self.data.get(key).cloned().ok_or(OpError::KeyNotFound)
    }

    fn set(&mut self, key: &Vec<u8>, metadata: Metadata) {
        self.data.insert(key.clone(), metadata);
    }

    fn remove(&mut self, key: &Vec<u8>) {
        self.data.remove(key);
    }

    fn len(&self) -> usize {
        self.data.len()
    }

    fn keys(&self) -> Vec<&Vec<u8>> {
        let mut result: Vec<&Vec<u8>> = Vec::with_capacity(self.data.len());
        for k in self.data.keys() {
            result.push(k);
        }
        result
    }

    // 定义 extend 方法，接收一个实现了 IntoIterator trait 的类型
    fn extend<I>(&mut self, iter: I)
    where
        I: IntoIterator<Item = (Vec<u8>, Metadata)>,
    {
        self.data.extend(iter);
    }

    fn update_key(&mut self, file_id: u16) {
        for metadata in self.data.values_mut() {
            if metadata.file_id == ACTIVE_FILE_SEQ {
                metadata.file_id = file_id;
            }
        }
    }
}

// #[allow(dead_code)]
// fn read_file(seq: u16) {
//     use std::io::{self, Read};

//     let cfg = config::Config::new().unwrap();
//     let mut path = cfg.get_filepath_by_seq(seq);
//     if seq == ACTIVE_FILE_SEQ {
//         path = cfg.get_active_filepath();
//     }
//     let mut the_file = file::open(&path).unwrap();
//     let mut buffer = vec![0; 24];

//     loop {
//         //  crc = 4 |  time = 8 |  ksize = 4 | vsize = 8
//         match the_file.read_exact(&mut buffer) {
//             Ok(()) => {
//                 //debug!("Successfully read {} bytes", buffer.len());
//             }
//             Err(ref e) if e.kind() == io::ErrorKind::UnexpectedEof => {
//                 // 文件结束EOF，处理不完整数据
//                 debug!("Reached end of file");
//                 break; // 退出循环
//             }
//             Err(e) => {
//                 // 处理其他 I/O 错误
//                 error!("Error reading file: {}", e);
//                 break; // 退出循环
//             }
//         }
//         let key_size = u32::from_le_bytes(
//             buffer[12..16]
//                 .try_into()
//                 .map_err(|_| "Invalid key_size data")
//                 .unwrap(),
//         );
//         let value_size = u64::from_le_bytes(
//             buffer[16..24]
//                 .try_into()
//                 .map_err(|_| "Invalid value_size data")
//                 .unwrap(),
//         );

//         // key:: value
//         // key
//         let mut key_buf = vec![0; key_size as usize];
//         the_file.read_exact(&mut key_buf).unwrap();

//         let mut value_buf = vec![0; value_size as usize];
//         the_file.read_exact(&mut value_buf).unwrap();

//         let mut all_bytes =
//             Vec::with_capacity(buffer.len() + key_size as usize + value_size as usize);
//         all_bytes.extend_from_slice(&buffer);
//         all_bytes.extend(key_buf);
//         all_bytes.extend(value_buf);

//         match Entry::try_from(all_bytes) {
//             Ok(value) => {
//                 println!("{:?}", value);
//             }
//             Err(e) => {
//                 panic!("format{}", e)
//             }
//         }
//     }
// }

// #[allow(dead_code)]
// fn read_hint_file(seq: u16) {
//     let cfg = config::Config::new();
//     let path = cfg.get_hint_filepath_by_seq(seq);
//     let the_file = file::open(&path).unwrap();
//     // let mut buffer = vec![0; 24];
//     println!("{:?}", path);

//     let the_file = HintFile::new(the_file);
//     let result: Vec<Hint> = HintFile::into(the_file);
//     for item in result {
//         println!("{:?}", item);
//     }
// }

//------ metadata
#[derive(Clone, Debug)]
pub struct Metadata {
    file_id: u16,
    value_sz: u64,  // entry size
    value_pos: u64, // entry pos
    tstamp: u64,
}

#[cfg(test)]
mod tests {
    use env_logger;
    use std::sync::Once;

    static INIT: Once = Once::new();

    #[allow(dead_code)]
    fn init_logger() {
        INIT.call_once(|| {
            env_logger::builder()
                .is_test(true) // 仅在测试时启用日志
                .filter_level(log::LevelFilter::Debug) // 设置日志级别
                .init();
        });
    }

    // #[test]
    // fn store_new() {
    //     let store = super::Store::new();

    //     assert_eq!(store.keydir.data.len(), 0);
    // }

    // #[test]
    // fn store_get() {
    //     let store = super::new_store();
    //     // store.set(key, value);
    //     assert!(store.get("key").is_err());
    // }

    // #[test]
    // fn store_set() {
    //     let mut store = super::new_store();
    //     store.start();

    //     let key = "language";
    //     let value = "rust";
    //     store.set(key, value);
    //     assert!(store.get(key).is_ok());
    //     assert_eq!(value.to_string(), store.get(key).unwrap());
    // }

    // // #[test]
    // fn store_delete() {
    //     let mut store = super::new_store();
    //     store.start();

    //     let key = "age";
    //     let value = "11";
    //     store.set(key, value);
    //     assert_eq!("11".to_string(), store.get(key).unwrap());
    //     store.delete(key);
    //     assert_eq!(0, store.len())
    // }
    // #[test]
    // fn generate_next_file() {
    //     init_logger();
    //     use crate::config::{self, Config};
    //     use rand::Rng;
    //     use std::path::Path;

    //     // let file = Path::new("./config.toml");
    //     // let c = Config::try_from(file).unwrap();
    //     let (tx, rx) = std::sync::mpsc::channel();
    //     let cnf = Config::default();
    //     let store = super::new_store(cnf, tx, rx);
    //     // let mut store = store.write().unwrap();

    //     // store.start();
    //     for num in 1..20000 {
    //         // let num = rand::thread_rng().gen_range(1..=20000);
    //         // 将数字转换为字符串，再转换为 Vec<u8>
    //         let key: Vec<u8> = num.to_string().into_bytes();
    //         let value: Vec<u8> = (num * 2).to_string().into_bytes();
    //         store.set(&key, &value, 0);
    //     }
    // }

    // #[test]
    // fn test_read_file() {
    //     super::read_file(0);
    // }

    // #[test]
    // fn test_read_hint_file() {
    //     super::read_hint_file(1);
    // }

    // #[test]
    // fn load_hint_file() {
    //     let mut store = super::new_store();
    //     store.start();
    // }
}
