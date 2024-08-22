use super::super::entry::entry;
use std::fs::{self, File};
use std::io::{self, BufReader, Read, Seek, SeekFrom, Write};
use std::path::PathBuf;
use std::sync::{Arc, RwLock};

// 读写
pub fn new(filepath: &PathBuf) -> io::Result<File> {
    let file = fs::OpenOptions::new()
        .read(true)
        .create(true)
        .append(true)
        .open(filepath);

    file
}

pub fn new_writer(filepath: &PathBuf) -> io::Result<io::BufWriter<File>> {
    let f = fs::OpenOptions::new()
        .read(true)
        .create(true)
        .append(true)
        .open(filepath)?;

    Ok(io::BufWriter::new(f))
}

// readonly
pub fn open(path: &PathBuf) -> io::Result<File> {
    File::open(path)
}

pub fn open_reader(path: &PathBuf) -> io::Result<io::BufReader<File>> {
    let f = open(path)?;
    Ok(io::BufReader::new(f))
}

pub fn read(file: &Arc<RwLock<File>>, offset: u64, size: u64) -> io::Result<Vec<u8>> {
    let mut buf = vec![0; size as usize];
    let mut file = file.write().unwrap();
    file.seek(SeekFrom::Start(offset))?;
    file.read_exact(&mut buf)?;

    Ok(buf)
}

pub fn read_reader(
    file: &Arc<RwLock<BufReader<File>>>,
    offset: u64,
    size: u64,
) -> io::Result<Vec<u8>> {
    let mut buf = vec![0; size as usize];
    let mut file = file.write().unwrap();
    file.seek(SeekFrom::Start(offset))?;
    file.read_exact(&mut buf)?;

    Ok(buf)
}

// return entry's position and size
pub fn append(file: &Arc<RwLock<File>>, e: entry::Entry) -> (u64, u64) {
    let mut file = file.write().unwrap();
    // let mut file = file.borrow_mut();
    let pos = file.seek(SeekFrom::End(0)).unwrap();
    let size = e.size() as u64;
    let buf = e.as_bytes();
    file.write_all(&buf).unwrap();
    // file.flush().unwrap();

    (pos, size)
}
