use crate::util;
use crc32fast::Hasher;
use log::*;
use std::fmt;
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::time::{SystemTime, UNIX_EPOCH};

#[repr(u8)]
#[derive(Default, Debug, Clone, Copy, PartialEq, Eq)]
enum Op {
    #[default]
    Add = 0,
    Put = 1, // unused!
    Del = 2,
}
impl fmt::Display for Op {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let op_str = match self {
            Op::Add => "0",
            Op::Put => "1",
            Op::Del => "2",
        };
        write!(f, "{}", op_str)
    }
}
impl Op {
    fn from_u8(value: u8) -> Option<Op> {
        match value {
            0 => Some(Op::Add),
            1 => Some(Op::Put),
            2 => Some(Op::Del),
            _ => None,
        }
    }
    fn to_u8(self) -> u8 {
        self as u8
    }
    fn to_le_bytes(self) -> [u8; 1] {
        self.to_u8().to_le_bytes()
    }
    fn from_le_bytes(bytes: &[u8]) -> Result<Op, &'static str> {
        if bytes.len() != 1 {
            return Err("Invalid byte array length for Op");
        }
        let value = u8::from_le_bytes(
            bytes
                .try_into()
                .map_err(|_| "Invalid byte array conversion")?,
        );
        Op::from_u8(value).ok_or("Invalid value for Op")
    }
}
// impl Default for Op {
//     fn default() -> Self {
//         Op::Add
//     }
// }

// crc (4 bytes) | timestamp (8 bytes) | key_size (4 bytes) | value_size (8 bytes) | op (1 bytes) |  key | value
#[derive(Default)]
#[allow(dead_code)]
pub struct Entry {
    pub crc: u32,
    pub timestamp: u64,
    pub key_size: u32,
    pub value_size: u64,
    op: Op,
    pub key: Vec<u8>,
    pub value: Vec<u8>,
}

pub struct EntryParseResult {
    pub value_pos: u64,
    pub entry: Entry,
}

pub struct EntryFile(File);
impl EntryFile {
    pub fn new(file: File) -> EntryFile {
        EntryFile(file)
    }
}

impl From<EntryFile> for Vec<EntryParseResult> {
    fn from(mut val: EntryFile) -> Self {
        // crc (4 bytes) | timestamp (8 bytes) | key_size (4 bytes) | value_size (8 bytes) | op (1 bytes) | key | value
        let header_size = Entry::default().header_size();
        let mut buffer = vec![0; header_size];

        let _ = val.0.seek(SeekFrom::Start(0));

        let mut offset: usize = 0;
        let mut results = Vec::new();
        loop {
            //  crc = 4 |  time = 8 |  ksize = 4 | vsize = 8 | op = 1
            match val.0.read_exact(&mut buffer) {
                Ok(()) => {
                    // 读取成功，处理 buffer 的内容
                    //debug!("Successfully read {} bytes", buffer.len());
                    // 处理 buffer 内容的其他操作
                }
                Err(ref e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                    // 文件结束EOF，处理不完整数据
                    // debug!("Reached end of file");
                    break; // 退出循环
                }
                Err(e) => {
                    // 处理其他 I/O 错误
                    debug!("Error reading file: {}", e);
                    break; // 退出循环
                }
            }

            let crc = u32::from_le_bytes(
                buffer[..4]
                    .try_into()
                    .map_err(|_| "Invalid CRC data")
                    .unwrap(),
            );
            let timestamp = u64::from_le_bytes(
                buffer[4..12]
                    .try_into()
                    .map_err(|_| "Invalid timestamp data")
                    .unwrap(),
            );
            let key_size = u32::from_le_bytes(
                buffer[12..16]
                    .try_into()
                    .map_err(|_| "Invalid key_size data")
                    .unwrap(),
            );
            let value_size = u64::from_le_bytes(
                buffer[16..24]
                    .try_into()
                    .map_err(|_| "Invalid value_size data")
                    .unwrap(),
            );

            let op = Op::from_le_bytes(
                buffer[24..25]
                    .try_into()
                    .map_err(|_| "Invalid value_size data")
                    .unwrap(),
            )
            .unwrap();

            // key
            let mut key = vec![0; key_size as usize];
            val.0.read_exact(&mut key).unwrap();

            // value
            let mut value = vec![0; value_size as usize];
            val.0.read_exact(&mut value).unwrap();

            let entry = Entry {
                crc,
                timestamp,
                key_size,
                value_size,
                op,
                key,
                value,
            };

            results.push(EntryParseResult {
                value_pos: offset as u64,
                entry,
            });
            offset += header_size + key_size as usize + value_size as usize;
        }

        results
    }
}

// impl Into<Vec<EntryParseResult>> for EntryFile {
//     fn into(mut self) -> Vec<EntryParseResult> {
//         // crc (4 bytes) | timestamp (8 bytes) | key_size (4 bytes) | value_size (8 bytes) | op (1 bytes) | key | value
//         let header_size = Entry::default().header_size();
//         let mut buffer = vec![0; header_size];

//         let _ = self.0.seek(SeekFrom::Start(0));

//         let mut offset: usize = 0;
//         let mut results = Vec::new();
//         loop {
//             //  crc = 4 |  time = 8 |  ksize = 4 | vsize = 8 | op = 1
//             match self.0.read_exact(&mut buffer) {
//                 Ok(()) => {
//                     // 读取成功，处理 buffer 的内容
//                     //debug!("Successfully read {} bytes", buffer.len());
//                     // 处理 buffer 内容的其他操作
//                 }
//                 Err(ref e) if e.kind() == io::ErrorKind::UnexpectedEof => {
//                     // 文件结束EOF，处理不完整数据
//                     // debug!("Reached end of file");
//                     break; // 退出循环
//                 }
//                 Err(e) => {
//                     // 处理其他 I/O 错误
//                     debug!("Error reading file: {}", e);
//                     break; // 退出循环
//                 }
//             }

//             let crc = u32::from_le_bytes(
//                 buffer[..4]
//                     .try_into()
//                     .map_err(|_| "Invalid CRC data")
//                     .unwrap(),
//             );
//             let timestamp = u64::from_le_bytes(
//                 buffer[4..12]
//                     .try_into()
//                     .map_err(|_| "Invalid timestamp data")
//                     .unwrap(),
//             );
//             let key_size = u32::from_le_bytes(
//                 buffer[12..16]
//                     .try_into()
//                     .map_err(|_| "Invalid key_size data")
//                     .unwrap(),
//             );
//             let value_size = u64::from_le_bytes(
//                 buffer[16..24]
//                     .try_into()
//                     .map_err(|_| "Invalid value_size data")
//                     .unwrap(),
//             );

//             let op = Op::from_le_bytes(
//                 buffer[24..25]
//                     .try_into()
//                     .map_err(|_| "Invalid value_size data")
//                     .unwrap(),
//             )
//             .unwrap();

//             // key
//             let mut key = vec![0; key_size as usize];
//             self.0.read_exact(&mut key).unwrap();

//             // value
//             let mut value = vec![0; value_size as usize];
//             self.0.read_exact(&mut value).unwrap();

//             let entry = Entry {
//                 crc,
//                 timestamp,
//                 key_size,
//                 value_size,
//                 op,
//                 key,
//                 value,
//             };

//             results.push(EntryParseResult {
//                 value_pos: offset as u64,
//                 entry,
//             });
//             offset += header_size + key_size as usize + value_size as usize;
//         }

//         results
//     }
// }

impl fmt::Debug for Entry {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Entry {{ crc: {}, timestamp: {}, key_size: {}, value_size: {}, op:{}, key: {:?}, value: {:?} }}",
            self.crc,
            self.timestamp,
            self.key_size,
            self.value_size,
            self.op,
            util::format_bytes_as_str(&self.key),
                    util:: format_bytes_as_str(&self.value)
        )
    }
}

#[allow(dead_code)]
impl Entry {
    pub fn new(key: Vec<u8>, value: Vec<u8>, timestamp: u64) -> Entry {
        // 将 bytes 转为 entry
        let key_size = key.len() as u32;
        let value_size = value.len() as u64;
        // let timestamp = Utc::now().timestamp() as u64;
        let op = Op::Add;
        // let timestamp = 0;

        let mut entry = Entry {
            crc: 0,
            timestamp,
            key_size,
            value_size,
            op,
            key,
            value,
        };
        entry.crc = entry.calculate_crc();

        entry
    }

    pub fn with_timestamp(mut self, ts: u64) -> Entry {
        self.timestamp = ts;
        self.refresh_crc();
        self
    }

    pub fn set_updated(mut self) -> Entry {
        self.op = Op::Put;
        self.refresh_crc();
        self
    }
    pub fn set_removed(mut self) -> Entry {
        self.op = Op::Del;
        self.refresh_crc();
        self
    }

    fn refresh_crc(&mut self) {
        self.crc = self.calculate_crc();
    }

    fn calculate_crc(&self) -> u32 {
        let mut hasher = Hasher::new();
        hasher.update(&self.timestamp.to_le_bytes());
        hasher.update(&self.key_size.to_le_bytes());
        hasher.update(&self.value_size.to_le_bytes());
        hasher.update(&self.op.to_le_bytes());
        hasher.update(&self.key);
        hasher.update(&self.value);

        hasher.finalize()
    }

    pub fn is_valid(&self) -> bool {
        self.crc == self.calculate_crc()
    }

    pub fn is_expired(&self) -> bool {
        if self.timestamp == 0 {
            return false;
        }

        let now = SystemTime::now()
            .duration_since(UNIX_EPOCH)
            .expect("SystemTime before UNIX EPOCH")
            .as_millis() as u64;

        now > self.timestamp
    }

    pub fn is_removed(&self) -> bool {
        matches!(self.op, Op::Del)
    }

    // crc (4 bytes) | timestamp (8 bytes) | key_size (4 bytes) | value_size (8 bytes) | op (1 bytes) | key | value
    pub fn header_size(&self) -> usize {
        let header_size: usize = 2 * std::mem::size_of::<u32>()
            + 2 * std::mem::size_of::<u64>()
            + std::mem::size_of::<Op>();
        header_size
    }

    pub fn size(&self) -> usize {
        let total_size: usize = self.header_size() + self.key.len() + self.value.len();

        total_size
    }

    pub fn get_value(&self) -> &Vec<u8> {
        &self.value
    }

    pub fn as_bytes(&self) -> Vec<u8> {
        let mut result: Vec<u8> = Vec::new();

        // 将各个字段的字节添加到结果中
        result.extend_from_slice(&self.crc.to_le_bytes());
        result.extend_from_slice(&self.timestamp.to_le_bytes());
        result.extend_from_slice(&self.key_size.to_le_bytes());
        result.extend_from_slice(&self.value_size.to_le_bytes());
        result.extend_from_slice(&self.op.to_le_bytes());
        result.extend_from_slice(&self.key);
        result.extend_from_slice(&self.value);

        result
    }
}

// TryFrom trait
impl TryFrom<Vec<u8>> for Entry {
    type Error = String;

    fn try_from(bytes: Vec<u8>) -> Result<Self, Self::Error> {
        // crc (4 bytes) | timestamp (8 bytes) | key_size (4 bytes) | value_size (8 bytes) | op (1 bytes) | key | value
        let header_size = Entry::default().header_size();
        if bytes.len() < header_size {
            // 除去 key 和 value 之外的固定长度
            return Err("Input Vec<u8> is too short".into());
        }

        let crc = u32::from_le_bytes(bytes[..4].try_into().map_err(|_| "Invalid CRC data")?);
        let timestamp = u64::from_le_bytes(
            bytes[4..12]
                .try_into()
                .map_err(|_| "Invalid timestamp data")?,
        );
        let key_size = u32::from_le_bytes(
            bytes[12..16]
                .try_into()
                .map_err(|_| "Invalid key_size data")?,
        );
        let value_size = u64::from_le_bytes(
            bytes[16..24]
                .try_into()
                .map_err(|_| "Invalid value_size data")?,
        );
        let op = Op::from_le_bytes(bytes[24..25].try_into().map_err(|_| "Invalid op data")?)?;

        if bytes.len() < (header_size + key_size as usize + value_size as usize) {
            return Err("Input Vec<u8> is too short for the key and value".into());
        }

        let key = bytes[header_size..(header_size + key_size as usize)].to_vec();
        let value = bytes[(header_size + key_size as usize)
            ..(header_size + key_size as usize + value_size as usize)]
            .to_vec();

        Ok(Entry {
            crc,
            timestamp,
            key_size,
            value_size,
            op,
            key,
            value,
        })
    }
}

// From trait
impl From<Entry> for Vec<u8> {
    fn from(val: Entry) -> Self {
        // 计算总容量（4 字段的长度 + key 和 value 的长度）
        let total_size = Entry::default().size();
        println!("inro {:?}", val.op.to_le_bytes());
        println!("inro2 {:?}", val.key_size.to_le_bytes());

        let mut result = Vec::with_capacity(total_size);
        result.extend_from_slice(&val.crc.to_le_bytes());
        result.extend_from_slice(&val.timestamp.to_le_bytes());
        result.extend_from_slice(&val.key_size.to_le_bytes());
        result.extend_from_slice(&val.value_size.to_le_bytes());
        result.extend_from_slice(&val.op.to_le_bytes());
        result.extend(val.key);
        result.extend(val.value);
        println!("{:?}", result);
        result
    }
}

// Into trait
// impl Into<Vec<u8>> for Entry {
//     fn into(self) -> Vec<u8> {
//         // 计算总容量（4 字段的长度 + key 和 value 的长度）
//         let total_size = Entry::default().size();
//         println!("inro {:?}", self.op.to_le_bytes());
//         println!("inro2 {:?}", self.key_size.to_le_bytes());

//         let mut result = Vec::with_capacity(total_size);
//         result.extend_from_slice(&self.crc.to_le_bytes());
//         result.extend_from_slice(&self.timestamp.to_le_bytes());
//         result.extend_from_slice(&self.key_size.to_le_bytes());
//         result.extend_from_slice(&self.value_size.to_le_bytes());
//         result.extend_from_slice(&self.op.to_le_bytes());
//         result.extend(self.key);
//         result.extend(self.value);
//         println!("{:?}", result);
//         result
//     }
// }

#[cfg(test)]
mod tests {
    use super::Op;
    use crate::entry::entry::Entry;

    #[test]
    fn entry_new() {
        let key = "foo";
        let value = "1";

        let entry = Entry::new(key.as_bytes().to_vec(), value.as_bytes().to_vec(), 0);
        assert_eq!(entry.key, key.as_bytes().to_vec());
        assert_eq!(entry.value, value.as_bytes().to_vec());

        let crc = 12345;
        assert_ne!(crc, entry.crc);
    }

    #[test]
    fn entry_into_bytes() {
        let key1 = "name".as_bytes();
        let value1 = "li".as_bytes();

        // create an entry
        let entry = Entry::new(key1.to_vec(), value1.to_vec(), 0);
        let e_key = &entry.key;
        let e_value = &entry.value;
        let e_key_size = entry.key_size as u32;
        let e_value_size = entry.value_size as u64;
        let e_timestamp = entry.timestamp;
        let op = Op::Add;
        assert_eq!(key1.to_vec(), *e_key);
        assert_eq!(value1.to_vec(), *e_value);
        assert_eq!(key1.len() as u32, e_key_size);
        assert_eq!(value1.len() as u64, e_value_size);
        assert_eq!(Op::Add, entry.op);

        // entry into
        let bytes: Vec<u8> = entry.into();
        assert_ne!(0, bytes.len());

        // entry try_from
        let rentry = Entry::try_from(bytes);
        assert!(rentry.is_ok());

        let ins = rentry.unwrap();
        assert_eq!(key1.to_vec(), ins.key);
        assert_eq!(value1.to_vec(), ins.value);
        assert_eq!(e_key_size, ins.key_size);
        assert_eq!(e_value_size, ins.value_size);
        assert_eq!(e_timestamp, ins.timestamp);
        assert_eq!(op, ins.op);
        assert!(ins.is_valid());
    }
}
