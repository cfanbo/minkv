use std::fmt;
use std::fs::File;
use std::io::{self, Read};

#[derive(Default)]
pub struct Hint {
    pub timestamp: u64,
    pub key_size: u32,
    pub value_size: u64, // entry 大小
    pub value_pos: u64,  // entry pos, 再加上大小，就可以快速定位到整个entry
    pub key: Vec<u8>,
}

// Hint => bytes
impl Into<Vec<u8>> for Hint {
    fn into(self) -> Vec<u8> {
        let mut result: Vec<u8> = Vec::with_capacity(28 + self.key_size as usize);

        // 将各个字段的字节添加到结果中
        result.extend_from_slice(&self.timestamp.to_le_bytes());
        result.extend_from_slice(&self.key_size.to_le_bytes());
        result.extend_from_slice(&self.value_size.to_le_bytes());
        result.extend_from_slice(&self.value_pos.to_le_bytes());
        result.extend_from_slice(&self.key);

        result
    }
}

// | timestamp (8 bytes) | key_size (4 bytes) | value_size (8 bytes) | value_pos (8 bytes)  | key |
pub struct HintFile(File);

impl HintFile {
    pub fn new(file: File) -> HintFile {
        HintFile(file)
    }
}

fn format_bytes_as_str(bytes: &[u8]) -> String {
    match std::str::from_utf8(bytes) {
        Ok(s) => s.to_string(),
        Err(_) => "[invalid UTF-8]".to_string(),
    }
}
impl fmt::Debug for Hint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(
            f,
            "Hint {{  timestamp: {}, key_size: {}, value_size: {},  value_pos: {:?}, value: {:?} }}",
            self.timestamp,
            self.key_size,
            self.value_size,
            self.value_pos,
            format_bytes_as_str(&self.key),
        )
    }
}

impl Into<Vec<Hint>> for HintFile {
    fn into(mut self) -> Vec<Hint> {
        let mut buffer = vec![0; 28];
        let mut hits_result = Vec::new();
        loop {
            //  crc = 4 |  time = 8 |  ksize = 4 | vsize = 8
            match self.0.read_exact(&mut buffer) {
                Ok(()) => {
                    // 读取成功，处理 buffer 的内容
                    // println!("Successfully read {} bytes", buffer.len());
                    // 处理 buffer 内容的其他操作
                }
                Err(ref e) if e.kind() == io::ErrorKind::UnexpectedEof => {
                    // 文件结束EOF，处理不完整数据
                    println!("Reached end of file");
                    break; // 退出循环
                }
                Err(e) => {
                    // 处理其他 I/O 错误
                    eprintln!("Error reading file: {}", e);
                    break; // 退出循环
                }
            }

            let timestamp = u64::from_le_bytes(
                buffer[0..8]
                    .try_into()
                    .map_err(|_| "Invalid timestamp data")
                    .unwrap(),
            );
            let key_size = u32::from_le_bytes(
                buffer[8..12]
                    .try_into()
                    .map_err(|_| "Invalid key_size data")
                    .unwrap(),
            );
            let value_size = u64::from_le_bytes(
                buffer[12..20]
                    .try_into()
                    .map_err(|_| "Invalid value_size data")
                    .unwrap(),
            );

            // value_pos
            let value_pos = u64::from_le_bytes(
                buffer[20..28]
                    .try_into()
                    .map_err(|_| "Invalid value_size data")
                    .unwrap(),
            );

            // key 再次读取指定长度的字节
            let mut key = vec![0; key_size as usize];
            self.0.read_exact(&mut key).unwrap();

            hits_result.push(Hint {
                timestamp,
                key_size,
                value_size,
                value_pos,
                key,
            })
        }

        hits_result
    }
}

#[cfg(test)]
pub mod tests {
    use crate::entry::entry::Entry;
    use std::io::Write;
    use tempfile::NamedTempFile;

    #[test]
    fn hit_write_and_read() -> anyhow::Result<()> {
        use crate::entry::hint::{Hint, HintFile};

        let mut tmpfile = NamedTempFile::new()?;

        let mut offset = 0;
        for num in 1..=2 {
            let key: Vec<u8> = num.to_string().into_bytes();
            let value: Vec<u8> = (num * 2).to_string().into_bytes();

            // let key = "a".as_bytes().to_vec();
            // let value = "1".as_bytes().to_vec();
            let entry = Entry::new(key.clone(), value, 0);

            let ht = Hint {
                timestamp: entry.timestamp,
                key_size: entry.key_size,
                value_pos: offset,
                value_size: entry.size() as u64,
                key: key.clone(),
            };
            offset += entry.size() as u64;

            let config_content: Vec<u8> = ht.into();
            tmpfile.write_all(&config_content)?;
        }

        // let mut tmpfile = NamedTempFile::new()?;
        // let config_content: Vec<u8> = ht.into();
        // tmpfile.write_all(&config_content)?;

        let f = std::fs::File::open(tmpfile.path().to_str().unwrap())?;
        let the_file = HintFile::new(f);
        let result: Vec<Hint> = HintFile::into(the_file);
        assert_eq!(2, result.len());

        let mut offset: u64 = 0;
        for item in result {
            assert_eq!(offset, item.value_pos);
            offset += item.value_size;
            println!("{:?}", item);
        }

        Ok(())
    }
}
