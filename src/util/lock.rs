use fs2::FileExt;
use std::fs::{File, OpenOptions};
use std::io::{self, Error, ErrorKind};

pub struct Locker(File);

impl Locker {
    pub fn acquire() -> Result<Self, io::Error> {
        let file_path = "lock";
        let file = OpenOptions::new()
            .read(true)
            .write(true)
            .create(true)
            .truncate(true)
            .open(file_path)?;

        // 尝试获取独占锁
        match file.try_lock_exclusive() {
            Ok(_) => Ok(Locker(file)),
            Err(_) => Err(Error::new(ErrorKind::Other, "The lock has been used")),
        }
    }

    pub fn release(self) {
        // 锁会在文件关闭时自动释放
        drop(self.0);
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    #[test]
    fn locker_test() -> io::Result<()> {
        // 清理旧的锁文件（如果存在）
        let file_path = "lock";
        let _ = std::fs::remove_file(file_path);

        // 尝试获取第一个锁
        let lock = Locker::acquire();
        assert!(lock.is_ok());

        // 尝试获取第二个锁，应该失败
        let lock2 = Locker::acquire();
        assert!(lock2.is_err());

        // 释放第一个锁
        lock.unwrap().release();

        // 再次尝试获取锁，应该成功
        let lock = Locker::acquire();
        assert!(lock.is_ok());

        // 清理锁文件
        std::fs::remove_file(file_path)?;

        Ok(())
    }
}
