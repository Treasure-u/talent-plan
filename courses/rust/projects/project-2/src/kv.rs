use std::collections::{BTreeMap, HashMap};
use std::fs::{self, create_dir_all, read_dir, remove_file, File, OpenOptions};
use std::io::{self, BufReader, BufWriter, Read, Seek, SeekFrom, Write};
use std::ops::Range;
use std::path::{Path, PathBuf};

use serde::{Deserialize, Serialize};
use serde_json::Deserializer;

use crate::{KvsError, Result};
use std::ffi::OsStr;

const COMPACTION_THRESHOLD: u64 = 1024 * 1024;
const MAX_USELESS_SIZE: u64 = 1024 * 1024;
/// The `KvStore` stores string key/value pairs.
///
/// Key/value pairs are persisted to disk in log files. Log files are named after
/// monotonically increasing generation numbers with a `log` extension name.
/// A `BTreeMap` in memory stores the keys and the value locations for fast query.
///
/// ```rust
/// # use kvs::{KvStore, Result};
/// # fn try_main() -> Result<()> {
/// use std::env::current_dir;
/// let mut store = KvStore::open(current_dir()?)?;
/// store.set("key".to_owned(), "value".to_owned())?;
/// let val = store.get("key".to_owned())?;
/// assert_eq!(val, Some("value".to_owned()));
/// # Ok(())
/// # }
/// ```
pub struct KvStore {
    // 当前 key 存储的位置，这里参照 bitcask 模型,key 就是 kv 当中的 key,value 存储的是该 value 的位置
    // 存放在第 file_number 个文件中的 offset 处,长度为 length
    index: HashMap<String, CommandPosition>,
    // 对于已经存在了的文件,kvstore 缓存了一个 bufreader 来便于 seek 到对应的 offset 来进行 reader
    current_reader: HashMap<u64, BufReader<File>>,
    // 当前正在写入的 file,每次写入只需要 append 并不需要 seek
    current_writer: BufWriterWithPos<File>,
    // 当前最大的 file_number，每次 compaction 之后会新增 1。file_number 越大的文件越新，该 version 能够保证恢复时的正确性
    current_file_number: u64,
    // 文件路径
    dir_path: PathBuf,
    // 无用的数据总和,当达到一定阈值的时候会进行一次 compact
    useless_size: u64,
}
#[derive(Debug)]
pub struct CommandPosition {
    offset: u64,
    length: u64,
    file_number: u64,
}
impl KvStore {
    /// Opens a `KvStore` with the given path.
    ///
    /// This will create a new directory if the given one does not exist.
    ///
    /// # Errors
    ///
    /// It propagates I/O or deserialization errors during the log replay.
    pub fn open(path: impl Into<PathBuf>) -> Result<KvStore> {
        let dir_path = path.into();
        create_dir_all(&dir_path)?;

        let mut index = HashMap::new();

        let mut current_reader = HashMap::new();

        let (current_file_number, useless_size) =
            Self::recover(&dir_path, &mut current_reader, &mut index)?;

        let current_file_path = dir_path.join(format!("data_{}.txt", current_file_number));

        let current_writer = BufWriterWithPos::new(
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(&current_file_path)?,
        )?;

        if current_file_number == 0 {
            current_reader.insert(
                current_file_number,
                BufReader::new(File::open(&current_file_path)?),
            );
        }

        let mut store = KvStore {
            index,
            current_reader,
            current_writer,
            current_file_number,
            dir_path,
            useless_size,
        };

        if store.useless_size > MAX_USELESS_SIZE {
            store.compact()?;
        }

        Ok(store)
    }
    fn recover(
        dir_path: &PathBuf,
        current_readers: &mut HashMap<u64, BufReader<File>>,
        index: &mut HashMap<String, CommandPosition>,
    ) -> Result<(u64, u64)> {
        let mut versions: Vec<u64> = read_dir(dir_path)?
            .flat_map(|res| res.map(|e| e.path()))
            .filter(|path| path.is_file() && path.extension() == Some("txt".as_ref()))
            .flat_map(|path| {
                path.file_name()
                    .and_then(|filename| filename.to_str())
                    .map(|filename| {
                        filename
                            .trim_start_matches("data_")
                            .trim_end_matches(".txt")
                    })
                    .map(str::parse::<u64>)
            })
            .flatten()
            .collect();
        versions.sort();

        let mut useless_size = 0;
        for version in &versions {
            let file_path = dir_path.join(format!("data_{}.txt", version));
            let reader = BufReader::new(File::open(&file_path)?);
            let mut iter = Deserializer::from_reader(reader).into_iter::<Command>();
            let mut before_offset = iter.byte_offset() as u64;
            while let Some(command) = iter.next() {
                let after_offset = iter.byte_offset() as u64;
                match command? {
                    Command::SET(key, _) => {
                        useless_size += after_offset - before_offset;
                        index.insert(
                            key,
                            CommandPosition {
                                offset: before_offset,
                                length: after_offset - before_offset,
                                file_number: *version,
                            },
                        );
                    }
                    Command::RM(key) => {
                        useless_size += index.remove(&key).map(|cp| cp.length).unwrap_or(0);
                        useless_size += after_offset - before_offset;
                    }
                };
                before_offset = after_offset;
            }
            current_readers.insert(*version, BufReader::new(File::open(&file_path)?));
        }

        Ok((*versions.last().unwrap_or(&0), useless_size))
    }
    /// Sets the value of a string key to a string.
    ///
    /// If the key already exists, the previous value will be overwritten.
    ///
    /// # Errors
    ///
    /// It propagates I/O or serialization errors during writing the log.
    pub fn set(&mut self, key: String, value: String) -> Result<()> {
        let cmd = Command::set(key.clone(), value.clone());

        let data = serde_json::to_vec(&cmd).unwrap();
        let offset = self.current_writer.get_position();

        // self.current_writer.write_all(&data)?;
        serde_json::to_writer(&mut self.current_writer, &cmd)?;
        self.current_writer.flush()?;

        let length = self.current_writer.get_position() - offset;
        let file_number = self.current_file_number;
        if let Command::SET(key, _) = cmd {
            self.useless_size += self
                .index
                .insert(
                    key.clone(),
                    CommandPosition {
                        offset,
                        length,
                        file_number,
                    },
                )
                .map(|cp| cp.length)
                .unwrap_or(0);
        }

        if self.useless_size > MAX_USELESS_SIZE {
            self.compact()?;
        }

        Ok(())
    }

    /// Gets the string value of a given string key.
    ///
    /// Returns `None` if the given key does not exist.
    ///
    /// # Errors
    ///
    /// It returns `KvsError::UnexpectedCommandType` if the given command type unexpected.
    pub fn get(&mut self, key: String) -> Result<Option<String>> {
        if let Some(position) = self.index.get(&key) {
            let source_reader = self
                .current_reader
                .get_mut(&position.file_number)
                .expect("Can not find key in files but it is in memory");
            source_reader.seek(SeekFrom::Start(position.offset))?;
            let data_reader = source_reader.take(position.length as u64);

            if let Command::SET(key, value) = serde_json::from_reader(data_reader)? {
                Ok(Some(value))
            } else {
                Err(KvsError::UnexpectedCommandType)
            }
        } else {
            Ok(None)
        }
    }

    /// Removes a given key.
    ///
    /// # Errors
    ///
    /// It returns `KvsError::KeyNotFound` if the given key is not found.
    ///
    /// It propagates I/O or serialization errors during writing the log.
    pub fn remove(&mut self, key: String) -> Result<()> {
        if self.index.contains_key(&key) {
            let cmd = Command::remove(key);
            serde_json::to_writer(&mut self.current_writer, &cmd)?;
            self.current_writer.flush()?;
            if let Command::RM(key) = cmd {
                let old_cmd = self.index.remove(&key).expect("key not found");
                self.useless_size += old_cmd.length;
            }
            Ok(())
        } else {
            Err(KvsError::KeyNotFound)
        }
    }

    /// Clears stale entries in the log.
    fn compact(&mut self) -> Result<()> {
        self.create_new_file()?;

        let mut before_offset = 0;
        for position in self.index.values_mut() {
            let source_reader = self
                .current_reader
                .get_mut(&position.file_number)
                .expect("Can not find key in files but it is in memory");
            source_reader.seek(SeekFrom::Start(position.offset))?;
            let mut data_reader = source_reader.take(position.length);
            io::copy(&mut data_reader, &mut self.current_writer)?;
            let after_offset = self.current_writer.pos;
            *position = CommandPosition {
                offset: before_offset,
                length: after_offset - before_offset,
                file_number: self.current_file_number,
            };
            before_offset = after_offset;
        }
        self.current_writer.flush()?;

        let delete_file_numbers: Vec<u64> = self
            .current_reader
            .iter()
            .map(|(key, _)| *key)
            .filter(|key| *key < self.current_file_number)
            .collect();

        for number in delete_file_numbers {
            self.current_reader.remove(&number);
            remove_file(self.dir_path.join(format!("data_{}.txt", number)))?;
        }

        self.create_new_file()?;

        Ok(())
    }
    /// Create a new log file with given generation number and add the reader to the readers map.
    ///
    /// Returns the writer to the log.
    // fn new_log_file(&mut self, gen: u64) -> Result<BufWriterWithPos<File>> {
    //     new_log_file(&self.dir_path, gen, &mut self.current_reader)
    // }

    fn create_new_file(&mut self) -> Result<()> {
        self.current_file_number += 1;
        let new_file_path = self
            .dir_path
            .join(format!("data_{}.txt", self.current_file_number));
        self.current_writer = BufWriterWithPos::new(
            OpenOptions::new()
                .create(true)
                .append(true)
                .open(&new_file_path)?,
        )?;
        self.current_reader.insert(
            self.current_file_number,
            BufReader::new(File::open(new_file_path)?),
        );

        Ok(())
    }
}

/// Struct representing a command.
#[derive(Serialize, Deserialize, Debug)]
enum Command {
    /// for set command
    SET(String, String),
    /// for rm command
    RM(String),
}

impl Command {
    fn set(key: String, value: String) -> Command {
        Command::SET(key, value)
    }

    fn remove(key: String) -> Command {
        Command::RM(key)
    }
}

/// Represents the position and length of a json-serialized command in the log.
struct CommandPos {
    gen: u64,
    pos: u64,
    len: u64,
}

impl From<(u64, Range<u64>)> for CommandPos {
    fn from((gen, range): (u64, Range<u64>)) -> Self {
        CommandPos {
            gen,
            pos: range.start,
            len: range.end - range.start,
        }
    }
}

struct BufReaderWithPos<R: Read + Seek> {
    reader: BufReader<R>,
    pos: u64,
}

impl<R: Read + Seek> BufReaderWithPos<R> {
    fn new(mut inner: R) -> Result<Self> {
        let pos = inner.seek(SeekFrom::Current(0))?;
        Ok(BufReaderWithPos {
            reader: BufReader::new(inner),
            pos,
        })
    }
}

impl<R: Read + Seek> Read for BufReaderWithPos<R> {
    fn read(&mut self, buf: &mut [u8]) -> io::Result<usize> {
        let len = self.reader.read(buf)?;
        self.pos += len as u64;
        Ok(len)
    }
}

impl<R: Read + Seek> Seek for BufReaderWithPos<R> {
    fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
        self.pos = self.reader.seek(pos)?;
        Ok(self.pos)
    }
}

struct BufWriterWithPos<W: Write + Seek> {
    writer: BufWriter<W>,
    pos: u64,
}

impl<W: Write + Seek> BufWriterWithPos<W> {
    fn new(mut inner: W) -> Result<Self> {
        let pos = inner.seek(SeekFrom::Current(0))?;
        Ok(BufWriterWithPos {
            writer: BufWriter::new(inner),
            pos,
        })
    }
    fn get_position(&self) -> u64 {
        self.pos
    }
}

impl<W: Write + Seek> Write for BufWriterWithPos<W> {
    fn write(&mut self, buf: &[u8]) -> io::Result<usize> {
        let len = self.writer.write(buf)?;
        self.pos += len as u64;
        Ok(len)
    }

    fn flush(&mut self) -> io::Result<()> {
        self.writer.flush()
    }
}

// impl<W: Write + Seek> Seek for BufWriterWithPos<W> {
//     fn seek(&mut self, pos: SeekFrom) -> io::Result<u64> {
//         self.pos = self.writer.seek(pos)?;
//         Ok(self.pos)
//     }
// }
