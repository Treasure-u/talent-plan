# Talent-plan Rust Course Project2

原文链接:[talent-plan/courses/rust/projects/project-2 at master · pingcap/talent-plan (github.com)](https://github.com/pingcap/talent-plan/tree/master/courses/rust/projects/project-2)

参考链接:[Talent-Plan：用 Rust 实现简易 KV 引擎 - 知乎 (zhihu.com)](https://zhuanlan.zhihu.com/p/558840115)

该 project 主要是重写了返回的错误类型，以及对 set/get/rm/open/recover 操作进行了解析

## 错误处理

因为我们所考虑的错误类型如下：

- set 的时候出现的形式错误
- get 的时候出现 key not found
- rm 的时候没有该 key
- 解析的json 串有问题（后面会对数据转换成 json 写入文件)
- IO 错误

因此我们可以定义这样子的结构体:

```rust
#[derive(Fail, Debug)]
pub enum KvsError {
    /// IO error.
    #[fail(display = "{}", _0)]
    Io(#[cause] io::Error),
    /// Serialization or deserialization error.
    #[fail(display = "{}", _0)]
    Serde(#[cause] serde_json::Error),
    /// Removing non-existent key error.
    #[fail(display = "Key not found")]
    KeyNotFound,
    /// Unexpected command type error.
    /// It indicated a corrupted log or a program bug.
    #[fail(display = "Unexpected command type")]
    UnexpectedCommandType,
}

impl From<io::Error> for KvsError {
    fn from(err: io::Error) -> KvsError {
        KvsError::Io(err)
    }
}

impl From<serde_json::Error> for KvsError {
    fn from(err: serde_json::Error) -> KvsError {
        KvsError::Serde(err)
    }
}
```

## Kvstore 结构体定义

```rust
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
}struct BufWriterWithPos<W: Write + Seek> {
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

```

## 写流程

考虑之前有没有这个 key 存在

- 如果该 key 存在，需要返回改 key 旧的 value 所在的位置，并且对 useless_size 做一个增加
- 如果该 key 不存在，那么就直接插入即可

插入一个 key 的数据的时候可以将其用 serde_json::to_vec 的方式进行

```rust
pub fn set(&mut self, key: String, value: String) -> Result<()> 
{
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

```

## 读流程

在 index 中获取该 key 的索引，如果不存在则说明该 key 不存在直接返回即可，否则根据索引中的 file_number 在 current_readers 中拿到对应的 reader，seek 到对应的 offset 并读取长度为 length 的数据。如果存在则返回 value，否则说明遇到了异常，返回错误即可。

```rust
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

```



## 删除流程

首先在 index 中获取该 key 的索引，如果不存在则说明该 key 不存在返回 ErrNotFound 错误即可，否则移除该索引，接着将 rm 命令序列化并写入到 current_writer 中以保证该 key 能够被确定性删除。注意对于能够找到对应 key 的 rm 命令，useless_size 不仅需要增加 rm 命令本身的长度，还需要增加之前 set 命令的长度，因为此时他们俩都已经可以被一起回收。 

```rust
pub fn remove(&mut self, key: String) -> Result<()> {
        if self.index.get(&key).is_some() {
            self.useless_size += self.index.remove(&key).map(|cp| cp.length).unwrap_or(0);

            let command = serde_json::to_vec(&Command::RM(key))?;
            let offset = self.current_writer.get_position();
            self.current_writer.write_all(&command)?;
            self.current_writer.flush()?;

            self.useless_size += self.current_writer.get_position() - offset;

            if self.useless_size > MAX_USELESS_SIZE {
                self.compact()?;
            }

            Ok(())
        } else {
            Err(KvsError::KeyNotFound)
        }
    }
```





## 重启/open流程

调用 recover 函数，该函数将遍历当前所有的文件，不仅将索引维护到 index 结构中，还会将 reader 维护到 current_readers 结构中，最后返回（当前最大的文件版本，当前所有文件的 useless_size)，接着利用 current_file_number 构建当前最大文件的 writer，需要注意由于 bitcask 模型是 append_only 的机制，所以在构建 writer 时需要使用 OpenOptions 来使得 append 属性为 true，这样重启后直接 append 即可。最后根据 use_less 判断是否需要 compact，最后返回即可。



```rust
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
    
```

## 合并

当 useless_size 大于某个阈值时，会触发一次合并，此时会增加 file_number 并将 index 中所有的数据都写入到当前新建的文件中，同时更新内存中的索引。接着再删除老文件和对应的 reader，最后再新建一个文件承载之后的写入即可。

```rust
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
    
```

