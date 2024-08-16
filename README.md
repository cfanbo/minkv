# MinKV

`minkv` 是一款基于 [Bitcask](https://riak.com/assets/bitcask-intro.pdf) 存储模型构建的轻量级、快速、可靠的 KV 存储引擎，同时支持持久化存储。

其设计思想基于 `LSM (Log-Structured Merge-tree) `数据结构和算法，此算法目前已经非常的成熟，它在许多现代数据库系统和存储引擎中被广泛使用，例如 [LevelDB](https://github.com/google/leveldb)、[RocksDB](https://rocksdb.org/)、[Cassandra](https://github.com/apache/cassandra) 等。

# 安装

## 手动下载安装

下载并解压安装包，将 `minkv` 可执行文件移动到 `/usr/local/bin/` 目录或将其添加到 `$PATH` 环境变量。

下载地址:

- [MacOS x86_64](https://githubfiles.oss-cn-shanghai.aliyuncs.com/minkv/minkv-latest-x86_64-apple-darwin.tar.gz)
- [MacOS aarch64](https://githubfiles.oss-cn-shanghai.aliyuncs.com/minkv/minkv-latest-aarch64-apple-darwin.tar.gz)
- [Linux x86_64](https://githubfiles.oss-cn-shanghai.aliyuncs.com/minkv/minkv-latest-x86_64-unknown-linux-gnu.tar.gz)
- [Linux aarch64](https://githubfiles.oss-cn-shanghai.aliyuncs.com/minkv/minkv-latest-aarch64-unknown-linux-gnu.tar.gz)
- [Windows x86_64](https://githubfiles.oss-cn-shanghai.aliyuncs.com/minkv/minkv-latest-x86_64-pc-windows-msvc.zip)
- [Windows aarch64](https://githubfiles.oss-cn-shanghai.aliyuncs.com/minkv/minkv-latest-aarch64-pc-windows-msvc.zip)

所有发布版本请[点击这里](https://github.com/cfanbo/minkv/releases)

## shell 脚本安装

您可以在macOS终端或Linux shell提示符中粘贴以下命令并执行，注意权限问题。

```shell
/bin/bash -c "$(curl -fsSL https://raw.githubusercontent.com/cfanbo/minkv/HEAD/install.sh)"
```

如果您使用的是 `zsh`的话，命令行前面更改为 `/bin/zsh`即可。

# 配置

创建配置文件
```toml
db_dir = "/server/dbdata"
data = "dbdata"
file_max_size = 10240000

# 当更新key达到指定数量时刷新
sync_keys = 1

[server]
address = "127.0.0.1"
port = 6381
```

字段意义

- `db_dir` 存放数据库文件目录路径

- `data` 表示数据文件名，至少会存在一个 `data` 文件，如果文件进行了分隔，则可能产生 `data.N`文件

- `file_max_size` 表示文件大小达到这个值的时候，将自动进行文件分隔，生成新的数据文件，文件名为 `data.N`

- `sync_keys` 表示写入内容时达到多少次写或删除操作，会刷新缓存到磁盘。

  如果指定为`0`，则表示启用操作系统的缓存刷新磁盘机制
  如果指定为`1` ，则表示每次更新文件后自动调用  `flush()` 函数，对内容进行持久化
  
- `server.address` 表示服务监听 IP 地址

-  `server.port` 表示服务监听端口号

对于  `sync_keys` 的设置一定要根据业务访问量情况设置，如果设置为 `1`，会频繁的进行文件内容同步，可能性能会有一些影响。如果设置的值过大，可能存在意外断电导致部分内容未持久化磁盘，如果此值过大，超出了系统默认的同步周期，系统也会自动同步缓存至磁盘的。


# 启动服务

```shell
$ minkv serve -c config.toml
Listening on 127.0.0.1:6381
```

# 使用

客户端与服务端通讯基于 redis 协议开发，因此可以直接使用 redis 客户端进行访问，只需要指定对应的 `ip:port` 即可。

目前支持的指令

- get
- set
- del
- mset
- mget
- getset
- incr
- decr
- incrby
- decrby
- append
- exists
- expire
- pexpire
- expireat
- pexpireat
- ttl
- pttl
- persist
- keys

以上用户完全与 redis 用法一样。

# 备份与恢复

对于备份只需要简单的复制数据库目录 `db_dir` 里的所有文件即可，同样恢复也是将所有备份文件放在这个目录即可。数据库文件主要是`data` 、`data.N` 数据文件和  `hint.N` 索引文件组成，文件名中n` 表示文件编号，索引文件编号与数据文件编写是一一对应的。

# 其它

后续根据使用场景，可能会支持更多的指令。
