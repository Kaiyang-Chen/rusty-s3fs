# rusty-s3fs



## About

A rust implementation for object storage FUSE, was for AWS s3, currently focus on Google Storage Services. Aims for accelerating model downloading (cold starting) time for LLM serving engine.



## Dependencies

FUSE must be installed to build or run programs that use FUSE-Rust (i.e. kernel driver and libraries. Some platforms may also require userland utils like `fusermount`). A default installation of FUSE is usually sufficient.

To build FUSE-Rust or any program that depends on it, `pkg-config` needs to be installed as well.

### Linux

[FUSE for Linux](https://github.com/libfuse/libfuse/) is available in most Linux distributions and usually called `fuse` or `fuse3` (this crate is compatible with both). To install on a Debian based system:

```
sudo apt-get install fuse
```

Install on CentOS:

```
sudo yum install fuse
```

To build, FUSE libraries and headers are required. The package is usually called `libfuse-dev` or `fuse-devel`. Also `pkg-config` is required for locating libraries and headers.

```
sudo apt-get install libfuse-dev pkg-config
sudo yum install fuse-devel pkgconfig
```

### macOS

Installer packages can be downloaded from the [FUSE for macOS homepage](https://osxfuse.github.io/). This is the *kernel* part that needs to be installed always.

### FreeBSD

Install packages `fusefs-libs` and `pkgconf`.

```
pkg install fusefs-libs pkgconf
```



## Getting started

First, clone the repo and cd to it

```
git clone https://github.com/Kaiyang-Chen/rusty-s3fs.git
cd rusty-s3fs
```

Second, build it with

```
cargo build
```

Last, run it with

```
./target/debug/rusty-s3fs 
    --mount-point MOUNT_POINT
    --bucket-name BUCKET_NAME
    --data-dir CACHE_DATA_DIRECTORY
    --auto_unmount 
    --allow-root
    --direct-io
```

- `mount-point` is the directory path of your mount point
- `bucket-name` is the name of cloud object storage bucket that you want to mount
- `data-dir` is the real data directory on your local filesystem to cache your read from cloud, default to be `/tmp/fuser`
- `auto-unmount` is the option stating whether you want to auto unmount the bucket when the program exists
- `allow-root` is the option stating whether your mount filesystem can be accessed by root
- `direct-io`  is the option stating whether you want to open your file with `FOPEN_DIRECT_IO` flag
