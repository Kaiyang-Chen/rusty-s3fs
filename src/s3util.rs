use opendal::Operator;
use opendal::services::Gcs;
use opendal::Metadata;
use futures::TryStreamExt;
use std::sync::Arc;
// use std::task::{Context, Poll};
// use futures::future::poll_fn;
// use opendal::raw::oio::Read;
// use std::ops::RangeBounds;
// use std::error::Error;
use tokio::sync::{Mutex, Semaphore};
use tokio::fs::File;
use tokio::io::{BufReader, AsyncSeekExt, copy_buf, SeekFrom, AsyncRead,AsyncBufReadExt};
use tokio::task;
// use tokio::runtime::Runtime;

// A custom AsyncWrite implementation that allows concurrent writes to different positions.
pub struct ConcurrentFile {
    file: Arc<Mutex<tokio::fs::File>>,
}

impl ConcurrentFile {
    pub async fn new(file: tokio::fs::File) -> Self {
        Self {
            file: Arc::new(Mutex::new(file)),
        }
    }

    pub async fn write_at<R: AsyncRead + Unpin + tokio::io::AsyncBufRead>(
        &self,
        mut reader: R,
        pos: u64,
    ) -> Result<u64, std::io::Error> {
        let mut file = self.file.lock().await;
        file.seek(SeekFrom::Start(pos)).await?;

        let bytes_written = copy_buf(&mut reader, &mut *file).await?;
        Ok(bytes_written)
    }
}




pub(crate) struct GcsWorker {
    bucket: String,
    builder: Gcs,
}

impl GcsWorker {
    pub fn new(
        bucket: String,
    ) -> GcsWorker {
        let mut builder = Gcs::default();
        builder.bucket(bucket.as_str());
        // builder.endpoint("http://127.0.0.1:9000");
        // builder.access_key_id("admin");
        // builder.secret_access_key("password");
        GcsWorker {
            bucket,
            builder,
        }
    }

    pub async fn is_exist(&self, path: &str) -> Result<bool, Box<dyn std::error::Error>> {
        let op = Operator::new(self.builder.clone())?.finish();
        let exist = op.is_exist(path).await?;
        Ok(exist)
    }

    pub async fn get_stats(&self, path: &str) -> Result<Metadata, Box<dyn std::error::Error>> {
        let op = Operator::new(self.builder.clone())?.finish();
        let metadata = op.stat(path).await?;
        Ok(metadata)
    }

    pub async fn is_file(&self, path: &str) -> Result<bool, Box<dyn std::error::Error>> {
        let op = Operator::new(self.builder.clone())?.finish();
        let metadata = op.stat(path).await?;
        if metadata.is_file(){
            Ok(true)
        } else {
            Ok(false)
        }
        
    }
    

    pub async fn get_data(
        &self,
        path: &str,
        local_file_path: &str,
        num_threads: usize,
    ) -> Result<u64, anyhow::Error> {
        let op = Operator::new(self.builder.clone())?.finish();
        let metadata = op.stat(path).await?;
        let size = metadata.content_length();

        // Create and initialize the file
        let file = File::create(local_file_path).await?;
        file.set_len(size).await?;
        let concurrent_file = Arc::new(ConcurrentFile::new(file).await);
        let block_size = 64 * 1024 * 1024;
        let num_blocks = (size as f64 / block_size as f64).ceil() as usize;
        let semaphore = Arc::new(Semaphore::new(num_threads));
        let mut tasks = Vec::with_capacity(num_blocks);
        let builder_clone = self.builder.clone();
        for i in 0..num_blocks {
            let start = block_size * i as u64;
            let end = std::cmp::min(start + block_size, size);
            let range = start..end;
            let semaphore_clone = Arc::clone(&semaphore);
            let path_clone = path.to_owned();
            let builder_clone = builder_clone.clone();
            let concurrent_file_clone = Arc::clone(&concurrent_file);

            let task = task::spawn(async move {
                let _permit = semaphore_clone.acquire().await;
                let op_c = Operator::new(builder_clone)?.finish();
        
                let mut reader = op_c.range_reader(&path_clone, range).await?;
                let reader = BufReader::new(reader);
                let bytes_written = concurrent_file_clone.write_at(reader, start).await?;
        
                Ok::<u64, anyhow::Error>(bytes_written)
            });

            tasks.push(task);
        }

        let mut total_bytes_written: u64 = 0;
        for result in futures::future::join_all(tasks).await {
            match result {
                Ok(bytes_written) => total_bytes_written += bytes_written.unwrap(),
                Err(e) => return Err(e.into()),
            }
        }
        // Return the total bytes written
        Ok(total_bytes_written)
    }


    
    

    pub async fn list_dir(&self, path: &str) -> Result<Vec<String>, Box<dyn std::error::Error>>{
        let op = Operator::new(self.builder.clone())?.finish();
        let mut ds = op.list(path).await?;
        let mut filenames = Vec::new();

        while let Some(de) = ds.try_next().await? {
            filenames.push(de.name().to_string());
        }
        Ok(filenames)
    }
}