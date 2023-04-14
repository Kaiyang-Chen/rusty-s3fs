use opendal::Operator;
use opendal::services::Gcs;
use opendal::Metadata;
use futures::TryStreamExt;
// use std::task::{Context, Poll};
// use futures::future::poll_fn;
// use opendal::raw::oio::Read;
// use std::ops::RangeBounds;
// use std::error::Error;
use anyhow::Error;
use tokio::fs::{File, OpenOptions};
use tokio::io::{AsyncSeekExt, AsyncWriteExt, SeekFrom};
use tokio::task::JoinHandle;
// use tokio::runtime::Runtime;

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

    // pub async fn get_data(&self, path: &str) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
    //     let op = Operator::new(self.builder.clone())?.finish();
    
    //     // Get the metadata and file size
    //     let metadata = op.stat(path).await?;
    //     let size = metadata.content_length();
    
    //     // Create a buffer to hold the data in memory temporarily
    //     let mut data = Vec::new();
    
    //     // Define the read block size
    //     let block_size: usize = 512 * 1024 * 1024;
    
    //     // Create a buffer outside the loop to increase efficiency
    //     let mut buffer = vec![0; block_size];
    
    //     // Read the remote file in chunks using range_reader
    //     for offset in (0..size).step_by(block_size) {
    //         let end = std::cmp::min(offset + block_size as u64, size);
    
    //         // Read a range of data from the remote file
    //         let mut range_reader = op.range_reader(path, offset..end).await?;
    
    //         // Resize the buffer if the last chunk is smaller than the block size
    //         if end - offset < block_size as u64 {
    //             buffer.resize((end - offset) as usize, 0);
    //         }
    
    //         // Read the data from the range reader into the buffer using poll_read
    //         let mut bytes_read = 0;
    //         while bytes_read < buffer.len() {
    //             let poll_result = poll_fn(|cx: &mut Context<'_>| range_reader.poll_read(cx, &mut buffer[bytes_read..])).await;
    //             match poll_result {
    //                 Ok(n) => {
    //                     if n == 0 {
    //                         break;
    //                     }
    //                     bytes_read += n;
    //                 }
    //                 Err(e) => return Err(Box::new(e))
    //             }
    //         }
    
    //         // Append the read data to the `data` Vec
    //         data.extend_from_slice(&buffer[0..bytes_read]);
    //     }
    
    //     Ok(data)
    // }    
    

    pub async fn get_data(
        &self,
        path: &str,
        local_file_path: &str,
    ) -> Result<u64, anyhow::Error> {
        let op = Operator::new(self.builder.clone())?.finish();
        let metadata = op.stat(path).await?;
        let size = metadata.content_length();

        // Create and initialize the file
        let mut file = File::create(local_file_path).await?;
        file.set_len(size).await?;

        // Calculate the block size
        let block_size: u64 = 256 * 1024 * 1024;
        let num_threads = (size as f64 / block_size as f64).ceil() as usize;

        // Spawn multiple tasks to read and write different data blocks
        let mut tasks: Vec<JoinHandle<Result<(), Error>>> = Vec::new();
        
        for i in 0..num_threads {
            let mut file_clone = file.try_clone().await?;
            let path_clone = path.to_owned();
            let start = block_size * i as u64;
            let end = std::cmp::min(start + block_size, size);
            let range = start..end;
            let op_clone = op.clone();

            let task = tokio::spawn(async move {
                let data = op_clone.range_read(&path_clone, range).await?;
                file_clone.seek(SeekFrom::Start(start)).await?;
                file_clone.write_all(&data).await?;
                Ok(())
            });
            tasks.push(task);
        }
    
        // Wait for all tasks to complete
        let mut total_bytes_read: u64 = 0;
        for task in tasks {
            match task.await {
                Ok(Ok(_)) => total_bytes_read += block_size,
                Ok(Err(e)) => return Err(e),
                Err(e) => return Err(Error::new(e)),
            }
        }

        // Return the total bytes read
        Ok(total_bytes_read)
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