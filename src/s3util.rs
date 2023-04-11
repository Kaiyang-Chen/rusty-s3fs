use opendal::Operator;
use opendal::services::Gcs;
use opendal::Metadata;
use futures::TryStreamExt;

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

    pub async fn get_data(&self, path: &str) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let op = Operator::new(self.builder.clone())?.finish();
        let data = op.read(path).await?;
        Ok(data)
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