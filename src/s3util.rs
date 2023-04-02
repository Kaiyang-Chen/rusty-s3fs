use opendal::Operator;
use opendal::services::S3;
use opendal::Metadata;

pub(crate) struct S3Worker {
    bucket: String,
    builder: S3,
}

impl S3Worker {
    pub fn new(
        bucket: String,
    ) -> S3Worker {
        let mut builder = S3::default();
        builder.bucket(bucket.as_str());
        builder.endpoint("http://127.0.0.1:9000");
        builder.access_key_id("admin");
        builder.secret_access_key("password");
        S3Worker {
            bucket,
            builder,
        }
    }

    pub async fn is_exist(&self, path: &str) -> Result<bool, Box<dyn std::error::Error>> {
        let op = Operator::new(self.builder.clone())?.finish();
        let exist = op.is_exist(path).await?;
        println!("{:?}", exist);
        Ok(exist)
    }

    pub async fn get_stats(&self, path: &str) -> Result<Metadata, Box<dyn std::error::Error>> {
        let op = Operator::new(self.builder.clone())?.finish();
        let metadata = op.stat(path).await?;
        println!("{:?}", metadata);
        Ok(metadata)
    }

    pub async fn get_data(&self, path: &str) -> Result<Vec<u8>, Box<dyn std::error::Error>> {
        let op = Operator::new(self.builder.clone())?.finish();
        let data = op.read(path).await?;
        println!("{:?}", data);
        Ok(data)
    }
}