use std::time::Duration;

use anyhow::Context;
use pollux::{MessageProcessor, MessageReceiver, WorkerPool, WorkerPoolConfig};
use tracing_subscriber::EnvFilter;

#[derive(Clone, Debug)]
pub struct SQSConfig {
    pub queue_url: String,
    pub max_messages: i32,
    pub wait_time_seconds: i32,
    pub message_attribute_names: Option<Vec<String>>,
}

impl Default for SQSConfig {
    fn default() -> Self {
        Self {
            queue_url: String::new(),
            max_messages: 10,
            wait_time_seconds: 20,
            message_attribute_names: None,
        }
    }
}

#[derive(Clone)]
pub struct SQSWorker {
    inner: aws_sdk_sqs::Client,
    config: SQSConfig,
}

impl SQSWorker {
    pub fn new(client: aws_sdk_sqs::Client, config: SQSConfig) -> Self {
        Self {
            inner: client,
            config,
        }
    }
}

impl SQSWorker {
    pub async fn delete_message(&self, receipt_handle: &str) -> anyhow::Result<()> {
        if receipt_handle.is_empty() {
            anyhow::bail!("receipt handle is empty");
        }

        self.inner
            .delete_message()
            .queue_url(&self.config.queue_url)
            .receipt_handle(receipt_handle)
            .send()
            .await?;

        Ok(())
    }

    pub async fn receive_messages(&self) -> anyhow::Result<Vec<aws_sdk_sqs::types::Message>> {
        let recv_output = self
            .inner
            .receive_message()
            .queue_url(&self.config.queue_url)
            .wait_time_seconds(self.config.wait_time_seconds)
            .max_number_of_messages(self.config.max_messages)
            .set_message_attribute_names(self.config.message_attribute_names.clone())
            .send()
            .await?;

        Ok(recv_output.messages.unwrap_or_default())
    }
}

impl MessageReceiver for SQSWorker {
    type Error = anyhow::Error;
    type Payload = aws_sdk_sqs::types::Message;
    type AckInfo = String;

    async fn receive_messages(
        &self,
    ) -> Result<Vec<pollux::MessageEnvelope<Self::Payload, Self::AckInfo>>, Self::Error> {
        println!("RECEVING");
        let messages = self.receive_messages().await?;

        // Convert SQS messages to MessageEnvelopes
        let envelopes = messages
            .into_iter()
            .filter_map(|msg| {
                msg.receipt_handle
                    .clone()
                    .map(|receipt| pollux::MessageEnvelope::new(msg, receipt))
            })
            .collect();

        Ok(envelopes)
    }

    async fn acknowledge(&self, ack_info: Self::AckInfo) -> Result<(), Self::Error> {
        self.delete_message(&ack_info).await
    }
}

pub struct MessageProcessorImpl;

impl MessageProcessor<aws_sdk_sqs::types::Message> for MessageProcessorImpl {
    async fn process_message(
        &self,
        message: &aws_sdk_sqs::types::Message,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        if let Some(body) = message.body.as_ref() {
            println!("Processing message: {:?}", body);
            let wait_time = body.parse::<u64>().unwrap();
            tokio::time::sleep(Duration::from_secs(wait_time)).await;
            println!("Done processing message: {:?}", body);
        }

        Ok(())
    }
}

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    dotenv::dotenv().ok();

    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::from_default_env())
        .with_line_number(true)
        .init();

    let queue_url = std::env::var("SQS_QUEUE_URL").context("SQS_QUEUE_URL is not set")?;
    let aws_config = aws_config::defaults(aws_config::BehaviorVersion::latest())
        .region("us-east-1")
        .endpoint_url(&queue_url)
        .load()
        .await;

    let sqs_worker_config = SQSConfig {
        queue_url,
        message_attribute_names: Some(vec!["*".to_string()]),
        ..Default::default()
    };

    let sqs_worker = SQSWorker::new(aws_sdk_sqs::Client::new(&aws_config), sqs_worker_config);

    tracing::info!("initialized sqs worker");

    let message_processor = MessageProcessorImpl {};

    let worker_pool_config = WorkerPoolConfig {
        receiver_count: 3, // 3 receiver loops fetching from queue
        max_in_flight: 3,  // Up to 100 messages being processed concurrently
        processing_timeout: Duration::from_secs(120),
        heartbeat_interval: Duration::from_secs(30),
        restart_delay: Duration::from_secs(2),
    };

    let worker_pool = WorkerPool::new(sqs_worker, message_processor, worker_pool_config);

    worker_pool.spawn_workers();

    tokio::signal::ctrl_c().await?;

    println!("done");

    Ok(())
}
