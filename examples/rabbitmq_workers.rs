use std::time::Duration;

use anyhow::Context;
use lapin::{Channel, Connection, ConnectionProperties, options::*, types::FieldTable};
use pollux::{MessageProcessor, MessageReceiver, WorkerPool, WorkerPoolConfig};
use tracing_subscriber::EnvFilter;

#[derive(Clone, Debug)]
pub struct RabbitMQConfig {
    pub queue_name: String,
    pub prefetch_count: u16,
}

impl Default for RabbitMQConfig {
    fn default() -> Self {
        Self {
            queue_name: String::new(),
            prefetch_count: 10,
        }
    }
}

#[derive(Clone)]
pub struct RabbitMQWorker {
    channel: Channel,
    config: RabbitMQConfig,
}

impl RabbitMQWorker {
    pub async fn new(connection: &Connection, config: RabbitMQConfig) -> anyhow::Result<Self> {
        let channel = connection.create_channel().await?;

        // Set QoS (prefetch count)
        channel
            .basic_qos(config.prefetch_count, BasicQosOptions::default())
            .await?;

        // Declare the queue (idempotent operation)
        channel
            .queue_declare(
                &config.queue_name,
                QueueDeclareOptions {
                    durable: true,
                    ..Default::default()
                },
                FieldTable::default(),
            )
            .await?;

        Ok(Self { channel, config })
    }

    pub async fn acknowledge(&self, delivery_tag: u64) -> anyhow::Result<()> {
        self.channel
            .basic_ack(delivery_tag, BasicAckOptions::default())
            .await?;

        Ok(())
    }

    pub async fn receive_message(&self) -> anyhow::Result<Option<lapin::message::Delivery>> {
        let result = self
            .channel
            .basic_get(&self.config.queue_name, BasicGetOptions::default())
            .await?;

        Ok(result.map(|get_ok| get_ok.delivery))
    }
}

#[derive(Clone)]
pub struct RabbitMQMessage {
    pub body: Vec<u8>,
    pub delivery_tag: u64,
}

impl MessageReceiver for RabbitMQWorker {
    type Error = anyhow::Error;
    type Payload = RabbitMQMessage;
    type AckInfo = u64;

    async fn receive_messages(
        &self,
    ) -> Result<Vec<pollux::MessageEnvelope<Self::Payload, Self::AckInfo>>, Self::Error> {
        let mut envelopes = Vec::new();

        // Fetch up to prefetch_count messages
        for _ in 0..self.config.prefetch_count {
            match self.receive_message().await? {
                Some(delivery) => {
                    let delivery_tag = delivery.delivery_tag;
                    let message = RabbitMQMessage {
                        body: delivery.data,
                        delivery_tag,
                    };
                    envelopes.push(pollux::MessageEnvelope::new(message, delivery_tag));
                }
                None => break, // No more messages available
            }
        }

        Ok(envelopes)
    }

    async fn acknowledge(&self, ack_info: Self::AckInfo) -> Result<(), Self::Error> {
        self.acknowledge(ack_info).await
    }
}

pub struct MessageProcessorImpl;

impl MessageProcessor<RabbitMQMessage> for MessageProcessorImpl {
    async fn process_message(
        &self,
        message: &RabbitMQMessage,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
        let body = String::from_utf8_lossy(&message.body);
        println!("Processing message: {:?}", body);

        let wait_time = body.parse::<u64>().unwrap_or(1);
        tokio::time::sleep(Duration::from_secs(wait_time)).await;

        println!("Done processing message: {:?}", body);

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

    let rabbitmq_url = std::env::var("RABBITMQ_URL").context("RABBITMQ_URL is not set")?;

    let queue_name = std::env::var("RABBITMQ_QUEUE_NAME")
        .unwrap_or_else(|_| "rabbitmq-worker-queue".to_string());

    let connection = Connection::connect(&rabbitmq_url, ConnectionProperties::default()).await?;

    let rabbitmq_config = RabbitMQConfig {
        queue_name,
        prefetch_count: 10,
    };

    let rabbitmq_worker = RabbitMQWorker::new(&connection, rabbitmq_config).await?;

    tracing::info!("initialized rabbitmq worker");

    let message_processor = MessageProcessorImpl {};

    let worker_pool_config = WorkerPoolConfig {
        receiver_count: 3,  // 3 receiver loops fetching from queue
        max_in_flight: 100, // Up to 100 messages being processed concurrently
        processing_timeout: Duration::from_secs(120),
        heartbeat_interval: Duration::from_secs(30),
        restart_delay: Duration::from_secs(2),
        ..Default::default()
    };

    let worker_pool = WorkerPool::new(rabbitmq_worker, message_processor, worker_pool_config);

    worker_pool.spawn_workers();

    tokio::signal::ctrl_c().await?;

    println!("done");

    Ok(())
}
