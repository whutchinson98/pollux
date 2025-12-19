//! # Pollux
//!
//! A flexible, asynchronous queue worker pool library for Rust that allows you to easily create
//! and manage pools of workers to process messages from various queue systems in parallel.
//!
//! ## Features
//!
//! - **Generic Design**: Works with any queue system by implementing the [`MessageReceiver`] and [`MessageProcessor`] traits
//! - **Semaphore-Based Concurrency**: Control maximum concurrent message processing with configurable limits
//! - **High Throughput**: Multiple receiver loops continuously fetch messages without blocking
//! - **Fault Tolerance**: Receivers automatically restart on crashes with configurable delays
//! - **Timeout Handling**: Built-in timeout support for message processing
//! - **Structured Logging**: Comprehensive tracing support with receiver IDs and error details
//! - **Concurrent Processing**: Process hundreds of messages concurrently across few receiver loops
//!
//! ## Quick Start
//!
//! ```rust
//! use pollux::{MessageReceiver, MessageProcessor, MessageEnvelope, WorkerPool, WorkerPoolConfig};
//! use std::time::Duration;
//!
//! // 1. Implement MessageReceiver for your queue system
//! struct MyReceiver;
//! impl MessageReceiver for MyReceiver {
//!     type Error = Box<dyn std::error::Error + Send + Sync>;
//!     type Payload = String;
//!     type AckInfo = String;  // SQS uses String receipt handles
//!
//!     async fn receive_messages(&self) -> Result<Vec<MessageEnvelope<Self::Payload, Self::AckInfo>>, Self::Error> {
//!         // Your queue receiving logic here
//!         let envelope1 = MessageEnvelope::new("message1".to_string(), "receipt_1".to_string());
//!         let envelope2 = MessageEnvelope::new("message2".to_string(), "receipt_2".to_string());
//!         Ok(vec![envelope1, envelope2])
//!     }
//!
//!     async fn acknowledge(&self, ack_info: Self::AckInfo) -> Result<(), Self::Error> {
//!         // Your message acknowledgment logic here
//!         println!("Acknowledging: {}", ack_info);
//!         Ok(())
//!     }
//! }
//!
//! // 2. Implement MessageProcessor for your business logic
//! struct MyProcessor;
//! impl MessageProcessor<String> for MyProcessor {
//!     async fn process_message(
//!         &self,
//!         payload: &String,
//!     ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
//!         // Your message processing logic here
//!         println!("Processing: {}", payload);
//!         Ok(())
//!     }
//! }
//!
//! // 3. Create and configure the worker pool
//! # async fn example() {
//! let config = WorkerPoolConfig {
//!     receiver_count: 3,           // Number of receiver loops fetching from queue
//!     max_in_flight: 100,          // Maximum concurrent message processing tasks
//!     processing_timeout: Duration::from_secs(30),
//!     heartbeat_interval: Duration::from_secs(60),
//!     restart_delay: Duration::from_secs(5),
//!     ..Default::default()
//! };
//!
//! let pool = WorkerPool::new(MyReceiver, MyProcessor, config);
//!
//! // 4. Spawn receiver loops (non-blocking)
//! pool.spawn_workers();
//!
//! // Receivers will run indefinitely until the program exits
//! # }
//! ```
//!
//! ## Architecture
//!
//! The library is built around three main components:
//!
//! - **[`MessageReceiver`]**: Defines how to receive messages from your queue system
//! - **[`MessageProcessor`]**: Defines how to process individual messages
//! - **[`WorkerPool`]**: Manages multiple receiver loops with channel-based worker pool
//!
//! ### How it Works
//!
//! Each receiver loop runs in its own async task and continuously:
//! 1. Spawns a fixed pool of `max_in_flight` worker tasks at startup
//! 2. Fetches messages from the queue (non-blocking)
//! 3. Distributes messages to worker tasks via mpsc channels (round-robin)
//! 4. Receives completion notifications from workers
//! 5. Acknowledges successfully processed messages
//! 6. Logs heartbeats showing available workers
//! 7. Automatically restarts on crashes
//!
//! Worker tasks:
//! 1. Wait on their channel to receive work
//! 2. Process the message with timeout
//! 3. Send completion notification back to main loop
//! 4. Wait for next message
//!
//! This architecture provides high throughput and predictable resource usage by:
//! - Using a fixed pool of workers (no unbounded task spawning)
//! - Decoupling message fetching from processing
//! - Allowing the main loop to control acknowledgments based on processing results
//!
//! ## Examples
//!
//! See the `examples/` directory for complete working examples, including:
//! - AWS SQS integration (`examples/sqs_workers.rs`)

use futures::Future;
use std::{
    sync::Arc,
    time::{Duration, Instant},
};
use tokio::sync::mpsc;

/// A message envelope that wraps the message payload with acknowledgment information.
///
/// This struct decouples the message content from the queue-specific acknowledgment
/// mechanism, allowing different queue systems to use their own acknowledgment types
/// (e.g., SQS uses String receipt handles, RabbitMQ uses u64 delivery tags).
///
/// # Type Parameters
///
/// * `P` - The message payload type
/// * `A` - The acknowledgment information type
#[derive(Debug, Clone)]
pub struct MessageEnvelope<P, A> {
    /// The actual message payload to be processed
    pub payload: P,
    /// Queue-specific acknowledgment information (e.g., receipt handle, delivery tag)
    pub ack_info: A,
}

impl<P, A> MessageEnvelope<P, A> {
    /// Create a new message envelope
    pub fn new(payload: P, ack_info: A) -> Self {
        Self { payload, ack_info }
    }
}

/// A trait for message receivers that can be used with the worker pool.
///
/// Implement this trait to integrate your queue system (SQS, RabbitMQ, Redis, etc.)
/// with the worker pool. The trait provides methods to receive messages and acknowledge
/// them after successful processing.
///
/// # Type Parameters
///
/// * `Payload` - The message payload type that will be processed
/// * `AckInfo` - The acknowledgment information type (e.g., receipt handle, delivery tag)
///
/// # Examples
///
/// ```rust
/// use pollux::{MessageReceiver, MessageEnvelope};
///
/// struct MyQueueReceiver {
///     queue_url: String,
/// }
///
/// impl MessageReceiver for MyQueueReceiver {
///     type Error = Box<dyn std::error::Error + Send + Sync>;
///     type Payload = String;
///     type AckInfo = String;
///
///     async fn receive_messages(&self) -> Result<Vec<MessageEnvelope<Self::Payload, Self::AckInfo>>, Self::Error> {
///         // Implementation to receive messages from your queue
///         let envelope = MessageEnvelope::new("message1".to_string(), "receipt_handle_123".to_string());
///         Ok(vec![envelope])
///     }
///
///     async fn acknowledge(&self, ack_info: Self::AckInfo) -> Result<(), Self::Error> {
///         // Implementation to acknowledge/delete the message
///         println!("Acknowledging message with handle: {}", ack_info);
///         Ok(())
///     }
/// }
/// ```
pub trait MessageReceiver {
    type Error: std::fmt::Display + std::fmt::Debug + Send + Sync + 'static;
    type Payload: Send + Sync + 'static;
    type AckInfo: Send + Sync + 'static;

    /// Receive messages from the source (queue, stream, etc.)
    ///
    /// This method should return a batch of message envelopes from your queue system.
    /// It's called continuously by workers, so it should handle cases where
    /// no messages are available (return empty Vec) and implement appropriate
    /// polling/waiting behavior.
    ///
    /// # Returns
    ///
    /// * `Ok(Vec<MessageEnvelope<Self::Payload, Self::AckInfo>>)` - A vector of message envelopes to process (can be empty)
    /// * `Err(Self::Error)` - An error occurred while receiving messages
    #[allow(clippy::type_complexity)]
    fn receive_messages(
        &self,
    ) -> impl Future<
        Output = Result<Vec<MessageEnvelope<Self::Payload, Self::AckInfo>>, Self::Error>,
    > + Send;

    /// Acknowledge a message after it's been successfully processed.
    ///
    /// This method is called after a message has been successfully processed
    /// to remove it from the queue or mark it as acknowledged. The ack_info
    /// is provided by the queue system when the message was received.
    ///
    /// # Parameters
    ///
    /// * `ack_info` - Queue-specific acknowledgment information
    ///
    /// # Returns
    ///
    /// * `Ok(())` - Message was successfully acknowledged
    /// * `Err(Self::Error)` - An error occurred while acknowledging the message
    fn acknowledge(
        &self,
        ack_info: Self::AckInfo,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}

/// A trait for message processors that defines how to handle individual messages.
///
/// Implement this trait to define your business logic for processing messages.
/// The trait uses `Box<dyn std::error::Error + Send + Sync>` for maximum flexibility
/// in error handling - you can return any error type that implements the standard Error trait.
///
/// # Type Parameters
///
/// * `P` - The message payload type that will be processed
///
/// # Error Handling
///
/// Processing errors are automatically logged by the worker pool. Failed messages
/// will NOT be acknowledged, allowing them to be redelivered based on the queue's
/// visibility timeout or retry settings. Successful processing results in automatic
/// acknowledgment.
///
/// # Timeouts
///
/// Message processing is subject to timeouts configured in [`WorkerPoolConfig::processing_timeout`].
/// If processing takes longer than the timeout, it will be cancelled and logged as an error.
///
/// # Examples
///
/// ```rust
/// use pollux::MessageProcessor;
///
/// struct MyProcessor {
///     database_url: String,
/// }
///
/// impl MessageProcessor<String> for MyProcessor {
///     async fn process_message(
///         &self,
///         payload: &String,
///     ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
///         // Your business logic here
///         println!("Processing message: {}", payload);
///
///         // Example: parse JSON, save to database, call external API, etc.
///         if payload.contains("error") {
///             return Err("Message contains error".into());
///         }
///
///         Ok(())
///     }
/// }
/// ```
pub trait MessageProcessor<P> {
    /// Process a single message payload
    ///
    /// This method contains your business logic for handling a message.
    /// It should be idempotent when possible, as messages may be retried
    /// if processing fails.
    ///
    /// # Parameters
    ///
    /// * `payload` - The message payload to process
    ///
    /// # Returns
    ///
    /// * `Ok(())` - Message was processed successfully and will be acknowledged
    /// * `Err(Box<dyn std::error::Error + Send + Sync>)` - Processing failed, message will not be acknowledged
    fn process_message(
        &self,
        payload: &P,
    ) -> impl Future<Output = Result<(), Box<dyn std::error::Error + Send + Sync>>> + Send;
}

/// Configuration for the worker pool
///
/// This struct controls the behavior of the worker pool, including the number of receiver loops,
/// maximum concurrent processing tasks, timeouts, and monitoring intervals.
///
/// # Examples
///
/// ```rust
/// use pollux::WorkerPoolConfig;
/// use std::time::Duration;
///
/// // Use default configuration
/// let config = WorkerPoolConfig::default();
///
/// // Or customize specific values
/// let config = WorkerPoolConfig {
///     receiver_count: 5,
///     max_in_flight: 200,
///     processing_timeout: Duration::from_secs(120),
///     heartbeat_interval: Duration::from_secs(30),
///     restart_delay: Duration::from_secs(10),
///     ..Default::default()
/// };
/// ```
#[derive(Clone, Debug)]
pub struct WorkerPoolConfig {
    /// Number of receiver loops to spawn
    ///
    /// Each receiver loop continuously fetches messages from the queue and spawns
    /// tasks to process them. More receivers can help ensure the queue is polled
    /// frequently, but too many may cause contention.
    /// Default: 3
    pub receiver_count: u8,
    /// Maximum number of messages being processed concurrently
    ///
    /// This controls the global concurrency limit across all receivers using a semaphore.
    /// Higher values allow more parallel processing but consume more resources.
    /// Default: 100
    pub max_in_flight: usize,
    /// Timeout duration for processing individual messages
    ///
    /// If a message takes longer than this duration to process, it will be cancelled
    /// and logged as a timeout error. The worker will continue processing other messages.
    /// Default: 5 minutes
    pub processing_timeout: Duration,
    /// Heartbeat interval for worker health logging
    ///
    /// Workers log a "heartbeat" message at this interval to indicate they are still
    /// running and processing messages. Useful for monitoring worker health.
    /// Default: 60 seconds
    pub heartbeat_interval: Duration,
    /// Delay before restarting crashed workers
    ///
    /// If a worker crashes (due to panic or other fatal error), it will be restarted
    /// after this delay. This prevents rapid restart loops that could consume resources.
    /// Default: 5 seconds
    pub restart_delay: Duration,
}

impl Default for WorkerPoolConfig {
    fn default() -> Self {
        Self {
            receiver_count: 3,
            max_in_flight: 100,
            processing_timeout: Duration::from_secs(300), // 5 minutes
            heartbeat_interval: Duration::from_secs(60),  // 1 minute
            restart_delay: Duration::from_secs(5),        // 5 seconds
        }
    }
}

/// A generic worker pool that can spawn multiple workers to process messages
///
/// The `WorkerPool` coordinates multiple worker tasks that continuously receive and process
/// messages from a queue system. Each worker operates independently and can handle failures
/// gracefully with automatic restarts.
///
/// # Type Parameters
///
/// * `R` - The message receiver type that implements [`MessageReceiver`]
/// * `P` - The message processor type that implements [`MessageProcessor`]
///
/// # Examples
///
/// ```rust
/// use pollux::{WorkerPool, WorkerPoolConfig, MessageEnvelope};
/// use std::time::Duration;
///
/// # struct MyReceiver;
/// # struct MyProcessor;
/// # impl pollux::MessageReceiver for MyReceiver {
/// #     type Error = Box<dyn std::error::Error + Send + Sync>;
/// #     type Payload = String;
/// #     type AckInfo = String;
/// #     async fn receive_messages(&self) -> Result<Vec<MessageEnvelope<Self::Payload, Self::AckInfo>>, Self::Error> { Ok(vec![]) }
/// #     async fn acknowledge(&self, _: Self::AckInfo) -> Result<(), Self::Error> { Ok(()) }
/// # }
/// # impl pollux::MessageProcessor<String> for MyProcessor {
/// #     async fn process_message(&self, _: &String) -> Result<(), Box<dyn std::error::Error + Send + Sync>> { Ok(()) }
/// # }
///
/// // Create a worker pool with custom configuration
/// let config = WorkerPoolConfig {
///     receiver_count: 4,
///     processing_timeout: Duration::from_secs(60),
///     ..Default::default()
/// };
///
/// let pool = WorkerPool::new(MyReceiver, MyProcessor, config);
///
/// // Or use the builder pattern
/// let pool = WorkerPool::with_defaults(MyReceiver, MyProcessor)
///     .with_receiver_count(8)
///     .with_processing_timeout(Duration::from_secs(30));
/// ```
pub struct WorkerPool<R, P> {
    receiver: Arc<R>,
    processor: Arc<P>,
    config: WorkerPoolConfig,
}

impl<R, P> WorkerPool<R, P> {
    /// Create a new worker pool with the given receiver, processor, and configuration
    ///
    /// # Parameters
    ///
    /// * `receiver` - An instance that implements [`MessageReceiver`] for your queue system
    /// * `processor` - An instance that implements [`MessageProcessor`] for your business logic
    /// * `config` - Configuration settings for the worker pool
    ///
    /// # Examples
    ///
    /// ```rust
    /// use pollux::{WorkerPool, WorkerPoolConfig, MessageEnvelope};
    /// use std::time::Duration;
    ///
    /// # struct MyReceiver;
    /// # struct MyProcessor;
    /// # impl pollux::MessageReceiver for MyReceiver {
    /// #     type Error = Box<dyn std::error::Error + Send + Sync>;
    /// #     type Payload = String;
    /// #     type AckInfo = String;
    /// #     async fn receive_messages(&self) -> Result<Vec<MessageEnvelope<Self::Payload, Self::AckInfo>>, Self::Error> { Ok(vec![]) }
    /// #     async fn acknowledge(&self, _: Self::AckInfo) -> Result<(), Self::Error> { Ok(()) }
    /// # }
    /// # impl pollux::MessageProcessor<String> for MyProcessor {
    /// #     async fn process_message(&self, _: &String) -> Result<(), Box<dyn std::error::Error + Send + Sync>> { Ok(()) }
    /// # }
    ///
    /// let config = WorkerPoolConfig {
    ///     receiver_count: 4,
    ///     processing_timeout: Duration::from_secs(120),
    ///     ..Default::default()
    /// };
    ///
    /// let pool = WorkerPool::new(MyReceiver, MyProcessor, config);
    /// ```
    pub fn new(receiver: R, processor: P, config: WorkerPoolConfig) -> Self {
        Self {
            receiver: Arc::new(receiver),
            processor: Arc::new(processor),
            config,
        }
    }

    /// Create a new worker pool with default configuration
    ///
    /// This is a convenience method that creates a worker pool with [`WorkerPoolConfig::default()`].
    /// Use the builder methods to customize the configuration.
    ///
    /// # Parameters
    ///
    /// * `receiver` - An instance that implements [`MessageReceiver`]
    /// * `processor` - An instance that implements [`MessageProcessor`]
    ///
    /// # Examples
    ///
    /// ```rust
    /// use pollux::{WorkerPool, MessageEnvelope};
    ///
    /// # struct MyReceiver;
    /// # struct MyProcessor;
    /// # impl pollux::MessageReceiver for MyReceiver {
    /// #     type Error = Box<dyn std::error::Error + Send + Sync>;
    /// #     type Payload = String;
    /// #     type AckInfo = String;
    /// #     async fn receive_messages(&self) -> Result<Vec<MessageEnvelope<Self::Payload, Self::AckInfo>>, Self::Error> { Ok(vec![]) }
    /// #     async fn acknowledge(&self, _: Self::AckInfo) -> Result<(), Self::Error> { Ok(()) }
    /// # }
    /// # impl pollux::MessageProcessor<String> for MyProcessor {
    /// #     async fn process_message(&self, _: &String) -> Result<(), Box<dyn std::error::Error + Send + Sync>> { Ok(()) }
    /// # }
    ///
    /// let pool = WorkerPool::with_defaults(MyReceiver, MyProcessor);
    /// ```
    pub fn with_defaults(receiver: R, processor: P) -> Self {
        Self::new(receiver, processor, WorkerPoolConfig::default())
    }

    /// Set the processing timeout (builder pattern)
    ///
    /// # Parameters
    ///
    /// * `timeout` - Maximum time allowed for processing a single message
    ///
    /// # Examples
    ///
    /// ```rust
    /// use pollux::{WorkerPool, MessageEnvelope};
    /// use std::time::Duration;
    ///
    /// # struct MyReceiver;
    /// # struct MyProcessor;
    /// # impl pollux::MessageReceiver for MyReceiver {
    /// #     type Error = Box<dyn std::error::Error + Send + Sync>;
    /// #     type Payload = String;
    /// #     type AckInfo = String;
    /// #     async fn receive_messages(&self) -> Result<Vec<MessageEnvelope<Self::Payload, Self::AckInfo>>, Self::Error> { Ok(vec![]) }
    /// #     async fn acknowledge(&self, _: Self::AckInfo) -> Result<(), Self::Error> { Ok(()) }
    /// # }
    /// # impl pollux::MessageProcessor<String> for MyProcessor {
    /// #     async fn process_message(&self, _: &String) -> Result<(), Box<dyn std::error::Error + Send + Sync>> { Ok(()) }
    /// # }
    ///
    /// let pool = WorkerPool::with_defaults(MyReceiver, MyProcessor)
    ///     .with_processing_timeout(Duration::from_secs(30));
    /// ```
    pub fn with_processing_timeout(mut self, timeout: Duration) -> Self {
        self.config.processing_timeout = timeout;
        self
    }

    /// Set the heartbeat interval (builder pattern)
    ///
    /// # Parameters
    ///
    /// * `interval` - How often workers log heartbeat messages
    ///
    /// # Examples
    ///
    /// ```rust
    /// use pollux::{WorkerPool, MessageEnvelope};
    /// use std::time::Duration;
    ///
    /// # struct MyReceiver;
    /// # struct MyProcessor;
    /// # impl pollux::MessageReceiver for MyReceiver {
    /// #     type Error = Box<dyn std::error::Error + Send + Sync>;
    /// #     type Payload = String;
    /// #     type AckInfo = String;
    /// #     async fn receive_messages(&self) -> Result<Vec<MessageEnvelope<Self::Payload, Self::AckInfo>>, Self::Error> { Ok(vec![]) }
    /// #     async fn acknowledge(&self, _: Self::AckInfo) -> Result<(), Self::Error> { Ok(()) }
    /// # }
    /// # impl pollux::MessageProcessor<String> for MyProcessor {
    /// #     async fn process_message(&self, _: &String) -> Result<(), Box<dyn std::error::Error + Send + Sync>> { Ok(()) }
    /// # }
    ///
    /// let pool = WorkerPool::with_defaults(MyReceiver, MyProcessor)
    ///     .with_heartbeat_interval(Duration::from_secs(30));
    /// ```
    pub fn with_heartbeat_interval(mut self, interval: Duration) -> Self {
        self.config.heartbeat_interval = interval;
        self
    }

    /// Set the restart delay (builder pattern)
    ///
    /// # Parameters
    ///
    /// * `delay` - How long to wait before restarting a crashed worker
    ///
    /// # Examples
    ///
    /// ```rust
    /// use pollux::{WorkerPool, MessageEnvelope};
    /// use std::time::Duration;
    ///
    /// # struct MyReceiver;
    /// # struct MyProcessor;
    /// # impl pollux::MessageReceiver for MyReceiver {
    /// #     type Error = Box<dyn std::error::Error + Send + Sync>;
    /// #     type Payload = String;
    /// #     type AckInfo = String;
    /// #     async fn receive_messages(&self) -> Result<Vec<MessageEnvelope<Self::Payload, Self::AckInfo>>, Self::Error> { Ok(vec![]) }
    /// #     async fn acknowledge(&self, _: Self::AckInfo) -> Result<(), Self::Error> { Ok(()) }
    /// # }
    /// # impl pollux::MessageProcessor<String> for MyProcessor {
    /// #     async fn process_message(&self, _: &String) -> Result<(), Box<dyn std::error::Error + Send + Sync>> { Ok(()) }
    /// # }
    ///
    /// let pool = WorkerPool::with_defaults(MyReceiver, MyProcessor)
    ///     .with_restart_delay(Duration::from_secs(10));
    /// ```
    pub fn with_restart_delay(mut self, delay: Duration) -> Self {
        self.config.restart_delay = delay;
        self
    }

    /// Set the number of receiver loops (builder pattern)
    ///
    /// # Parameters
    ///
    /// * `count` - Number of receiver loops to spawn
    ///
    /// # Examples
    ///
    /// ```rust
    /// use pollux::{WorkerPool, MessageEnvelope};
    ///
    /// # struct MyReceiver;
    /// # struct MyProcessor;
    /// # impl pollux::MessageReceiver for MyReceiver {
    /// #     type Error = Box<dyn std::error::Error + Send + Sync>;
    /// #     type Payload = String;
    /// #     type AckInfo = String;
    /// #     async fn receive_messages(&self) -> Result<Vec<MessageEnvelope<Self::Payload, Self::AckInfo>>, Self::Error> { Ok(vec![]) }
    /// #     async fn acknowledge(&self, _: Self::AckInfo) -> Result<(), Self::Error> { Ok(()) }
    /// # }
    /// # impl pollux::MessageProcessor<String> for MyProcessor {
    /// #     async fn process_message(&self, _: &String) -> Result<(), Box<dyn std::error::Error + Send + Sync>> { Ok(()) }
    /// # }
    ///
    /// let pool = WorkerPool::with_defaults(MyReceiver, MyProcessor)
    ///     .with_receiver_count(5);
    /// ```
    pub fn with_receiver_count(mut self, count: u8) -> Self {
        self.config.receiver_count = count;
        self
    }

    /// Set the maximum number of concurrent messages (builder pattern)
    ///
    /// # Parameters
    ///
    /// * `max` - Maximum number of messages being processed concurrently
    ///
    /// # Examples
    ///
    /// ```rust
    /// use pollux::{WorkerPool, MessageEnvelope};
    ///
    /// # struct MyReceiver;
    /// # struct MyProcessor;
    /// # impl pollux::MessageReceiver for MyReceiver {
    /// #     type Error = Box<dyn std::error::Error + Send + Sync>;
    /// #     type Payload = String;
    /// #     type AckInfo = String;
    /// #     async fn receive_messages(&self) -> Result<Vec<MessageEnvelope<Self::Payload, Self::AckInfo>>, Self::Error> { Ok(vec![]) }
    /// #     async fn acknowledge(&self, _: Self::AckInfo) -> Result<(), Self::Error> { Ok(()) }
    /// # }
    /// # impl pollux::MessageProcessor<String> for MyProcessor {
    /// #     async fn process_message(&self, _: &String) -> Result<(), Box<dyn std::error::Error + Send + Sync>> { Ok(()) }
    /// # }
    ///
    /// let pool = WorkerPool::with_defaults(MyReceiver, MyProcessor)
    ///     .with_max_in_flight(200);
    /// ```
    pub fn with_max_in_flight(mut self, max: usize) -> Self {
        self.config.max_in_flight = max;
        self
    }

    /// Spawn the configured number of receiver loops
    ///
    /// This method spawns the receiver tasks and returns immediately. The receivers will
    /// run indefinitely in the background, continuously fetching and processing messages
    /// until the program exits.
    ///
    /// Each receiver loop fetches messages from the queue and distributes them to a fixed
    /// pool of worker tasks via mpsc channels. The total number of concurrent message
    /// processing tasks equals `max_in_flight`.
    ///
    /// Receivers log their activity using the `tracing` crate.
    ///
    /// # Examples
    ///
    /// ```rust
    /// use pollux::WorkerPool;
    ///
    /// # struct MyReceiver;
    /// # struct MyProcessor;
    /// # impl pollux::MessageReceiver for MyReceiver {
    /// #     type Error = Box<dyn std::error::Error + Send + Sync>;
    /// #     type Payload = String;
    /// #     type AckInfo = String;
    /// #     async fn receive_messages(&self) -> Result<Vec<pollux::MessageEnvelope<Self::Payload, Self::AckInfo>>, Self::Error> { Ok(vec![]) }
    /// #     async fn acknowledge(&self, _: Self::AckInfo) -> Result<(), Self::Error> { Ok(()) }
    /// # }
    /// # impl pollux::MessageProcessor<String> for MyProcessor {
    /// #     async fn process_message(&self, _: &String) -> Result<(), Box<dyn std::error::Error + Send + Sync>> { Ok(()) }
    /// # }
    ///
    /// # async fn example() {
    /// let pool = WorkerPool::with_defaults(MyReceiver, MyProcessor);
    /// pool.spawn_workers();
    ///
    /// // Receiver loops are now running in the background
    /// // Keep the main thread alive or wait for a signal
    /// // tokio::signal::ctrl_c().await.unwrap();
    /// # }
    /// ```
    pub fn spawn_workers(&self)
    where
        R: MessageReceiver + Send + Sync + 'static,
        P: MessageProcessor<R::Payload> + Send + Sync + 'static,
        R::Payload: Send + Sync + 'static,
        R::AckInfo: Send + Sync + 'static,
    {
        tracing::info!(
            receiver_count = self.config.receiver_count,
            max_in_flight = self.config.max_in_flight,
            "spawning receiver loops"
        );

        for worker_id in 0..self.config.receiver_count {
            let receiver = Arc::clone(&self.receiver);
            let processor = Arc::clone(&self.processor);
            let config = self.config.clone();

            tokio::spawn(async move {
                tracing::info!(worker_id, "spawning receiver loop");
                run_worker_with_id(receiver, processor, config, worker_id).await;
            });
        }
    }

    /// Run a single receiver loop synchronously
    ///
    /// This method runs a single receiver loop and blocks until it completes (which should never
    /// happen under normal circumstances since receivers run indefinitely). This is primarily
    /// useful for testing or when you want to manage receiver lifecycles manually.
    ///
    /// Most users should prefer [`spawn_workers`](Self::spawn_workers) which spawns receivers
    /// in background tasks.
    ///
    /// # Parameters
    ///
    /// * `worker_id` - Unique identifier for this receiver (used in logging)
    ///
    /// # Examples
    ///
    /// ```rust
    /// use pollux::WorkerPool;
    ///
    /// # struct MyReceiver;
    /// # struct MyProcessor;
    /// # impl pollux::MessageReceiver for MyReceiver {
    /// #     type Error = Box<dyn std::error::Error + Send + Sync>;
    /// #     type Payload = String;
    /// #     type AckInfo = String;
    /// #     async fn receive_messages(&self) -> Result<Vec<pollux::MessageEnvelope<Self::Payload, Self::AckInfo>>, Self::Error> { Ok(vec![]) }
    /// #     async fn acknowledge(&self, _: Self::AckInfo) -> Result<(), Self::Error> { Ok(()) }
    /// # }
    /// # impl pollux::MessageProcessor<String> for MyProcessor {
    /// #     async fn process_message(&self, _: &String) -> Result<(), Box<dyn std::error::Error + Send + Sync>> { Ok(()) }
    /// # }
    ///
    /// # async fn example() {
    /// let pool = WorkerPool::with_defaults(MyReceiver, MyProcessor);
    ///
    /// // This will block indefinitely
    /// pool.run_single_worker(0).await;
    /// # }
    /// ```
    pub async fn run_single_worker(&self, worker_id: u8)
    where
        R: MessageReceiver + Send + Sync + 'static,
        P: MessageProcessor<R::Payload> + Send + Sync + 'static,
        R::Payload: Send + Sync + 'static,
        R::AckInfo: Send + Sync + 'static,
    {
        let receiver = Arc::clone(&self.receiver);
        let processor = Arc::clone(&self.processor);
        run_worker_with_id(receiver, processor, self.config.clone(), worker_id).await;
    }
}

/// Message sent to worker tasks for processing
struct WorkMessage<P, A> {
    envelope: MessageEnvelope<P, A>,
}

/// Completion notification from worker tasks
struct CompletionMessage<A> {
    ack_info: A,
    success: bool,
}

#[tracing::instrument(skip(receiver, processor, config))]
async fn run_worker_with_id<R, P>(
    receiver: Arc<R>,
    processor: Arc<P>,
    config: WorkerPoolConfig,
    worker_id: u8,
) where
    R: MessageReceiver + Send + Sync + 'static,
    P: MessageProcessor<R::Payload> + Send + Sync + 'static,
    R::Payload: Send + Sync + 'static,
    R::AckInfo: Send + Sync + 'static,
{
    loop {
        let worker_result = tokio::spawn({
            let receiver = Arc::clone(&receiver);
            let processor = Arc::clone(&processor);
            let config = config.clone();

            async move {
                tracing::info!(worker_id, "receiver loop started");

                // Create completion channel for workers to send back results
                let (completion_tx, mut completion_rx) =
                    mpsc::channel::<CompletionMessage<R::AckInfo>>(config.max_in_flight * 2);

                // Create work channels and spawn worker tasks
                let mut work_senders = Vec::new();
                for task_id in 0..config.max_in_flight {
                    let (work_tx, mut work_rx) =
                        mpsc::channel::<WorkMessage<R::Payload, R::AckInfo>>(1);
                    work_senders.push(work_tx);

                    let processor = Arc::clone(&processor);
                    let completion_tx = completion_tx.clone();
                    let timeout = config.processing_timeout;

                    // Spawn worker task
                    tokio::spawn(async move {
                        tracing::debug!(worker_id, task_id, "worker task started");

                        while let Some(work_msg) = work_rx.recv().await {
                            let MessageEnvelope { payload, ack_info } = work_msg.envelope;

                            // Process message with timeout
                            let result =
                                tokio::time::timeout(timeout, processor.process_message(&payload))
                                    .await;

                            let success = match result {
                                Ok(Ok(())) => {
                                    tracing::trace!(task_id, "message processed successfully");
                                    true
                                }
                                Ok(Err(e)) => {
                                    tracing::error!(
                                        task_id,
                                        error = ?e,
                                        "error processing message"
                                    );
                                    false
                                }
                                Err(_) => {
                                    tracing::error!(
                                        task_id,
                                        timeout_secs = timeout.as_secs(),
                                        "message processing timed out"
                                    );
                                    false
                                }
                            };

                            // Send completion notification
                            let _ = completion_tx
                                .send(CompletionMessage { ack_info, success })
                                .await;
                        }
                    });
                }

                let mut last_heartbeat = Instant::now();
                let mut next_worker_idx = 0;
                let mut in_flight_count = 0;

                loop {
                    // Heartbeat logging
                    if last_heartbeat.elapsed() > config.heartbeat_interval {
                        let available_workers = config.max_in_flight - in_flight_count;
                        tracing::info!(
                            worker_id,
                            available_workers,
                            in_flight = in_flight_count,
                            max_in_flight = config.max_in_flight,
                            "receiver heartbeat - still running"
                        );
                        last_heartbeat = Instant::now();
                    }

                    // Process any completion messages first
                    while let Ok(completion) = completion_rx.try_recv() {
                        in_flight_count = in_flight_count.saturating_sub(1);

                        if completion.success {
                            if let Err(e) = receiver.acknowledge(completion.ack_info).await {
                                tracing::error!(
                                    worker_id,
                                    error = ?e,
                                    "unable to acknowledge message"
                                );
                            }
                        }
                    }

                    // Check if we have capacity to process messages before fetching
                    if in_flight_count >= config.max_in_flight {
                        tracing::debug!(worker_id, "all workers busy, waiting for completions");
                        // Wait for at least one completion
                        if let Some(completion) = completion_rx.recv().await {
                            in_flight_count = in_flight_count.saturating_sub(1);

                            if completion.success {
                                if let Err(e) = receiver.acknowledge(completion.ack_info).await {
                                    tracing::error!(
                                        worker_id,
                                        error = ?e,
                                        "unable to acknowledge message"
                                    );
                                }
                            }
                        }
                        continue;
                    }

                    // Receive messages from queue
                    match receiver.receive_messages().await {
                        Ok(envelopes) => {
                            if envelopes.is_empty() {
                                continue;
                            }

                            tracing::debug!(
                                worker_id,
                                message_count = envelopes.len(),
                                available_workers = config.max_in_flight - in_flight_count,
                                "received messages batch"
                            );

                            // Distribute messages to worker tasks
                            for envelope in envelopes {
                                // Find next available worker (round-robin)
                                let work_sender =
                                    &work_senders[next_worker_idx % work_senders.len()];
                                next_worker_idx = next_worker_idx.wrapping_add(1);

                                // Send work to worker task
                                if work_sender.send(WorkMessage { envelope }).await.is_ok() {
                                    in_flight_count += 1;
                                } else {
                                    tracing::error!(worker_id, "worker task channel closed");
                                }

                                // Stop if we've reached max capacity
                                if in_flight_count >= config.max_in_flight {
                                    break;
                                }
                            }
                        }
                        Err(e) => {
                            tracing::error!(
                                worker_id,
                                error = ?e,
                                "error receiving messages"
                            );
                            // Brief delay before retrying to avoid tight error loops
                            tokio::time::sleep(Duration::from_secs(1)).await;
                        }
                    }
                }
            }
        })
        .await;

        match worker_result {
            Ok(_) => {
                // This should never be hit since the inner loop is infinite
                tracing::error!(worker_id, "receiver loop exited successfully?");
            }
            Err(e) => {
                tracing::error!(worker_id, error = ?e, "receiver loop crashed with error");
            }
        }

        // Add delay before restarting to avoid rapid restart loops
        tracing::info!(worker_id, "receiver loop restarting...");
        tokio::time::sleep(config.restart_delay).await;
    }
}
