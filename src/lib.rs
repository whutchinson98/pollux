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
//! use pollux::{MessageReceiver, MessageProcessor, WorkerPool, WorkerPoolConfig};
//! use std::time::Duration;
//!
//! // 1. Implement MessageReceiver for your queue system
//! struct MyReceiver;
//! impl MessageReceiver<String> for MyReceiver {
//!     type Error = Box<dyn std::error::Error + Send + Sync>;
//!
//!     async fn receive_messages(&self) -> Result<Vec<String>, Self::Error> {
//!         // Your queue receiving logic here
//!         Ok(vec!["message1".to_string(), "message2".to_string()])
//!     }
//!
//!     async fn delete_message<S>(&self, receipt_handle: S) -> Result<(), Self::Error>
//!     where
//!         S: AsRef<str> + std::fmt::Debug + Send,
//!     {
//!         // Your message deletion logic here
//!         Ok(())
//!     }
//! }
//!
//! // 2. Implement MessageProcessor for your business logic
//! struct MyProcessor;
//! impl MessageProcessor<String> for MyProcessor {
//!     async fn process_message(
//!         &self,
//!         message: &String,
//!     ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
//!         // Your message processing logic here
//!         println!("Processing: {}", message);
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
//! pool.spawn_workers::<String>();
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
//! - **[`WorkerPool`]**: Manages multiple receiver loops with semaphore-based concurrency control
//!
//! ### How it Works
//!
//! Each receiver loop runs in its own async task and continuously:
//! 1. Fetches messages from the queue (non-blocking)
//! 2. Spawns processing tasks for each message (controlled by semaphore)
//! 3. Immediately fetches more messages without waiting for processing to complete
//! 4. Logs heartbeats showing available permits
//! 5. Automatically restarts on crashes
//!
//! Processing tasks:
//! 1. Acquire a semaphore permit (blocks if at `max_in_flight` limit)
//! 2. Process the message with timeout
//! 3. Release the permit automatically when done
//!
//! This architecture provides high throughput for I/O-bound tasks by decoupling
//! message fetching from processing, allowing receivers to keep the processing
//! pipeline full.
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
use tokio::sync::Semaphore;

/// A trait for message receivers that can be used with the worker pool.
///
/// Implement this trait to integrate your queue system (SQS, RabbitMQ, Redis, etc.)
/// with the worker pool. The trait provides methods to receive messages and delete
/// them after successful processing.
///
/// # Type Parameters
///
/// * `M` - The message type that will be received from the queue
///
/// # Examples
///
/// ```rust
/// use pollux::MessageReceiver;
///
/// struct MyQueueReceiver {
///     queue_url: String,
/// }
///
/// impl MessageReceiver<String> for MyQueueReceiver {
///     type Error = Box<dyn std::error::Error + Send + Sync>;
///
///     async fn receive_messages(&self) -> Result<Vec<String>, Self::Error> {
///         // Implementation to receive messages from your queue
///         Ok(vec!["message1".to_string()])
///     }
///
///     async fn delete_message<S>(&self, receipt_handle: S) -> Result<(), Self::Error>
///     where
///         S: AsRef<str> + std::fmt::Debug + Send,
///     {
///         // Implementation to delete/acknowledge the message
///         println!("Deleting message with handle: {}", receipt_handle.as_ref());
///         Ok(())
///     }
/// }
/// ```
pub trait MessageReceiver<M> {
    type Error: std::fmt::Display + std::fmt::Debug + Send + Sync + 'static;

    /// Receive messages from the source (queue, stream, etc.)
    ///
    /// This method should return a batch of messages from your queue system.
    /// It's called continuously by workers, so it should handle cases where
    /// no messages are available (return empty Vec) and implement appropriate
    /// polling/waiting behavior.
    ///
    /// # Returns
    ///
    /// * `Ok(Vec<M>)` - A vector of messages to process (can be empty)
    /// * `Err(Self::Error)` - An error occurred while receiving messages
    fn receive_messages(&self) -> impl Future<Output = Result<Vec<M>, Self::Error>> + Send;

    /// Delete a given message after it's been processed.
    ///
    /// This method is called after a message has been successfully processed
    /// to remove it from the queue or mark it as acknowledged. The receipt_handle
    /// is typically provided by the queue system when the message was received.
    ///
    /// # Parameters
    ///
    /// * `receipt_handle` - A handle or identifier for the message to delete
    ///
    /// # Returns
    ///
    /// * `Ok(())` - Message was successfully deleted
    /// * `Err(Self::Error)` - An error occurred while deleting the message
    fn delete_message<S>(
        &self,
        receipt_handle: S,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send
    where
        S: AsRef<str> + std::fmt::Debug + Send;
}

/// A trait for message processors that defines how to handle individual messages.
///
/// Implement this trait to define your business logic for processing messages.
/// The trait uses `Box<dyn std::error::Error + Send + Sync>` for maximum flexibility
/// in error handling - you can return any error type that implements the standard Error trait.
///
/// # Type Parameters
///
/// * `M` - The message type that will be processed
///
/// # Error Handling
///
/// Processing errors are automatically logged by the worker pool. Failed messages
/// do not cause the worker to crash - instead, errors are logged and the worker
/// continues processing other messages.
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
///         message: &String,
///     ) -> Result<(), Box<dyn std::error::Error + Send + Sync>> {
///         // Your business logic here
///         println!("Processing message: {}", message);
///         
///         // Example: parse JSON, save to database, call external API, etc.
///         if message.contains("error") {
///             return Err("Message contains error".into());
///         }
///         
///         Ok(())
///     }
/// }
/// ```
pub trait MessageProcessor<M> {
    /// Process a single message
    ///
    /// This method contains your business logic for handling a message.
    /// It should be idempotent when possible, as messages may be retried
    /// if processing fails.
    ///
    /// # Parameters
    ///
    /// * `message` - The message to process
    ///
    /// # Returns
    ///
    /// * `Ok(())` - Message was processed successfully
    /// * `Err(Box<dyn std::error::Error + Send + Sync>)` - Processing failed
    fn process_message(
        &self,
        message: &M,
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
    /// Number of workers to spawn (DEPRECATED: use receiver_count instead)
    ///
    /// This field is deprecated and maintained for backwards compatibility.
    /// It will be used as receiver_count if receiver_count is not explicitly set.
    #[deprecated(since = "0.1.0", note = "use receiver_count instead")]
    pub worker_count: u8,
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
            #[allow(deprecated)]
            worker_count: 3, // Set to same as receiver_count for backwards compatibility
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
/// use pollux::{WorkerPool, WorkerPoolConfig};
/// use std::time::Duration;
///
/// # struct MyReceiver;
/// # struct MyProcessor;
/// # impl pollux::MessageReceiver<String> for MyReceiver {
/// #     type Error = Box<dyn std::error::Error + Send + Sync>;
/// #     async fn receive_messages(&self) -> Result<Vec<String>, Self::Error> { Ok(vec![]) }
/// #     async fn delete_message<S>(&self, _: S) -> Result<(), Self::Error> where S: AsRef<str> + std::fmt::Debug + Send { Ok(()) }
/// # }
/// # impl pollux::MessageProcessor<String> for MyProcessor {
/// #     async fn process_message(&self, _: &String) -> Result<(), Box<dyn std::error::Error + Send + Sync>> { Ok(()) }
/// # }
///
/// // Create a worker pool with custom configuration
/// let config = WorkerPoolConfig {
///     worker_count: 4,
///     processing_timeout: Duration::from_secs(60),
///     ..Default::default()
/// };
///
/// let pool = WorkerPool::new(MyReceiver, MyProcessor, config);
///
/// // Or use the builder pattern
/// let pool = WorkerPool::with_defaults(MyReceiver, MyProcessor)
///     .with_worker_count(8)
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
    /// use pollux::{WorkerPool, WorkerPoolConfig};
    /// use std::time::Duration;
    ///
    /// # struct MyReceiver;
    /// # struct MyProcessor;
    /// # impl pollux::MessageReceiver<String> for MyReceiver {
    /// #     type Error = Box<dyn std::error::Error + Send + Sync>;
    /// #     async fn receive_messages(&self) -> Result<Vec<String>, Self::Error> { Ok(vec![]) }
    /// #     async fn delete_message<S>(&self, _: S) -> Result<(), Self::Error> where S: AsRef<str> + std::fmt::Debug + Send { Ok(()) }
    /// # }
    /// # impl pollux::MessageProcessor<String> for MyProcessor {
    /// #     async fn process_message(&self, _: &String) -> Result<(), Box<dyn std::error::Error + Send + Sync>> { Ok(()) }
    /// # }
    ///
    /// let config = WorkerPoolConfig {
    ///     worker_count: 4,
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
    /// use pollux::WorkerPool;
    ///
    /// # struct MyReceiver;
    /// # struct MyProcessor;
    /// # impl pollux::MessageReceiver<String> for MyReceiver {
    /// #     type Error = Box<dyn std::error::Error + Send + Sync>;
    /// #     async fn receive_messages(&self) -> Result<Vec<String>, Self::Error> { Ok(vec![]) }
    /// #     async fn delete_message<S>(&self, _: S) -> Result<(), Self::Error> where S: AsRef<str> + std::fmt::Debug + Send { Ok(()) }
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

    /// Set the number of workers (builder pattern)
    ///
    /// DEPRECATED: Use `with_receiver_count` instead.
    ///
    /// # Parameters
    ///
    /// * `count` - Number of worker tasks to spawn
    ///
    /// # Examples
    ///
    /// ```rust
    /// use pollux::WorkerPool;
    ///
    /// # struct MyReceiver;
    /// # struct MyProcessor;
    /// # impl pollux::MessageReceiver<String> for MyReceiver {
    /// #     type Error = Box<dyn std::error::Error + Send + Sync>;
    /// #     async fn receive_messages(&self) -> Result<Vec<String>, Self::Error> { Ok(vec![]) }
    /// #     async fn delete_message<S>(&self, _: S) -> Result<(), Self::Error> where S: AsRef<str> + std::fmt::Debug + Send { Ok(()) }
    /// # }
    /// # impl pollux::MessageProcessor<String> for MyProcessor {
    /// #     async fn process_message(&self, _: &String) -> Result<(), Box<dyn std::error::Error + Send + Sync>> { Ok(()) }
    /// # }
    ///
    /// let pool = WorkerPool::with_defaults(MyReceiver, MyProcessor)
    ///     .with_receiver_count(8);
    /// ```
    #[deprecated(since = "0.1.0", note = "use with_receiver_count instead")]
    #[allow(deprecated)]
    pub fn with_worker_count(mut self, count: u8) -> Self {
        self.config.worker_count = count;
        self.config.receiver_count = count;
        self
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
    /// use pollux::WorkerPool;
    /// use std::time::Duration;
    ///
    /// # struct MyReceiver;
    /// # struct MyProcessor;
    /// # impl pollux::MessageReceiver<String> for MyReceiver {
    /// #     type Error = Box<dyn std::error::Error + Send + Sync>;
    /// #     async fn receive_messages(&self) -> Result<Vec<String>, Self::Error> { Ok(vec![]) }
    /// #     async fn delete_message<S>(&self, _: S) -> Result<(), Self::Error> where S: AsRef<str> + std::fmt::Debug + Send { Ok(()) }
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
    /// use pollux::WorkerPool;
    /// use std::time::Duration;
    ///
    /// # struct MyReceiver;
    /// # struct MyProcessor;
    /// # impl pollux::MessageReceiver<String> for MyReceiver {
    /// #     type Error = Box<dyn std::error::Error + Send + Sync>;
    /// #     async fn receive_messages(&self) -> Result<Vec<String>, Self::Error> { Ok(vec![]) }
    /// #     async fn delete_message<S>(&self, _: S) -> Result<(), Self::Error> where S: AsRef<str> + std::fmt::Debug + Send { Ok(()) }
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
    /// use pollux::WorkerPool;
    /// use std::time::Duration;
    ///
    /// # struct MyReceiver;
    /// # struct MyProcessor;
    /// # impl pollux::MessageReceiver<String> for MyReceiver {
    /// #     type Error = Box<dyn std::error::Error + Send + Sync>;
    /// #     async fn receive_messages(&self) -> Result<Vec<String>, Self::Error> { Ok(vec![]) }
    /// #     async fn delete_message<S>(&self, _: S) -> Result<(), Self::Error> where S: AsRef<str> + std::fmt::Debug + Send { Ok(()) }
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
    /// use pollux::WorkerPool;
    ///
    /// # struct MyReceiver;
    /// # struct MyProcessor;
    /// # impl pollux::MessageReceiver<String> for MyReceiver {
    /// #     type Error = Box<dyn std::error::Error + Send + Sync>;
    /// #     async fn receive_messages(&self) -> Result<Vec<String>, Self::Error> { Ok(vec![]) }
    /// #     async fn delete_message(&self, _: impl AsRef<str>) -> Result<(), Self::Error> { Ok(()) }
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
    /// use pollux::WorkerPool;
    ///
    /// # struct MyReceiver;
    /// # struct MyProcessor;
    /// # impl pollux::MessageReceiver<String> for MyReceiver {
    /// #     type Error = Box<dyn std::error::Error + Send + Sync>;
    /// #     async fn receive_messages(&self) -> Result<Vec<String>, Self::Error> { Ok(vec![]) }
    /// #     async fn delete_message(&self, _: impl AsRef<str>) -> Result<(), Self::Error> { Ok(()) }
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
    /// Each receiver loop fetches messages from the queue and spawns tasks to process them.
    /// The total number of concurrent message processing tasks is controlled by the
    /// `max_in_flight` configuration using a shared semaphore.
    ///
    /// Receivers log their activity using the `tracing` crate.
    ///
    /// # Type Parameters
    ///
    /// * `M` - The message type that will be processed
    ///
    /// # Examples
    ///
    /// ```rust
    /// use pollux::WorkerPool;
    ///
    /// # struct MyReceiver;
    /// # struct MyProcessor;
    /// # impl pollux::MessageReceiver<String> for MyReceiver {
    /// #     type Error = Box<dyn std::error::Error + Send + Sync>;
    /// #     async fn receive_messages(&self) -> Result<Vec<String>, Self::Error> { Ok(vec![]) }
    /// #     async fn delete_message<S>(&self, _: S) -> Result<(), Self::Error> where S: AsRef<str> + std::fmt::Debug + Send { Ok(()) }
    /// # }
    /// # impl pollux::MessageProcessor<String> for MyProcessor {
    /// #     async fn process_message(&self, _: &String) -> Result<(), Box<dyn std::error::Error + Send + Sync>> { Ok(()) }
    /// # }
    ///
    /// # async fn example() {
    /// let pool = WorkerPool::with_defaults(MyReceiver, MyProcessor);
    /// pool.spawn_workers::<String>();
    ///
    /// // Receiver loops are now running in the background
    /// // Keep the main thread alive or wait for a signal
    /// // tokio::signal::ctrl_c().await.unwrap();
    /// # }
    /// ```
    pub fn spawn_workers<M>(&self)
    where
        R: MessageReceiver<M> + Send + Sync + 'static,
        P: MessageProcessor<M> + Send + Sync + 'static,
        M: Send + Sync + 'static,
    {
        // Create shared semaphore for controlling max concurrent message processing
        let semaphore = Arc::new(Semaphore::new(self.config.max_in_flight));

        tracing::info!(
            receiver_count = self.config.receiver_count,
            max_in_flight = self.config.max_in_flight,
            "spawning receiver loops"
        );

        for worker_id in 0..self.config.receiver_count {
            let receiver = Arc::clone(&self.receiver);
            let processor = Arc::clone(&self.processor);
            let semaphore = Arc::clone(&semaphore);
            let config = self.config.clone();

            tokio::spawn(async move {
                tracing::info!(worker_id, "spawning receiver loop");
                run_worker_with_id(receiver, processor, config, worker_id, semaphore).await;
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
    /// # Type Parameters
    ///
    /// * `M` - The message type that will be processed
    ///
    /// # Examples
    ///
    /// ```rust
    /// use pollux::WorkerPool;
    ///
    /// # struct MyReceiver;
    /// # struct MyProcessor;
    /// # impl pollux::MessageReceiver<String> for MyReceiver {
    /// #     type Error = Box<dyn std::error::Error + Send + Sync>;
    /// #     async fn receive_messages(&self) -> Result<Vec<String>, Self::Error> { Ok(vec![]) }
    /// #     async fn delete_message<S>(&self, _: S) -> Result<(), Self::Error> where S: AsRef<str> + std::fmt::Debug + Send { Ok(()) }
    /// # }
    /// # impl pollux::MessageProcessor<String> for MyProcessor {
    /// #     async fn process_message(&self, _: &String) -> Result<(), Box<dyn std::error::Error + Send + Sync>> { Ok(()) }
    /// # }
    ///
    /// # async fn example() {
    /// let pool = WorkerPool::with_defaults(MyReceiver, MyProcessor);
    ///
    /// // This will block indefinitely
    /// pool.run_single_worker::<String>(0).await;
    /// # }
    /// ```
    pub async fn run_single_worker<M>(&self, worker_id: u8)
    where
        R: MessageReceiver<M> + Send + Sync + 'static,
        P: MessageProcessor<M> + Send + Sync + 'static,
        M: Send + Sync + 'static,
    {
        let receiver = Arc::clone(&self.receiver);
        let processor = Arc::clone(&self.processor);
        let semaphore = Arc::new(Semaphore::new(self.config.max_in_flight));
        run_worker_with_id(
            receiver,
            processor,
            self.config.clone(),
            worker_id,
            semaphore,
        )
        .await;
    }
}

#[tracing::instrument(skip(receiver, processor, config, semaphore))]
async fn run_worker_with_id<R, P, M>(
    receiver: Arc<R>,
    processor: Arc<P>,
    config: WorkerPoolConfig,
    worker_id: u8,
    semaphore: Arc<Semaphore>,
) where
    R: MessageReceiver<M> + Send + Sync + 'static,
    P: MessageProcessor<M> + Send + Sync + 'static,
    M: Send + Sync + 'static,
{
    loop {
        let worker_result = tokio::spawn({
            let receiver = Arc::clone(&receiver);
            let processor = Arc::clone(&processor);
            let semaphore = Arc::clone(&semaphore);
            let config = config.clone();

            async move {
                tracing::info!(worker_id, "receiver loop started");
                let mut last_heartbeat = Instant::now();

                loop {
                    // Heartbeat logging
                    if last_heartbeat.elapsed() > config.heartbeat_interval {
                        let available_permits = semaphore.available_permits();
                        tracing::info!(
                            worker_id,
                            available_permits,
                            max_in_flight = config.max_in_flight,
                            "receiver heartbeat - still running"
                        );
                        last_heartbeat = Instant::now();
                    }

                    // Receive messages
                    match receiver.receive_messages().await {
                        Ok(messages) => {
                            if messages.is_empty() {
                                continue;
                            }

                            tracing::debug!(
                                worker_id,
                                message_count = messages.len(),
                                available_permits = semaphore.available_permits(),
                                "received messages batch"
                            );

                            // Spawn a task for each message (controlled by semaphore)
                            for message in messages {
                                let processor = Arc::clone(&processor);
                                let semaphore = Arc::clone(&semaphore);
                                let timeout = config.processing_timeout;

                                tokio::spawn(async move {
                                    // Acquire semaphore permit (will wait if at max_in_flight)
                                    let _permit =
                                        semaphore.acquire().await.expect("semaphore closed");

                                    // Process message with timeout
                                    let result = tokio::time::timeout(
                                        timeout,
                                        processor.process_message(&message),
                                    )
                                    .await;

                                    match result {
                                        Ok(Ok(_)) => {
                                            tracing::trace!("message processed successfully");
                                        }
                                        Ok(Err(e)) => {
                                            tracing::error!(
                                                error = ?e,
                                                "error processing message"
                                            );
                                        }
                                        Err(_) => {
                                            tracing::error!(
                                                timeout_secs = timeout.as_secs(),
                                                "message processing timed out"
                                            );
                                        }
                                    }
                                    // Permit is automatically released when _permit is dropped
                                });
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
