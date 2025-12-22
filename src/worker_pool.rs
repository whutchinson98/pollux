//! This module contains the configuration and implementation for the worker pool

use tokio::sync::{Semaphore, mpsc};

use crate::{MessageEnvelope, MessageProcessor, MessageReceiver};

use std::{sync::Arc, time::Duration};

/// Result of processing a message, sent through mpsc channel for acknowledgment
struct ProcessingResult<A> {
    /// The acknowledgment info from the original message
    ack_info: A,
    /// Whether the message was processed successfully
    success: bool,
    /// Error if processing failed
    error: Option<Box<dyn std::error::Error + Send + Sync>>,
}

/// Spawn a processor task for a single message with timeout handling
fn spawn_processor_task<P, Payload, AckInfo>(
    processor: Arc<P>,
    message: MessageEnvelope<Payload, AckInfo>,
    result_tx: mpsc::Sender<ProcessingResult<AckInfo>>,
    timeout: Duration,
    _permit: tokio::sync::OwnedSemaphorePermit,
) where
    P: MessageProcessor<Payload> + Send + Sync + 'static,
    Payload: Send + Sync + 'static,
    AckInfo: Send + Sync + 'static,
{
    tokio::spawn(async move {
        let MessageEnvelope { payload, ack_info } = message;

        // Process message with timeout
        let process_result =
            tokio::time::timeout(timeout, processor.process_message(&payload)).await;

        let result = match process_result {
            Ok(Ok(())) => {
                // Processing succeeded
                ProcessingResult {
                    ack_info,
                    success: true,
                    error: None,
                }
            }
            Ok(Err(e)) => {
                // Processing failed
                ProcessingResult {
                    ack_info,
                    success: false,
                    error: Some(e),
                }
            }
            Err(_) => {
                // Timeout
                ProcessingResult {
                    ack_info,
                    success: false,
                    error: Some("processing timeout".into()),
                }
            }
        };

        // Send result back for acknowledgment
        // If this fails, the channel is closed and we're shutting down
        if let Err(e) = result_tx.send(result).await {
            tracing::error!(error = %e, "failed to send processing result");
        }

        // Permit is automatically dropped here
    });
}

/// Configuration for the worker pool
///
/// This struct controls the behavior of the worker pool, including the number of receiver loops
#[derive(Clone, Debug)]
pub struct WorkerPoolConfig {
    /// Number of receiver loops to spawn
    ///
    /// Each receiver will loop continuously to try and fetch messages until all available tasks are processing messages
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
pub struct WorkerPool<R, P> {
    receiver: Arc<R>,
    processor: Arc<P>,
    config: WorkerPoolConfig,
}

impl<R, P> WorkerPool<R, P> {
    /// Create a new worker pool with the given receiver, processor, and configuration
    pub fn new(receiver: R, processor: P, config: WorkerPoolConfig) -> Self {
        Self {
            receiver: Arc::new(receiver),
            processor: Arc::new(processor),
            config,
        }
    }

    /// Create a new worker pool with default configuration
    pub fn with_defaults(receiver: R, processor: P) -> Self {
        Self::new(receiver, processor, WorkerPoolConfig::default())
    }

    /// Set the processing timeout (builder pattern)
    pub fn with_processing_timeout(mut self, timeout: Duration) -> Self {
        self.config.processing_timeout = timeout;
        self
    }

    /// Set the heartbeat interval (builder pattern)
    pub fn with_heartbeat_interval(mut self, interval: Duration) -> Self {
        self.config.heartbeat_interval = interval;
        self
    }

    /// Set the restart delay (builder pattern)
    pub fn with_restart_delay(mut self, delay: Duration) -> Self {
        self.config.restart_delay = delay;
        self
    }

    /// Set the number of receiver loops (builder pattern)
    pub fn with_receiver_count(mut self, count: u8) -> Self {
        self.config.receiver_count = count;
        self
    }

    /// Set the maximum number of concurrent messages (builder pattern)
    pub fn with_max_in_flight(mut self, max: usize) -> Self {
        self.config.max_in_flight = max;
        self
    }

    /// Spawn the acknowledgment task that receives processing results and acknowledges successful messages
    fn spawn_ack_task(
        receiver: Arc<R>,
        mut result_rx: mpsc::Receiver<ProcessingResult<R::AckInfo>>,
        semaphore: Arc<Semaphore>,
    ) where
        R: MessageReceiver + Send + Sync + 'static,
        R::AckInfo: Send + Sync + 'static,
    {
        tokio::spawn(async move {
            tracing::info!("acknowledgment task started");

            while let Some(result) = result_rx.recv().await {
                if result.success {
                    match receiver.acknowledge(result.ack_info).await {
                        Ok(_) => {
                            tracing::debug!("message acknowledged successfully");
                        }
                        Err(e) => {
                            tracing::error!(error = %e, "failed to acknowledge message");
                        }
                    }
                } else if let Some(error) = result.error {
                    tracing::error!(error = %error, "message processing failed");
                }

                // Release the semaphore permit
                semaphore.add_permits(1);
            }

            tracing::warn!("acknowledgment task channel closed, shutting down");
        });
    }

    /// Spawn a single receiver loop with auto-restart capability
    fn spawn_receiver_loop(
        receiver_id: u8,
        receiver: Arc<R>,
        processor: Arc<P>,
        result_tx: mpsc::Sender<ProcessingResult<R::AckInfo>>,
        semaphore: Arc<Semaphore>,
        config: WorkerPoolConfig,
    ) where
        R: MessageReceiver + Send + Sync + 'static,
        P: MessageProcessor<R::Payload> + Send + Sync + 'static,
        R::Payload: Send + Sync + 'static,
        R::AckInfo: Send + Sync + 'static,
    {
        tokio::spawn(async move {
            loop {
                tracing::info!(receiver_id, "starting receiver loop");

                let result = Self::receiver_loop_inner(
                    receiver_id,
                    Arc::clone(&receiver),
                    Arc::clone(&processor),
                    result_tx.clone(),
                    Arc::clone(&semaphore),
                    config.clone(),
                )
                .await;

                if let Err(e) = result {
                    tracing::error!(receiver_id, error = %e, "receiver loop crashed, restarting in 3 seconds");
                    tokio::time::sleep(Duration::from_secs(3)).await;
                } else {
                    tracing::warn!(receiver_id, "receiver loop exited normally");
                    break;
                }
            }
        });
    }

    /// Inner receiver loop that polls for messages and spawns processor tasks
    async fn receiver_loop_inner(
        receiver_id: u8,
        receiver: Arc<R>,
        processor: Arc<P>,
        result_tx: mpsc::Sender<ProcessingResult<R::AckInfo>>,
        semaphore: Arc<Semaphore>,
        config: WorkerPoolConfig,
    ) -> Result<(), Box<dyn std::error::Error + Send + Sync>>
    where
        R: MessageReceiver + Send + Sync + 'static,
        P: MessageProcessor<R::Payload> + Send + Sync + 'static,
        R::Payload: Send + Sync + 'static,
        R::AckInfo: Send + Sync + 'static,
    {
        let mut heartbeat_interval = tokio::time::interval(config.heartbeat_interval);
        heartbeat_interval.set_missed_tick_behavior(tokio::time::MissedTickBehavior::Skip);

        loop {
            tokio::select! {
                _ = heartbeat_interval.tick() => {
                    tracing::debug!(
                        receiver_id,
                        available_permits = semaphore.available_permits(),
                        "receiver loop heartbeat"
                    );
                }
                _ = async {
                    // Check if there are available permits before polling
                    if semaphore.available_permits() == 0 {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        return;
                    }

                    // Poll for messages
                    let messages = match receiver.receive_messages().await {
                        Ok(msgs) => msgs,
                        Err(e) => {
                            tracing::error!(receiver_id, error = %e, "failed to receive messages");
                            return;
                        }
                    };

                    // If no messages, sleep briefly before next poll
                    if messages.is_empty() {
                        tokio::time::sleep(Duration::from_millis(100)).await;
                        return;
                    }

                    tracing::debug!(receiver_id, message_count = messages.len(), "received messages");

                    // Process each message
                    for message in messages {
                        // Acquire semaphore permit (this will wait if none available)
                        let permit = match semaphore.clone().acquire_owned().await {
                            Ok(p) => p,
                            Err(_) => {
                                tracing::error!(receiver_id, "failed to acquire semaphore permit");
                                continue;
                            }
                        };

                        spawn_processor_task(
                            Arc::clone(&processor),
                            message,
                            result_tx.clone(),
                            config.processing_timeout,
                            permit,
                        );
                    }
                } => {}
            }
        }
    }

    /// Spawn the configured number of receiver loops
    pub fn spawn_workers(&self)
    where
        R: MessageReceiver + Send + Sync + 'static,
        P: MessageProcessor<R::Payload> + Send + Sync + 'static,
        R::Payload: Send + Sync + 'static,
        R::AckInfo: Send + Sync + 'static,
    {
        // Create shared semaphore for controlling max concurrent message processing
        let semaphore = Arc::new(Semaphore::new(self.config.max_in_flight));

        // Create bounded mpsc channel for processing results
        let (result_tx, result_rx) = mpsc::channel(self.config.max_in_flight);

        tracing::info!(
            receiver_count = self.config.receiver_count,
            max_in_flight = self.config.max_in_flight,
            "spawning worker pool"
        );

        // Spawn acknowledgment task
        Self::spawn_ack_task(
            Arc::clone(&self.receiver),
            result_rx,
            Arc::clone(&semaphore),
        );

        // Spawn receiver loops
        for receiver_id in 0..self.config.receiver_count {
            Self::spawn_receiver_loop(
                receiver_id,
                Arc::clone(&self.receiver),
                Arc::clone(&self.processor),
                result_tx.clone(),
                Arc::clone(&semaphore),
                self.config.clone(),
            );
        }

        tracing::info!("worker pool started successfully");
    }
}
