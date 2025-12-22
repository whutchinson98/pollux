//! This module provides a trait to process the messages that are grabbed via the MessageReciever.

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
