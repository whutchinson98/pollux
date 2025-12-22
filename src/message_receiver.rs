//! This module provides a trait to handle recieving and acknowledging messages.

/// A message envelope that wraps the message payload with acknowledgment information.
///
/// This struct decouples the message content from the queue-specific acknowledgment
/// mechanism, allowing different queue systems to use their own acknowledgment types
/// (e.g., SQS uses String receipt handles, RabbitMQ uses u64 delivery tags).
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
pub trait MessageReceiver {
    /// The error that can be returned by the MessageReceiver
    type Error: std::fmt::Display + std::fmt::Debug + Send + Sync + 'static;
    /// The payload type of each message
    type Payload: Send + Sync + 'static;
    /// The info required to acknowledge a message
    type AckInfo: Send + Sync + 'static;

    /// Receive messages from the source (queue, stream, etc.)
    ///
    /// This method should return a batch of message envelopes from your queue system.
    /// It's called continuously by workers, so it should handle cases where
    /// no messages are available (return empty Vec) and implement appropriate
    /// polling/waiting behavior.
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
    fn acknowledge(
        &self,
        ack_info: Self::AckInfo,
    ) -> impl Future<Output = Result<(), Self::Error>> + Send;
}
