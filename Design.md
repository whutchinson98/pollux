# Pollux Design

# Message Receiver
Trait that is implemented to poll for messages and be able to acknowledge them

# Message Processor
This will define what to do with each message that is received from MessageReceiver trait and should be called inside of the WorkerPool

# WorkerPool
This takes a MessageRecevier and MessageProcessor. The WorkerPool should use the MessageReceiver to pool for messages to pass to tokio mpsc channels for processing. We should only allow max_in_flight of messages to be processed at once. As messages finish processing they should send back their status so the MessageReceiver::acknowledge can be called on that message if it's successful. We should only poll for messages if we have room to receive new messages.
