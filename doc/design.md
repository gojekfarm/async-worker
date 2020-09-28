# Introduction to async-worker

async-worker is built on top of RabbitMQ. Although the main API is meant to be agnostic of the implementation details of rabbitmq specific parts, much of the design is still dependent on it. This doc will try to capture the overview and some of the design considerations that went into building this library.

# Queues and exchanges

Rabbitmq does not have out of the box support for message retries. There are some community solutions that work around this limitation, and async-worker uses one of these approaches, which is to define multiple delay queues.

Rabbitmq does support TTL for both messages and queues, with the limitation that the TTL is checked only for the message at the head of the queue. A generic retry mechanism is created on top of these by the following approach:

1. Push all messages being enqueued into an `instant` queue. All consumers consume from this instant queue.
2. If a message fails processing, and needs to be retried, push it into a different queue with a static TTL and a dead letter exchange pointing to the instant queue.
3. When the message is pushed to the delay queue and later expires inside it, the message is routed through the dead letter exchange and it ends up in the instant queue again. A consumer will then pick it up.
4. Async-worker supports exponential backoff, and therefore constructs exchange-queue pairs for each possible backoff duration.
5. The job wrapper in the library will manually push a job to the dead letter queue when all the retries are exhausted. Retries are tracked using metadata wrapping the original message.

# Retry mechanism
The original user message is added into a map containing additional metadata. Of these, `retry-timeout-ms`, `retry-max` and `current-iteration` controls the retry mechanism.

All jobs start with `current-iteration : 0`. Every time the job fails, current-iteration is incremented and added to a delay queue for retry if it is less than `retry-max`.

The exact delay queue to use will be calculated by using the equation:
A
`Timeout duration = (2 ^ current-iteration) * retry-timeout-ms`

# Dead set
Failed messages without retry and those messages with retries exhausted are pushed into a dead-set queue. The `dead-set:replay` function will read messages from this queue and publish them to the instant queue for replaying them. The `current-iteration` remains unchanged and therefore replayed messages will not be retried.

# Reliability

Async-worker always uses Consumer Acknowledgements and has the option to use Publisher Confirms. See: https://www.rabbitmq.com/confirms.html

Consumer Acknowledgements are necessary for atleast-once delivery and cannot be disabled.

Publisher confirms, although a counter part of atleast-once, comes with a significant cost. We have seen it add as much as 32ms latencies for a 3 node configuration.


# Performance

After one round of benchmarking async-library in production, we have made some changes to improve the performance. This includes pooling channels for message publishing and making publisher-confirms optional.

With these changes, we have seen enqueue times drop to sub 1ms from the previous ~30ms.