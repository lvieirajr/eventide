# Eventide

[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)
[![Python Versions](https://img.shields.io/badge/python-3.9%20%7C%203.10%20%7C%203.11%20%7C%203.12%20%7C%203.13-blue)](https://pypi.org/project/eventide/)

A fast, simple, and extensible queue worker framework for Python.

## Overview

Eventide is a lightweight framework designed to simplify the process of consuming and processing messages from various queue systems. It provides a clean, modular architecture with built-in support for AWS SQS and Cloudflare Queues, with an extensible design that allows for easy integration with other queue providers.

### Key Features

- **Multi-queue support**: Process messages from multiple queues simultaneously
- **Multi-worker architecture**: Scale horizontally with multiple worker processes
- **Declarative message handling**: Simple decorator-based message routing
- **JMESPath-based message matching**: Powerful pattern matching for message routing
- **Graceful shutdown handling**: Clean shutdown on system signals
- **Provider-agnostic**: Built-in support for AWS SQS and Cloudflare Queues, with an extensible interface
- **Type safety**: Fully typed with Pydantic models for configuration and validation

## Installation

```bash
# Basic installation
pip install eventide

# With AWS SQS support
pip install eventide[sqs]

# With Cloudflare Queues support
pip install eventide[cloudflare]

# With all providers
pip install eventide[sqs,cloudflare]
```

## Quick Start

### Basic Usage

```python
from eventide import Eventide, EventideConfig, SQSQueueConfig, eventide_handler

# Define a message handler
@eventide_handler("body.type == 'greeting'")
def handle_greeting(message):
    print(f"Received greeting: {message.body.get('content')}")
    return True

# Configure and start Eventide
config = EventideConfig(
    queues=[
        SQSQueueConfig(
            name="my-queue",
            url="https://sqs.us-east-1.amazonaws.com/123456789012/my-queue",
            region="us-east-1",
        ),
    ],
    workers=2,  # Number of worker processes
)

# Start the application
Eventide(config).run()
```

### Using Multiple Queue Types

```python
from eventide import (
    Eventide,
    EventideConfig,
    SQSQueueConfig,
    CloudflareQueueConfig,
)

# Configure multiple queues
config = EventideConfig(
    queues=[
        SQSQueueConfig(
            name="sqs-queue",
            url="https://sqs.us-east-1.amazonaws.com/123456789012/my-queue",
            region="us-east-1",
        ),
        CloudflareQueueConfig(
            name="cf-queue",
            queue_id="my-queue-id",
            account_id="my-account-id",
            batch_size=10,
            visibility_timeout_ms=120000
        ),
    ],
    workers=4,
)

# Start the application
Eventide(config).run()
```

## Message Handling

Eventide uses a decorator-based approach for message handling. The `@eventide_handler` decorator registers functions to process messages that match specific patterns.

### Basic Handler

```python
from eventide import eventide_handler

@eventide_handler("body.type == 'email'")
def process_email(message):
    # Process email message
    print(f"Processing email: {message.body}")
    return True  # Acknowledge the message
```

### Multiple Matchers

```python
from eventide import eventide_handler

@eventide_handler(
    "body.type == 'notification'",
    "body.priority == 'high'",
    operator="and"  # Can be "and", "or", "all", or "any"
)
def process_high_priority_notification(message):
    # Process high priority notification
    print(f"High priority notification: {message.body}")
    return True
```

## Architecture

Eventide uses a multiprocess architecture for efficient message processing:

1. **Main Process**: Orchestrates the lifecycle of the application
2. **Queue Threads**: Pull messages from external queues into internal buffers
3. **Worker Processes**: Consume messages from buffers and route them to handlers
4. **Handler Threads**: Execute the actual message processing logic

This architecture allows for:
- Efficient resource utilization
- Isolation between message processing
- Graceful shutdown handling
- Scalability across CPU cores

## Configuration

### Eventide Configuration

```python
from eventide import EventideConfig

config = EventideConfig(
    # Directories to search for handler functions
    handler_discovery_paths={"path/to/handlers"},

    # Queue configurations
    queues=[...],

    # Worker configurations (can be a number or list of WorkerConfig)
    workers=4,  # or [WorkerConfig(name="worker1"), WorkerConfig(name="worker2")]
)
```

### Queue Configuration

Eventide provides built-in support for multiple queue types:

#### SQS Queue

```python
from eventide import SQSQueueConfig

sqs_config = SQSQueueConfig(
    name="my-sqs-queue",
    url="https://sqs.us-east-1.amazonaws.com/123456789012/my-queue",
    region="us-east-1",
    visibility_timeout=30,  # seconds
    max_number_of_messages=10,
    wait_time_seconds=20,  # for long polling
    size=100,  # internal buffer size
    min_poll_interval=1.0,  # seconds
    max_poll_interval=10.0,  # seconds
)
```

#### Cloudflare Queue

```python
from eventide import CloudflareQueueConfig

cf_config = CloudflareQueueConfig(
    name="my-cf-queue",
    queue_id="my-queue-id",
    account_id="my-account-id",
    batch_size=10,
    visibility_timeout_ms=30000,  # milliseconds
    size=100,  # internal buffer size
    min_poll_interval=1.0,  # seconds
    max_poll_interval=10.0,  # seconds
)
```

### Worker Configuration

```python
from eventide import WorkerConfig

worker_config = WorkerConfig(
    name="my-worker",
    timeout=600.0,  # maximum handler execution time in seconds
)
```

## Message Routing with JMESPath

Eventide uses JMESPath expressions to route messages to the appropriate handlers. This provides a powerful and flexible way to match messages based on their content.

### What is JMESPath?

JMESPath is a query language for JSON that allows you to extract and transform elements from a JSON document. In Eventide, it's used to match messages to handlers based on their content.

### Examples of JMESPath Expressions

```python
from eventide import eventide_handler

# Match messages with a specific type
@eventide_handler("body.type == 'email'")

# Match messages with a specific attribute value
@eventide_handler("body.customer_id == '12345'")

# Match messages with a specific attribute in an array
@eventide_handler("contains(body.tags, 'urgent')")

# Match messages with a numeric comparison
@eventide_handler("body.priority > 5")

# Match messages with a specific structure
@eventide_handler("body.user.verified == true")

# Complex condition with multiple operators
@eventide_handler("body.type == 'order' && body.total > 100")
```

### Combining Multiple Expressions

You can combine multiple JMESPath expressions with logical operators:

```python
from eventide import eventide_handler

# Match messages that satisfy ALL conditions
@eventide_handler(
    "body.type == 'notification'",
    "body.priority == 'high'",
    operator="and"  # Default is "all" which is the same as "and"
)

# Match messages that satisfy ANY condition
@eventide_handler(
    "body.type == 'email'",
    "body.type == 'sms'",
    operator="or"  # Same as "any"
)
```

This approach gives you fine-grained control over which messages are routed to which handlers, allowing for clean separation of concerns in your application.

## Practical Example: Order Processing System

Here's a complete example of using Eventide to build an order processing system:

```python
# app.py
from eventide import Eventide, EventideConfig, SQSQueueConfig, eventide_handler

# Define handlers for different message types
@eventide_handler("body.type == 'new_order'")
def process_new_order(message):
    order = message.body.get('order', {})
    order_id = order.get('id')
    print(f"Processing new order: {order_id}")
    # Your order processing logic here
    return True

@eventide_handler("body.type == 'payment_confirmed'")
def process_payment(message):
    order_id = message.body.get('order_id')
    amount = message.body.get('amount')
    print(f"Payment of ${amount} confirmed for order: {order_id}")
    # Update order status, trigger shipping, etc.
    return True

@eventide_handler(
    "body.type == 'order_status_update'",
    "body.status == 'shipped'"
)
def handle_shipped_order(message):
    order_id = message.body.get('order_id')
    tracking_number = message.body.get('tracking_number')
    print(f"Order {order_id} shipped with tracking number: {tracking_number}")
    # Send confirmation email to customer, update database, etc.
    return True

# Configure and run Eventide
if __name__ == "__main__":
    config = EventideConfig(
        queues=[
            SQSQueueConfig(
                name="orders-queue",
                url="https://sqs.us-east-1.amazonaws.com/123456789012/orders-queue",
                region="us-east-1",
                # Increase visibility timeout for longer processing tasks
                visibility_timeout=120,
            ),
        ],
        # Use multiple workers for better throughput
        workers=4,
    )

    # Start processing messages
    print("Starting order processing system...")
    Eventide(config).run()
```

To run this application:

```bash
# Install dependencies
pip install eventide[sqs]

# Run the application
python app.py
```

This example demonstrates how to:
1. Define multiple handlers for different types of messages
2. Use JMESPath expressions to route messages to the appropriate handlers
3. Configure the application with the appropriate queue settings
4. Run multiple workers for better throughput

## Roadmap

- [ ] Retries and dead-letter queues
- [ ] Cleanup and resource management
- [ ] Health checks and monitoring
- [ ] Queue partitioning between workers
- [ ] Comprehensive test suite
- [ ] Task scheduling (cron and one-off)
- [ ] Lifecycle hooks

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.
