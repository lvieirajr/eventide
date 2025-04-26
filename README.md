# Eventide
[![PyPI version](https://img.shields.io/pypi/v/eventide?style=flat-square)](https://pypi.org/project/eventide)
[![Python Versions](https://img.shields.io/badge/python-3.9%20%7C%203.10%20%7C%203.11%20%7C%203.12%20%7C%203.13-blue)](https://pypi.org/project/eventide/)
[![CI](https://github.com/lvieirajr/eventide/workflows/CI/badge.svg)](https://github.com/lvieirajr/eventide/actions/workflows/CI.yaml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

A fast, simple, and extensible queue worker framework for Python.

## Overview

Eventide is a modern, lightweight framework for building robust queue-based worker systems in Python. It provides a clean, modular, and provider-agnostic architecture for consuming and processing messages from a variety of queue backends (SQS, Cloudflare, and more to come).

**Key Features:**
- Multiprocess architecture for high throughput and resilience
- Provider-agnostic queue abstraction with built-in and custom queue support
- Declarative, decorator-based message handler registration
- Robust retry/backoff logic
- Graceful startup and shutdown with signal handling
- Type-safe configuration using Pydantic models
- Extensible handler matching and routing

## Architecture

**Eventide** orchestrates the lifecycle of your queue worker system:
- **Main Process:** Manages configuration, queue instantiation, worker process lifecycle, and graceful shutdown.
- **Queue:** Continuously pull messages from external queues (SQS, Cloudflare, etc.) into internal buffers.
- **Worker Processes:** Each worker consumes messages from the buffer and routes them to user-defined handlers that run within the Worker's process.
- **Handlers:** User functions decorated with `@app.handler` that process messages matching specific patterns.

All configuration is done via Pydantic models, ensuring type safety and validation.

## Installation

```bash
pip install eventide

# With SQS support:
pip install eventide[sqs]

# With Cloudflare Queues support:
pip install eventide[cloudflare]

# With autoreload support (for development):
pip install eventide[watch]
```

## Quick Start

```python
from eventide import Eventide, EventideConfig, Message, SQSQueueConfig

# Instantiate the eventide app
app = Eventide(
    config=EventideConfig(
        queue=SQSQueueConfig(
            region="us-east-1",
            url="https://sqs.us-east-1.amazonaws.com/123456789012/my-queue",
        ),
    ),
)

# Define a handler
@app.handler("body.type == 'greeting'")
def handle_greeting(message: Message) -> None:
    print(f"Received greeting: {message.body.get('content')}")
```

## Configuration

All configuration is via Pydantic models:
- `EventideConfig`: Main config (queue, concurrency, handler paths, timeouts, retry policies, etc)
- `SQSQueueConfig`, `CloudflareQueueConfig`, ...: Provider-specific queue configs

Example:
```python
from eventide import EventideConfig, SQSQueueConfig

config = EventideConfig(
    queue=SQSQueueConfig(
        region="us-east-1",
        url="https://sqs.us-east-1.amazonaws.com/123456789012/my-queue",
    ),
    concurrency=4,
    timeout=30.0,  # Handler timeout (seconds)
    retry_for=[ValueError],  # Retry for specific exceptions
    retry_limit=5,  # Max retries per message
    retry_min_backoff=1.0,  # Min backoff in seconds
    retry_max_backoff=60.0, # Max backoff in seconds
)
```

## Message Handlers

Handlers are registered using the `@app.handler` decorator.
You can match on message attributes, set retry/backoff policies, and more.

```python
from eventide import Eventide, EventideConfig

app = Eventide(EventideConfig(...))

@app.handler("body.type == 'email'", retry_limit=3, retry_for=[ValueError])
def process_email(message):
    print(f"Processing email: {message.body}")
```

Advanced matching (multiple matchers, logical operators):
```python
from eventide import Eventide, EventideConfig

app = Eventide(EventideConfig(...))

@app.handler(
    "body.type == 'notification'",
    "body.priority == 'high'",
    operator=all,  # or any
)
def process_notification(_message):
    ...
```

## Advanced Usage
- **Graceful Shutdown:** Eventide handles SIGINT/SIGTERM for clean shutdown.
- **Retries & Backoff:** Handlers can specify retry policies and backoff intervals.
- **Extensible Matching:** Handler matcher logic can be customized for advanced routing.


#### SQS Queue

```python
from eventide import SQSQueueConfig

sqs_config = SQSQueueConfig(
    region="us-east-1",
    url="https://sqs.us-east-1.amazonaws.com/123456789012/my-queue",
    visibility_timeout=30,  # seconds
    max_number_of_messages=10,  # max messages to fetch
    buffer_size=100,  # internal buffer size
)
```

#### Cloudflare Queue

```python
from eventide import CloudflareQueueConfig

cf_config = CloudflareQueueConfig(
    account_id="my-account-id",
    queue_id="my-queue-id",
    batch_size=10,  # max messages to fetch
    visibility_timeout_ms=30000,  # milliseconds
    buffer_size=100,  # internal buffer size
)
```

## Message Routing with JMESPath

Eventide uses JMESPath expressions to route messages to the appropriate handlers. This provides a powerful and flexible way to match messages based on their content.

### What is JMESPath?

JMESPath is a query language for JSON that allows you to extract and transform elements from a JSON document. In Eventide, it's used to match messages to handlers based on their content.

### Examples of JMESPath Expressions

```
# Match messages with a specific type
"body.type == 'email'"

# Match messages with a specific attribute value
"body.customer_id == '12345'"

# Match messages with a specific attribute in an array
"contains(body.tags, 'urgent')"

# Match messages with a numeric comparison
"body.priority > 5"

# Match messages with a specific structure
"body.user.verified == true"

# Complex condition with multiple operators
"body.type == 'order' && body.total > 100"
```

### Combining Multiple Expressions

You can combine multiple JMESPath expressions with logical operators:

```python
from eventide import Eventide, EventideConfig, Message

app = Eventide(EventideConfig(...))

# Match messages that satisfy ALL conditions
@app.handler(
    "body.type == 'notification'",
    "body.priority == 'high'",
    operator=all
)
def priority_notifications_handler(message: Message):
  pass

# Match messages that satisfy ANY condition
@app.handler(
    "body.type == 'email'",
    "body.type == 'sms'",
    operator=any
)
def email_or_sms_handler(message: Message):
  pass

```

This approach gives you fine-grained control over which messages are routed to which handlers, allowing for clean separation of concerns in your application.


## Lifecycle Hooks

Eventide supports user-defined lifecycle hooks to let you tap into key moments during your app’s runtime. These hooks are ideal for tasks like logging, resource initialization, metrics, or graceful shutdown logic.

You can register hooks using decorators on the `app` instance:

### Available Hooks

| Hook | Description |
|------|-------------|
| `@app.on_start` | Runs once before the queue and workers start |
| `@app.on_shutdown` | Runs once when the app is shutting down |
| `@app.on_handle_message` | Runs before a message is handled |
| `@app.on_handle_success` | Runs after a message is successfully handled |
| `@app.on_handle_failure` | Runs when a message handler raises an exception |

### Example

```python
from eventide import Eventide, EventideConfig, Message

app = Eventide(EventideConfig(...))

@app.on_start
def boot_log():
    print("Eventide is starting up...")

@app.on_shutdown
def shutdown_log():
    print("Eventide is shutting down...")

@app.on_handle_message
def before_handle(msg: Message):
    print(f"About to handle: {msg.id}")

@app.on_handle_success
def after_success(msg: Message):
    print(f"Successfully handled: {msg.id}")

@app.on_handle_failure
def on_error(msg: Message, exc: Exception):
    print(f"Error handling {msg.id}: {exc}")
```


## Scheduled Messages with Cron

Eventide supports lightweight message scheduling based on **cron expressions**, allowing you to inject messages into your queue at precise times, without needing external systems.

Cron jobs are **defined using decorators** directly on the `app` instance.

### Example

```python
from eventide import Eventide, EventideConfig

app = Eventide(EventideConfig(...))

@app.cron("* * * * * *")
def heartbeat() -> dict[str, Any]:
    return {"type": "heartbeat", "timestamp": "now"}
```

This will send a `heartbeat` message **every second**.

---

### How Cron Works

- Cron jobs are scheduled independently of workers.
- Each registered cron function generates a **single message body**.
- The body is automatically pushed into the queue.
- The cron process is meant to run **separately** from the worker process.

This separation ensures that you can horizontally scale workers without duplicating scheduled messages.

---

### Cron Expression Format

Eventide supports **6-field cron expressions** (`seconds minutes hours day month weekday`).

| Field | Allowed Values |
|:------|:---------------|
| Seconds | 0–59 |
| Minutes | 0–59 |
| Hours | 0–23 |
| Day of month | 1–31 |
| Month | 1–12 |
| Day of week | 0–6 (Sunday=0) |

Examples:
- `"* * * * * *"` — Every second
- `"0 * * * * *"` — Every minute, at second 0
- `"0 0 * * * *"` — Every day at midnight
- `"*/5 * * * * *"` — Every 5 seconds

---

### Running the Cron Process

You should run your cron scheduler **separately** from your workers:

```bash
eventide cron -a app:app
```

This will start the cron manager, which will evaluate and send messages based on your defined schedules.

Workers can continue running normally:

```bash
eventide run -a app:app
```

---

👉 Cron messages are treated exactly like any other messages once they are enqueued.
👉 You can scale your workers independently from your scheduled jobs.
👉 Cron functions are simple Python functions returning message bodies.

---


## Practical Example: Order Processing System

Here's a complete example of using Eventide to build an order processing system:

```python
# app.py
from eventide import Eventide, EventideConfig, Message, SQSQueueConfig

app = Eventide(
    config=EventideConfig(
        queue=SQSQueueConfig(
            region="us-east-1",
            url="https://sqs.us-east-1.amazonaws.com/123456789012/orders-queue",
            # Increase visibility timeout for longer processing tasks
            visibility_timeout=120,
        ),
        # Use multiple workers for better throughput
        concurrency=4,
    ),
)

# Define handlers for different message types
@app.handler("body.type == 'new_order'")
def process_new_order(message: Message):
    order = message.body.get('order', {})
    order_id = order.get('id')
    print(f"Processing new order: {order_id}")
    # Your order processing logic here
    return True

@app.handler("body.type == 'payment_confirmed'")
def process_payment(message: Message):
    order_id = message.body.get('order_id')
    amount = message.body.get('amount')
    print(f"Payment of ${amount} confirmed for order: {order_id}")
    # Update order status, trigger shipping, etc.
    return True

@app.handler(
    "body.type == 'order_status_update'",
    "body.status == 'shipped'"
)
def handle_shipped_order(message):
    order_id = message.body.get('order_id')
    tracking_number = message.body.get('tracking_number')
    print(f"Order {order_id} shipped with tracking number: {tracking_number}")
    # Send confirmation email to customer, update database, etc.
    return True
```

To run this application:

```bash
# Install dependencies
pip install eventide[sqs,watch]

# Run the application
eventide run -a app:app

# Run the application with autoreload (for development)
eventide run -a app:app --reload
```

This example demonstrates how to:
1. Define multiple handlers for different types of messages
2. Use JMESPath expressions to route messages to the appropriate handlers
3. Configure the application with the appropriate queue settings
4. Run multiple workers for better throughput

## License

This project is licensed under the Apache License 2.0 - see the [LICENSE](LICENSE) file for details.
