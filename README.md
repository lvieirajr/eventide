# Eventide
[![PyPI version](https://img.shields.io/pypi/v/eventide?style=flat-square)](https://pypi.org/project/eventide)
[![Python Versions](https://img.shields.io/badge/python-3.9%20%7C%203.10%20%7C%203.11%20%7C%203.12%20%7C%203.13-blue)](https://pypi.org/project/eventide)
[![CI](https://github.com/lvieirajr/eventide/workflows/CI/badge.svg)](https://github.com/lvieirajr/eventide/actions/workflows/CI.yaml)
[![License](https://img.shields.io/badge/License-Apache%202.0-blue.svg)](https://opensource.org/licenses/Apache-2.0)

Streamlined, no-fuss queue workers for Python.

---

## Overview

Eventide is a streamlined, easy-to-setup framework for building queue workers.
It lets you consume messages, schedule messages with cron, handle retries, and manage your worker lifecycle simply and reliably.


---

## Installation

```bash
# Base installation
pip install eventide

# Choose a queue provider:
pip install eventide[sqs]        # AWS SQS support
pip install eventide[cloudflare] # Cloudflare Queues support

# Optional features:
pip install eventide[cron]       # Cron scheduling
pip install eventide[watch]      # Dev autoreload
pip install eventide[core]       # Installs all optional features (not queue providers)
```

---

## Quick Start

A minimal Eventide app and handler:

```python
from eventide import Eventide, EventideConfig, Message, SQSQueueConfig

app = Eventide(
    config=EventideConfig(
        queue=SQSQueueConfig(
            region="us-east-1",
            url="https://sqs.us-east-1.amazonaws.com/123456789012/my-queue",
            buffer_size=10,  # internal buffer queue
        ),
    )
)

@app.handler("body.event == 'hello'")
def hello_world(message: Message) -> None:
    print("Hello ", message.body.get("content", "World"))
```

Run:

```bash
eventide run -a app:app

# or using autoreload (recommended for development)
eventide run -a app:app --reload
```

---

## Handlers

Handlers process messages matching [**JMESPath expressions**](https://jmespath.org/) and can define retry behavior.

Example:

```python
@app.handler(
    "body.type == 'email' && body.event == 'send'",
    retry_limit=5,
    retry_for=[ValueError, TimeoutError],
    retry_min_backoff=1.0,
    retry_max_backoff=30.0,
)
def send_email(message):
    print("Sending email:", message.body)
```

---

## Scheduled Messages - Cron

You can define functions decorated with `@app.cron` to schedule the sending of messages.
Each [**cron**](https://crontab.guru/) function generates and returns the message body that Eventide will automatically enqueue to your configured queue provider at the scheduled time.

Example:

```python
@app.cron("*/5 * * * * *")  # every 5 seconds
def send_heartbeat():
    return {"type": "heartbeat"}
```

Run the cron process separately from the worker process:

```bash
eventide cron -a app:app

# or using autoreload (recommended for development)
eventide cron -a app:app --reload
```

Workers and the cron scheduler are independent, allowing you to scale your workers without duplicating scheduled messages.

---

## Lifecycle Hooks

Hooks allow you to react to important lifecycle moments:

```python
@app.on_start
def startup():
    print("App is starting")

@app.on_shutdown
def shutdown():
    print("App is shutting down")

@app.on_message_failure
def on_failure(message, exc):
    print(f"Failed handling {message.id}: {exc}")
```

Available hooks:
- `on_start`
- `on_shutdown`
- `on_message_received`
- `on_message_success`
- `on_message_failure`

---

## Queue Providers

Eventide currently has built-in support for:

- AWS SQS
- Cloudflare Queues

More providers will be added over time.
You can also implement custom providers by extending the `Queue` base class.

---

## License

[Apache 2.0](LICENSE)
