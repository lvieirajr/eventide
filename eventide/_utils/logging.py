from logging import getLogger

eventide_logger = getLogger(name="eventide")
cron_logger = getLogger(name="eventide.cron")
queue_logger = getLogger(name="eventide.queue")
worker_logger = getLogger(name="eventide.worker")
