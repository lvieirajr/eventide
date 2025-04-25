from multiprocessing import get_context
from multiprocessing.context import ForkContext
from signal import SIGINT, SIGTERM, signal
from sys import exit as sys_exit
from time import time
from types import FrameType
from typing import Callable, Optional

from .._handlers import Handler
from .._queues import Message
from .._utils.logging import eventide_logger
from .config import EventideConfig
from .handler import HandlerManager
from .hook import HookManager
from .queue import QueueManager
from .worker import WorkerManager


class Eventide:
    config: EventideConfig

    context: ForkContext

    handler_manager: HandlerManager
    hook_manager: HookManager
    queue_manager: QueueManager
    worker_manager: WorkerManager

    def __init__(self, config: EventideConfig) -> None:
        self.config = config

        self.context = get_context("fork")

        self.handler_manager = HandlerManager(config=self.config)
        self.hook_manager = HookManager()
        self.queue_manager = QueueManager(
            config=self.config,
            context=self.context,
            handler_manager=self.handler_manager,
        )
        self.worker_manager = WorkerManager(
            config=self.config,
            context=self.context,
            hook_manager=self.hook_manager,
            queue_manager=self.queue_manager,
        )

    @property
    def handler(self) -> Callable[..., Callable[..., Handler]]:
        return self.handler_manager.handler

    def on_start(self, hook: Callable[[], None]) -> Callable[[], None]:
        self.hook_manager.register_start_hook(hook)
        return hook

    def on_shutdown(self, hook: Callable[[], None]) -> Callable[[], None]:
        self.hook_manager.register_shutdown_hook(hook)
        return hook

    def on_message_received(
        self,
        hook: Callable[[Message], None],
    ) -> Callable[[Message], None]:
        self.hook_manager.register_message_received_hook(hook)
        return hook

    def on_message_success(
        self,
        hook: Callable[[Message], None],
    ) -> Callable[[Message], None]:
        self.hook_manager.register_message_success_hook(hook)
        return hook

    def on_message_failure(
        self,
        hook: Callable[[Message, Exception], None],
    ) -> Callable[[Message, Exception], None]:
        self.hook_manager.register_message_failure_hook(hook)
        return hook

    def run(self) -> None:
        eventide_logger.info("Starting Eventide...")

        self.setup_signal_handlers()

        self.hook_manager.on_start()

        self.queue_manager.start()
        self.worker_manager.start()

        while not self.worker_manager.is_shutdown:
            self.queue_manager.enqueue_retries()
            self.queue_manager.enqueue_messages()

            interval_start = time()
            while (
                time() - interval_start < self.queue_manager.pull_interval
                and not self.worker_manager.is_shutdown
            ):
                self.worker_manager.monitor_workers()

        eventide_logger.info("Stopping Eventide...")

        self.shutdown(force=False)

    def setup_signal_handlers(self) -> None:
        def handle_signal(_signum: int, _frame: Optional[FrameType]) -> None:
            if not self.worker_manager.is_shutdown:
                eventide_logger.info("Shutting down gracefully...")
                self.worker_manager.send_shutdown_event()
            else:
                eventide_logger.info("Forcing immediate shutdown...")
                self.shutdown(force=True)
                sys_exit(1)

        signal(SIGINT, handle_signal)
        signal(SIGTERM, handle_signal)

    def shutdown(self, force: bool = False) -> None:
        self.worker_manager.shutdown(force=force)
        self.queue_manager.shutdown()

        self.hook_manager.on_shutdown()
