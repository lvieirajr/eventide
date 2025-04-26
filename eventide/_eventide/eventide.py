from importlib import import_module
from multiprocessing import get_context
from multiprocessing.context import ForkContext
from multiprocessing.synchronize import Event as MultiprocessingEvent
from pathlib import Path
from signal import SIGINT, SIGTERM, signal
from sys import exit as sys_exit
from sys import path
from time import sleep, time
from types import FrameType
from typing import Any, Callable, Optional

from .._handlers import Handler
from .._queues import Message
from .._utils.logging import eventide_logger
from .config import EventideConfig
from .cron import CronManager
from .handler import HandlerManager
from .hook import HookManager
from .queue import QueueManager
from .worker import WorkerManager


class Eventide:
    config: EventideConfig

    context: ForkContext
    shutdown_event: MultiprocessingEvent

    handler_manager: HandlerManager
    hook_manager: HookManager
    queue_manager: QueueManager
    worker_manager: WorkerManager

    def __init__(self, config: EventideConfig) -> None:
        self.config = config

        self.context = get_context("fork")
        self.shutdown_event = self.context.Event()

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
            shutdown_event=self.shutdown_event,
            hook_manager=self.hook_manager,
            queue_manager=self.queue_manager,
        )
        self.cron_manager = CronManager(queue_manager=self.queue_manager)

        self.autodiscover()

    @property
    def handler(self) -> Callable[..., Callable[..., Handler]]:
        return self.handler_manager.register

    @property
    def cron(self) -> Callable[[str], Callable[[Callable[[], Any]], Callable[[], Any]]]:
        return self.cron_manager.register

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

        self.shutdown_event.clear()
        self.setup_signal_handlers()
        self.autodiscover()
        self.hook_manager.on_start()
        self.queue_manager.start()
        self.worker_manager.start()

        while not self.shutdown_event.is_set():
            self.queue_manager.enqueue_retries()
            self.queue_manager.enqueue_messages()

            interval_start = time()
            while (
                time() - interval_start < self.queue_manager.pull_interval
                and not self.worker_manager.shutdown_event.is_set()
            ):
                self.worker_manager.monitor_workers()
                sleep(0.1)

        eventide_logger.info("Stopping Eventide...")

        self.shutdown(force=False)

    def run_cron(self) -> None:
        eventide_logger.info("Starting Eventide cron...")

        self.shutdown_event.clear()
        self.setup_signal_handlers()
        self.autodiscover()
        self.queue_manager.start()

        while not self.shutdown_event.is_set():
            self.cron_manager.evaluate_crons()
            sleep(1.0)

        self.shutdown(force=False)

    def setup_signal_handlers(self) -> None:
        def handle_signal(_signum: int, _frame: Optional[FrameType]) -> None:
            if not self.worker_manager.shutdown_event.is_set():
                eventide_logger.info("Shutting down gracefully...")
                self.shutdown_event.set()
            else:
                eventide_logger.info("Forcing immediate shutdown...")
                self.shutdown(force=True)
                sys_exit(1)

        signal(SIGINT, handle_signal)
        signal(SIGTERM, handle_signal)

    def autodiscover(self) -> None:
        cwd = Path(".").resolve()
        ignored_dirs = {".git", ".venv", "venv", "__pycache__"}

        if str(cwd) not in path:
            path.insert(0, str(cwd))

        for python_file in cwd.glob("*.py"):
            try:
                import_module(python_file.stem)
            except (ImportError, ModuleNotFoundError, TypeError):
                pass

        def import_from_directory(directory: Path) -> None:
            if not (directory / "__init__.py").exists():
                return

            for item in directory.iterdir():
                if item.is_dir() and item.name not in ignored_dirs:
                    import_from_directory(item)
                elif item.suffix == ".py":
                    module_name = ".".join(item.relative_to(cwd).with_suffix("").parts)

                    try:
                        import_module(module_name)
                    except (ImportError, ModuleNotFoundError, TypeError):
                        pass

        for child in cwd.iterdir():
            if child.is_dir() and child.name not in ignored_dirs:
                import_from_directory(child)

    def shutdown(self, force: bool = False) -> None:
        self.shutdown_event.set()

        self.worker_manager.shutdown(force=force)
        self.queue_manager.shutdown()

        self.hook_manager.on_shutdown()
