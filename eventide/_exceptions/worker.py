from .eventide import EventideError


class WorkerError(EventideError):
    pass


class WorkerTimeoutError(WorkerError, TimeoutError):
    pass


class WorkerCrashedError(WorkerError, ChildProcessError):
    pass
