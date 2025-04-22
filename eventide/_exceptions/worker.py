from .eventide import EventideError


class WorkerError(EventideError):
    pass


class WorkerCrashedError(WorkerError, ChildProcessError):
    pass
