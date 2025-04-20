class EventideError(Exception):
    pass


class WorkerError(EventideError):
    pass


class WorkerTimeoutError(WorkerError, TimeoutError):
    pass


class WorkerCrashedError(WorkerError, ChildProcessError):
    pass
