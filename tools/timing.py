import time


class TimerError(Exception):
    """A custom exception used to report errors in use of Timer class
    src: https://realpython.com/python-timer/
    """


class Timer:
    def __init__(self):
        self._start_time = None
        self.elapsed_time = None

    def start(self):
        """Start a new timer"""
        if self._start_time is not None:
            raise TimerError(f"Timer is running. Use .stop() to stop it")

        self._start_time = time.perf_counter()

    def stop(self):
        """Stop the timer, and report the elapsed time"""
        if self._start_time is None:
            raise TimerError(f"Timer is not running. Use .start() to start it")

        self.elapsed_time = time.perf_counter() - self._start_time
        self._start_time = None

    def __enter__(self):
        """Start a new timer as a context manager"""
        self.start()
        return self

    def __exit__(self, *exc_info):
        """Stop the context manager timer"""
        self.stop()

    @staticmethod
    def current_time():
        return time.strftime("%H:%M:%S", time.localtime())
