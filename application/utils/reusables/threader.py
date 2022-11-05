import inspect
import threading
from threading import Semaphore, BoundedSemaphore
import os
import ctypes
from typing import Any


threadList: dict = {}
MAX_THREAD = os.getenv('MAX_THREAD', 10)

"""
:research:
    - https://www.bogotobogo.com/python/Multithread/python_multithreading_Event_Objects_between_Threads.php
    - https://stackoverflow.com/questions/39669463/python-multithreading-complex-objects
"""


"""
Return a list of all Thread objects currently active. The list includes 
daemonic threads and dummy thread objects created by current_thread(). 
It excludes terminated threads and threads that have not yet been started. 
However, the main thread is always part of the result, even when terminated
"""
threading.enumerate()


class LearnSemaphore:
    # Usually, you create a Semaphore that will allow a certain number of threads
    # into a section of code. This one starts at 5.
    s1 = Semaphore(5)

    # When you want to enter the section of code, you acquire it first.
    # That lowers it to 4. (Four more threads could enter this section.)
    s1.acquire()

    # Then you do whatever sensitive thing needed to be restricted to five threads.

    # When you're finished, you release the semaphore, and it goes back to 5.
    s1.release()

    # That's all fine, but you can also release it without acquiring it first.
    s1.release()

    # The counter is now 6! That might make sense in some situations, but not in most.
    print(s1._value)  # => 6

    # If that doesn't make sense in your situation, use a BoundedSemaphore.

    s2 = BoundedSemaphore(5)  # Start at 5.

    s2.acquire()  # Lower to 4.

    s2.release()  # Go back to 5.

    try:
        s2.release()  # Try to raise to 6, above starting value.
    except ValueError:
        print('As expected, it complained.')


def _async_raise(tid, exc_type):
    """Raises an exception in the threads with id tid"""
    if not inspect.isclass(exc_type):
        raise TypeError("Only types can be raised (not instances)")
    res = ctypes.pythonapi.PyThreadState_SetAsyncExc(ctypes.c_long(tid), ctypes.py_object(exc_type))
    if res == 0:
        raise ValueError("invalid thread id")
    elif res != 1:
        """
        if it returns a number greater than one, you're in trouble,
        and you should call it again with exc=NULL to revert the effect
            >> ctypes.pythonapi.PyThreadState_SetAsyncExc(ctypes.c_long(tid), 0)
        """
        ctypes.pythonapi.PyThreadState_SetAsyncExc(ctypes.c_long(tid), None)
        raise SystemError("PyThreadState_SetAsyncExc failed")


class ThreadWithControl(threading.Thread):
    """
    We want to create threading class that can control maximum background
    agent and result after complete
    - Get return output from threading function
    - A thread class that supports raising an exception in the thread from
    another thread.
    usage:
        >> _thread = ThreadWithControl(target=lambda a: a * 2, args=(2, ))
        >> _thread.daemon = True
        >> _thread.start()
        >> print(_thread.join())
        4
    """
    threadLimiter = threading.BoundedSemaphore(MAX_THREAD)

    def __init__(self, *args, **kwargs):
        self._return = None
        self._target = None
        self._args = None
        self._kwargs = None
        super(ThreadWithControl, self).__init__(*args, **kwargs)
        self._stop_event = threading.Event()
        self.check_count = 0

    def run(self):
        self.threadLimiter.acquire()
        try:
            if self._target:
                self._return = self._target(*self._args, **self._kwargs)
        finally:
            del self._target, self._args, self._kwargs
            self.threadLimiter.release()

    def join(self, *args) -> Any:
        super().join(*args)
        return self._return

    def stop(self):
        self._stop_event.set()

    def stopped(self):
        return self._stop_event.is_set()

    def _get_my_tid(self):
        """
        determines this (self's) thread id

        CAREFUL: this function is executed in the context of the caller
        thread, to get the identity of the thread represented by this
        instance.
        """
        if not self.is_alive():
            raise threading.ThreadError("the thread is not active")

        # do we have it cached?
        if hasattr(self, "_thread_id"):
            return self._thread_id

        # no, look for it in the _active dict
        # for thread_id, thread_obj in threading._active.items():
        for thread_id, thread_obj in getattr(threading, '_active').items():
            if thread_obj is self:
                self._thread_id = thread_id
                return thread_id

        raise AssertionError("could not determine the thread's id")

    def raise_exc(self, exc_type):
        """
        Raises the given exception type in the context of this thread.

        If the thread is busy in a system call (time.sleep(),
        socket.accept(), ...), the exception is simply ignored.

        If you are sure that your exception should terminate the thread,
        one way to ensure that it works is:

            t = ThreadWithExc(...)
            ...
            t.raise_exc(SomeException)
            while t.isAlive():
                time.sleep(0.1)
                t.raise_exc(SomeException)

        If the exception is to be caught by the thread, you need a way to
        check that your thread has caught it.

        CAREFUL: this function is executed in the context of the
        caller thread, to raise an exception in the context of the
        thread represented by this instance.
        """
        _async_raise(self._get_my_tid(), exc_type)

    def terminate(self):
        """
        must raise the SystemExit type, instead of a SystemExit() instance
        due to a bug in PyThreadState_SetAsyncExc
        """
        self.raise_exc(SystemExit)
