import multiprocessing
import sys
from queue import Empty

# define the number of cores (this is how many processes wil run)
num_cores = multiprocessing.cpu_count()


class Parallelize:
    def __init__(self):
        self.input_queue = multiprocessing.Queue()
        self.output_queue = multiprocessing.Queue()

        self.processes = [multiprocessing.Process(
                target=Parallelize._run,
                args=(self.input_queue, self.output_queue),
            ) for _ in range(multiprocessing.cpu_count())]

        for p in self.processes:
            p.start()

    def __del__(self):
        self.shutdown()

    def shutdown(self):
        for process in self.processes:
            process.terminate()

    @property
    def parallelization_enabled(self):
        return True

    @staticmethod
    def create(parallelize):
        return Parallelize() if parallelize else NullParallelize()

    def map(self, contents, processing_func):
        size = 0
        for content_idx, content in enumerate(contents):
            self.input_queue.put((content_idx, content, processing_func))
            size += 1
        results = []
        while size > 0:
            try:
                result = self.output_queue.get(block=False, timeout=0.1)
                results.append(result)
                size -= 1
            except Empty:
                if any(process.exitcode for process in self.processes):
                    print(
                        "error: Parallelizer: One of the child processes "
                        "has exited prematurely."
                    )
                    self.shutdown()
                    sys.exit(1)
        return map(lambda r: r[1], sorted(results, key=lambda r: r[0]))

    # This version doesn't handle the following cases properly:
    # - when a child process exists unexpectedly
    # - when a child process raises exception
    # def map_does_not_work(self, contents, processing_func):
    #     with concurrent.futures.ProcessPoolExecutor() as executor:
    #         return executor.map(processing_func, contents)

    @staticmethod
    def _run(input_queue, output_queue):
        while True:
            content_idx, content, processing_func = input_queue.get(block=True)
            result = processing_func(content)
            sys.stdout.flush()
            sys.stderr.flush()
            output_queue.put((content_idx, result))


class NullParallelize:
    @staticmethod
    def map(contents, processing_func):
        return [processing_func(content) for content in contents]

    def shutdown(self):
        ...

    @property
    def parallelization_enabled(self):
        return False
