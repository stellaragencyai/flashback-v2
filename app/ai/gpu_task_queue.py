# app/ai/gpu_task_queue.py

import multiprocessing as mp

class GPUTaskQueue:
    def __init__(self):
        self.ctx = mp.get_context("spawn")
        self.queue = self.ctx.Queue()
        self.worker = self.ctx.Process(
            target=self._gpu_worker,
            args=(self.queue,),
            daemon=True
        )
        self.worker.start()

    def submit(self, fn, *args, **kwargs):
        self.queue.put((fn, args, kwargs))

    @staticmethod
    def _gpu_worker(queue):
        import torch
        from app.ai.gpu_runtime import configure_gpu
        from app.ai.gpu_warmup import warmup_gpu

        configure_gpu()
        warmup_gpu()

        while True:
            fn, args, kwargs = queue.get()
            try:
                fn(*args, **kwargs)
            except Exception as e:
                print("[GPU WORKER ERROR]", e)
