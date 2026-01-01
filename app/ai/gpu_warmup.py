# app/ai/gpu_warmup.py

def warmup_gpu(
    tensor_mb: int = 128,
    dtype="float16"
):
    """
    Pre-allocates GPU memory and initializes CUDA context.
    Safe no-op on CPU.
    """
    try:
        import torch

        if not torch.cuda.is_available():
            return False

        device = torch.device("cuda")

        bytes_per_elem = 2 if dtype == "float16" else 4
        num_elems = (tensor_mb * 1024 * 1024) // bytes_per_elem

        dt = torch.float16 if dtype == "float16" else torch.float32

        # Allocate tensor
        x = torch.empty(num_elems, device=device, dtype=dt)

        # Dummy compute (forces kernel launch)
        x = x * 1.0001
        torch.cuda.synchronize()

        del x
        return True

    except Exception:
        return False
