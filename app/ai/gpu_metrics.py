# app/ai/gpu_metrics.py
import time

def get_gpu_stats():
    try:
        import torch
        if not torch.cuda.is_available():
            return None

        idx = torch.cuda.current_device()
        props = torch.cuda.get_device_properties(idx)

        return {
            "name": props.name,
            "total_vram_gb": round(props.total_memory / 1e9, 2),
            "allocated_gb": round(torch.cuda.memory_allocated(idx) / 1e9, 2),
            "reserved_gb": round(torch.cuda.memory_reserved(idx) / 1e9, 2),
        }
    except Exception:
        return None


def log_gpu_stats(prefix="GPU"):
    stats = get_gpu_stats()
    if not stats:
        print(f"[{prefix}] CPU mode")
        return

    print(
        f"[{prefix}] {stats['name']} | "
        f"VRAM {stats['allocated_gb']} / {stats['total_vram_gb']} GB "
        f"(reserved {stats['reserved_gb']} GB)"
    )
