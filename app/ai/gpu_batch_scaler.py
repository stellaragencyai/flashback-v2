# app/ai/gpu_batch_scaler.py

def compute_batch_size(
    base_batch: int,
    min_batch: int = 4,
    max_batch: int = 512,
    safety_ratio: float = 0.70
):
    """
    Dynamically scale batch size based on available GPU VRAM.
    CPU fallback returns base_batch.
    """
    try:
        import torch
        if not torch.cuda.is_available():
            return base_batch

        idx = torch.cuda.current_device()
        props = torch.cuda.get_device_properties(idx)

        total_gb = props.total_memory / 1e9
        usable_gb = total_gb * safety_ratio

        # Reference: 8 GB GPU = baseline
        scale = usable_gb / 8.0
        scaled = int(base_batch * scale)

        return max(min_batch, min(scaled, max_batch))

    except Exception:
        return base_batch
