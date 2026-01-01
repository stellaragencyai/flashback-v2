# app/ai/gpu_runtime.py
# Canonical GPU runtime for Flashback

def configure_gpu():
    # ---- PyTorch ----
    try:
        import torch
        if torch.cuda.is_available():
            torch.backends.cudnn.benchmark = True
            return "cuda", torch.device("cuda")
    except Exception:
        pass

    # ---- TensorFlow ----
    try:
        import tensorflow as tf
        gpus = tf.config.list_physical_devices("GPU")
        if gpus:
            for gpu in gpus:
                tf.config.experimental.set_memory_growth(gpu, True)
            return "cuda", gpus
    except Exception:
        pass

    return "cpu", None
