# app/ai/torch_compile.py

def maybe_compile(model, mode="default"):
    """
    Safely applies torch.compile if supported.
    Falls back silently if unavailable.
    """
    try:
        import torch

        if not hasattr(torch, "compile"):
            return model

        return torch.compile(
            model,
            mode=mode,
            fullgraph=False
        )

    except Exception:
        return model
