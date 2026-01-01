import pytest
import subprocess
from pathlib import Path

VALIDATOR = Path("scripts/validate_config.py")

def test_config_validation():
    """Ensure that config validator exits cleanly."""
    result = subprocess.run(["python", str(VALIDATOR)], capture_output=True, text=True)
    assert result.returncode == 0, f"Config validation failed:\n{result.stdout}\n{result.stderr}"

def test_sample_strategy_keys():
    """Spot check a few required fields in strategy config."""
    import yaml
    strat = yaml.safe_load(Path("config/strategies.yaml").read_text())
    assert "subaccounts" in strat, "strategies.yaml missing `subaccounts` key"
    for s in strat["subaccounts"]:
        assert "strategy_name" in s, "Every strategy must have `strategy_name`"
