import json
import os
import yaml
import pytest
from jsonschema import validate, ValidationError

from pathlib import Path

# ------- Path setup -------

ROOT = Path(__file__).parent.parent
MASTER_YAML = ROOT / "config" / "master_strategies.yaml"
SCHEMA_JSON = ROOT / "config" / "master_strategies.schema.json"

# ------- Load Helpers -------

def load_yaml(p: Path):
    with p.open("r", encoding="utf-8") as f:
        return yaml.safe_load(f)

def load_json(p: Path):
    with p.open("r", encoding="utf-8") as f:
        return json.load(f)

# ------- Tests -------

def test_master_strategies_yaml_exists():
    assert MASTER_YAML.exists(), f"{MASTER_YAML} not found"

def test_master_strategies_schema_exists():
    assert SCHEMA_JSON.exists(), f"{SCHEMA_JSON} not found"

def test_validate_master_strategies_against_schema():
    """
    Ensures master_strategies.yaml matches the defined JSON schema.
    """
    data = load_yaml(MASTER_YAML)
    schema = load_json(SCHEMA_JSON)

    try:
        validate(instance=data, schema=schema)
    except ValidationError as e:
        pytest.fail(f"Schema validation failed: {e.message}")

def test_strategy_ids_unique():
    """
    Ensures strategy IDs are unique.
    """
    data = load_yaml(MASTER_YAML)
    ids = [s["id"] for s in data.get("strategies", [])]
    assert len(ids) == len(set(ids)), "Duplicate strategy IDs found"

def test_subaccounts_unique_keys():
    """
    Ensures each subaccount key in 'subaccounts' is unique.
    """
    data = load_yaml(MASTER_YAML)
    sub_keys = list(data.get("subaccounts", {}).keys())
    assert len(sub_keys) == len(set(sub_keys)), "Duplicate subaccount keys found"
