import builtins
import importlib
import sys
import pytest


def test_fan_uses_fallback_name_when_faker_missing(
    monkeypatch: pytest.MonkeyPatch,
):
    # Ensure a clean import of fan
    sys.modules.pop("fan", None)

    original_import = builtins.__import__

    def fake_import(name, *args, **kwargs):
        if name == "faker":
            raise ImportError("No faker available")
        return original_import(name, *args, **kwargs)

    # Cause ImportError specifically for 'faker' during fan import
    monkeypatch.setattr(builtins, "__import__", fake_import)

    fan = importlib.import_module("fan")
    Fan = fan.Fan

    f = Fan(42, verbose=False)
    # Fallback produces a non-empty string
    assert isinstance(f.name(), str)
    assert len(f.name()) > 0
