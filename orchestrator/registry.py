"""Model adapter registry."""

from __future__ import annotations

from orchestrator.adapters.base import ModelAdapter
from orchestrator.adapters.docetl import DocETLAdapter
from orchestrator.adapters.evaporate import EvaporateAdapter


def build_registry() -> dict[str, ModelAdapter]:
    return {
        "docetl": DocETLAdapter(),
        "evaporate": EvaporateAdapter(),
    }
