"""Model adapter registry."""

from __future__ import annotations

from orchestrator.adapters.base import ModelAdapter
from orchestrator.adapters.docetl import DocETLAdapter
from orchestrator.adapters.evaporate import EvaporateAdapter
from orchestrator.adapters.quest import QuestAdapter
from orchestrator.adapters.lotus import LotusAdapter
from orchestrator.adapters.DQL import DQLAdapter


def build_registry() -> dict[str, ModelAdapter]:
    return {
        "docetl": DocETLAdapter(),
        "evaporate": EvaporateAdapter(),
        "quest": QuestAdapter(),
        "lotus": LotusAdapter(),
        "dql": DQLAdapter()
    }
