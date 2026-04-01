from __future__ import annotations

from dataclasses import dataclass, asdict
from typing import List, Dict, Any
import json


@dataclass
class LLMCallMetrics:
    model: str
    prompt_tokens: int = 0
    completion_tokens: int = 0
    total_tokens: int = 0
    latency: float = 0.0
    call_id: str = ""


class GlobalLLMTracker:
    def __init__(self) -> None:
        self.calls: List[LLMCallMetrics] = []
        self.total_calls: int = 0

    def reset(self) -> None:
        self.calls = []
        self.total_calls = 0

    def add_call(self, metrics: LLMCallMetrics) -> None:
        self.calls.append(metrics)
        self.total_calls += 1

    def get_summary(self) -> Dict[str, Any]:
        total_prompt = sum(c.prompt_tokens for c in self.calls)
        total_completion = sum(c.completion_tokens for c in self.calls)
        total_tokens = sum(c.total_tokens for c in self.calls)
        total_latency = sum(c.latency for c in self.calls)

        # Costo non disponibile con Gemini qui: lo lasciamo a 0
        return {
            "total_calls": self.total_calls,
            "total_tokens": {
                "prompt": total_prompt,
                "completion": total_completion,
                "total": total_tokens,
            },
            "total_cost": {
                "prompt": 0.0,
                "completion": 0.0,
                "total": 0.0,
            },
            "average_latency": (total_latency / self.total_calls) if self.total_calls else 0.0,
        }

    def print_summary(self) -> None:
        summary = self.get_summary()
        print("\n=== LLM Call Summary ===")
        print(f"Total calls: {summary['total_calls']}")
        print(f"Prompt tokens: {summary['total_tokens']['prompt']:,}")
        print(f"Completion tokens: {summary['total_tokens']['completion']:,}")
        print(f"Total tokens: {summary['total_tokens']['total']:,}")
        print(f"Average latency: {summary['average_latency']:.3f}s")
        print(f"Estimated total cost: ${summary['total_cost']['total']:.4f}")

    def save_to_file(self, path: str) -> None:
        payload = {
            "summary": self.get_summary(),
            "calls": [asdict(c) for c in self.calls],
        }
        with open(path, "w", encoding="utf-8") as f:
            json.dump(payload, f, ensure_ascii=False, indent=2)


_GLOBAL_TRACKER = GlobalLLMTracker()


def get_global_tracker() -> GlobalLLMTracker:
    return _GLOBAL_TRACKER


def reset_global_tracker() -> None:
    _GLOBAL_TRACKER.reset()