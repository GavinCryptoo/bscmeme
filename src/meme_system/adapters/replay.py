"""Deterministic normalized replay runner; no network access."""

from __future__ import annotations

from dataclasses import dataclass
from datetime import datetime
from decimal import Decimal
from typing import Mapping, Sequence

from meme_system.adapters.protocols import ExecutableQuote
from meme_system.domain.models import (
    EntryFeatures,
    ShadowExitFeatures,
    Signal,
)
from meme_system.engines.simulation import (
    DeterministicSimulation,
    EntryResult,
    ExitResult,
)


@dataclass(frozen=True)
class ReplayExit:
    at: datetime
    sell_quote: ExecutableQuote | None
    shadow_features: ShadowExitFeatures | None = None
    returns_after_exit_pct: Mapping[int, Decimal | None] | None = None
    avoided_loss_pct: Decimal | None = None
    missed_profit_pct: Decimal | None = None


@dataclass(frozen=True)
class ReplayStep:
    step_id: str
    signal: Signal
    entry_features: EntryFeatures
    exits: Sequence[ReplayExit] = ()


@dataclass(frozen=True)
class ReplayResult:
    entries: tuple[EntryResult, ...]
    exits: tuple[ExitResult, ...]


class ReplayRunner:
    def __init__(self, engine: DeterministicSimulation) -> None:
        self.engine = engine

    def run(self, steps: Sequence[ReplayStep]) -> ReplayResult:
        entries: list[EntryResult] = []
        exits: list[ExitResult] = []
        for step in steps:
            entry = self.engine.process_entry(
                step.signal,
                step.entry_features,
                candidate_id=f"{step.step_id}:candidate",
                position_id=f"{step.step_id}:position",
            )
            entries.append(entry)
            if entry.position is None:
                continue
            for replay_exit in step.exits:
                if self.engine.mode == "paper":
                    result = self.engine.process_paper_exit(
                        entry.position.position_id,
                        replay_exit.at,
                        replay_exit.sell_quote,
                    )
                else:
                    if replay_exit.shadow_features is None:
                        raise ValueError(
                            "Shadow ReplayExit requires shadow_features"
                        )
                    result = self.engine.process_shadow_exit(
                        entry.position.position_id,
                        replay_exit.shadow_features,
                        replay_exit.at,
                        replay_exit.sell_quote,
                        replay_exit.returns_after_exit_pct,
                        replay_exit.avoided_loss_pct,
                        replay_exit.missed_profit_pct,
                    )
                exits.append(result)
                if result.closed_position is not None:
                    break
        return ReplayResult(tuple(entries), tuple(exits))
