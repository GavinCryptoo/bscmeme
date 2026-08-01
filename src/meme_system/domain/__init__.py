"""Domain contracts shared by Paper and Shadow."""

from .models import (
    BASELINE_IDENTITY,
    Candidate,
    CostBreakdown,
    EntryDecision,
    EntryFeatures,
    ExecutionRecord,
    ExitDecision,
    LifecycleEvent,
    PositionObservation,
    ShadowExitFeatures,
    ShadowOutcome,
    Signal,
    StrategyIdentity,
    VirtualPosition,
)

__all__ = [
    "BASELINE_IDENTITY",
    "Candidate",
    "CostBreakdown",
    "EntryDecision",
    "EntryFeatures",
    "ExecutionRecord",
    "ExitDecision",
    "LifecycleEvent",
    "PositionObservation",
    "ShadowExitFeatures",
    "ShadowOutcome",
    "Signal",
    "StrategyIdentity",
    "VirtualPosition",
]
