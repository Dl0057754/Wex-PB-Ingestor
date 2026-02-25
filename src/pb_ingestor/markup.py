from __future__ import annotations

import json
from dataclasses import dataclass
from decimal import Decimal, ROUND_HALF_UP
from pathlib import Path
from typing import Optional


@dataclass(frozen=True)
class MarkupTier:
    min_cost: Decimal
    max_cost: Optional[Decimal]
    markup_percent: Decimal
    enabled: bool = True
    order: int = 1

    def matches(self, cost: Decimal) -> bool:
        if not self.enabled:
            return False
        if cost < self.min_cost:
            return False
        if self.max_cost is None:
            return True
        return cost <= self.max_cost


class MarkupProfile:
    def __init__(self, tiers: list[MarkupTier]) -> None:
        self.tiers = sorted(tiers, key=lambda t: t.order)
        self.validate_tiers()

    @classmethod
    def from_file(cls, path: str | Path) -> "MarkupProfile":
        data = json.loads(Path(path).read_text())
        tiers = [
            MarkupTier(
                min_cost=Decimal(str(t["min_cost"])),
                max_cost=Decimal(str(t["max_cost"])) if t.get("max_cost") is not None else None,
                markup_percent=Decimal(str(t["markup_percent"])),
                enabled=t.get("enabled", True),
                order=t.get("order", i + 1),
            )
            for i, t in enumerate(data["tiers"])
        ]
        return cls(tiers)

    def validate_tiers(self) -> None:
        enabled = [t for t in self.tiers if t.enabled]
        if not enabled:
            raise ValueError("At least one enabled markup tier is required")
        previous_max: Optional[Decimal] = None
        for idx, tier in enumerate(enabled):
            if tier.max_cost is not None and tier.max_cost < tier.min_cost:
                raise ValueError(f"Tier {idx+1} has max_cost < min_cost")
            if previous_max is not None and tier.min_cost <= previous_max:
                raise ValueError("Markup tiers overlap or are out of order")
            previous_max = tier.max_cost

    def price_for_cost(self, cost: Decimal) -> Decimal:
        for tier in self.tiers:
            if tier.matches(cost):
                multiplier = Decimal("1") + (tier.markup_percent / Decimal("100"))
                return (cost * multiplier).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        raise ValueError(f"No markup tier found for cost {cost}")
