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

    def price_for_cost(self, cost: Decimal) -> Decimal:
        for tier in self.tiers:
            if tier.matches(cost):
                multiplier = Decimal("1") + (tier.markup_percent / Decimal("100"))
                return (cost * multiplier).quantize(Decimal("0.01"), rounding=ROUND_HALF_UP)
        raise ValueError(f"No markup tier found for cost {cost}")
