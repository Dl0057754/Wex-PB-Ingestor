from decimal import Decimal

import pytest

from pb_ingestor.markup import MarkupProfile, MarkupTier


def test_markup_rounding_and_pricing():
    profile = MarkupProfile(
        [
            MarkupTier(min_cost=Decimal("0.01"), max_cost=Decimal("1.00"), markup_percent=Decimal("400"), order=1),
            MarkupTier(min_cost=Decimal("1.01"), max_cost=None, markup_percent=Decimal("100"), order=2),
        ]
    )
    assert profile.price_for_cost(Decimal("0.10")) == Decimal("0.50")
    assert profile.price_for_cost(Decimal("1.50")) == Decimal("3.00")


def test_markup_overlap_raises():
    with pytest.raises(ValueError):
        MarkupProfile(
            [
                MarkupTier(min_cost=Decimal("0.01"), max_cost=Decimal("1.00"), markup_percent=Decimal("400"), order=1),
                MarkupTier(min_cost=Decimal("1.00"), max_cost=Decimal("2.00"), markup_percent=Decimal("300"), order=2),
            ]
        )
