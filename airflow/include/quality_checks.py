"""Observe check configurations for Project 2 Gold layer tables."""

from __future__ import annotations

from observe.checks import (
    DistributionCheck,
    NullRateCheck,
    RangeCheck,
    RowCountCheck,
    SchemaCheck,
    UniquenessCheck,
)


MART_SCHEMA = {
    "content_sk": "object",
    "movie_id": "object",
    "title": "object",
    "release_year": "int64",
    "era": "object",
    "primary_genre": "object",
    "total_ratings": "int64",
    "avg_rating": "float64",
    "pct_positive": "float64",
    "weighted_score": "float64",
}

GOLD_MART_CHECKS = [
    RowCountCheck(min=1_000, max=200_000),
    NullRateCheck("content_sk", threshold=0.0),
    NullRateCheck("avg_rating", threshold=0.0),
    NullRateCheck("primary_genre", threshold=0.0),
    SchemaCheck(expected=MART_SCHEMA),
    RangeCheck("avg_rating", min_val=0.5, max_val=5.0),
    RangeCheck("pct_positive", min_val=0.0, max_val=100.0),
    RangeCheck("release_year", min_val=1888, max_val=2026),
    UniquenessCheck("content_sk", threshold=0.0),
    UniquenessCheck("movie_id", threshold=0.0),
    DistributionCheck("avg_rating", baseline_mean=3.5, z_score_threshold=4.0),
]

FACT_VIEWERSHIP_CHECKS = [
    RowCountCheck(min=10_000_000, max=30_000_000),
    NullRateCheck("rating_sk", threshold=0.0),
    NullRateCheck("user_sk", threshold=0.0),
    NullRateCheck("content_sk", threshold=0.0),
    RangeCheck("rating_value", min_val=0.5, max_val=5.0),
    UniquenessCheck("rating_sk", threshold=0.0),
]
