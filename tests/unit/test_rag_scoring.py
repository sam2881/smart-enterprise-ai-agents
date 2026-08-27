"""Tests for RAG Reciprocal Rank Fusion (RRF) scoring.

The RRF algorithm is implemented in backend/rag/hybrid_search_engine.py.
These tests validate the mathematical correctness of the scoring formula
and the fusion logic without importing the actual module (which requires
weaviate, neo4j, and other heavy dependencies).

RRF Formula: score(rank) = 1 / (k + rank), where k=60 (constant)
"""
import pytest
from typing import List, Dict, Any


# --- Pure implementation of RRF for testing ---

RRF_K = 60  # matches hybrid_search_engine.py constant


def rrf_score(rank: int, k: int = RRF_K) -> float:
    """Compute RRF score for a single result at given rank (1-indexed)."""
    return 1.0 / (k + rank)


def merge_results_rrf(
    agent_results: List[List[Dict[str, Any]]],
    k: int = RRF_K,
) -> List[Dict[str, Any]]:
    """
    Merge results from multiple agents using RRF.
    Each result list is ordered by relevance (index 0 = most relevant).
    Results appearing in multiple lists get boosted scores.
    Returns merged list sorted by total RRF score descending.
    """
    scores: Dict[str, float] = {}
    metadata: Dict[str, Dict] = {}

    for results in agent_results:
        for rank, item in enumerate(results, start=1):
            item_id = item["id"]
            scores[item_id] = scores.get(item_id, 0.0) + rrf_score(rank, k)
            metadata[item_id] = item

    merged = []
    for item_id, total_score in scores.items():
        entry = dict(metadata[item_id])
        entry["rrf_score"] = total_score
        merged.append(entry)

    return sorted(merged, key=lambda x: x["rrf_score"], reverse=True)


def filter_by_confidence(
    results: List[Dict[str, Any]], threshold: float = 0.6
) -> List[Dict[str, Any]]:
    """Filter results below confidence threshold (rrf_score)."""
    return [r for r in results if r.get("rrf_score", 0) >= threshold]


# --- Tests ---

class TestRRFScoreFormula:
    """Test the RRF scoring formula: 1 / (k + rank)."""

    def test_rank_1_with_default_k(self):
        """Rank 1, k=60 → 1/61."""
        assert abs(rrf_score(1) - (1 / 61)) < 1e-10

    def test_rank_60_with_default_k(self):
        """Rank 60, k=60 → 1/120."""
        assert abs(rrf_score(60) - (1 / 120)) < 1e-10

    def test_higher_rank_lower_score(self):
        """Lower rank numbers (better position) give higher scores."""
        assert rrf_score(1) > rrf_score(10) > rrf_score(100)

    def test_k_larger_reduces_rank_difference(self):
        """With larger k, the score difference between ranks 1 and 2 is smaller."""
        diff_k60 = rrf_score(1, k=60) - rrf_score(2, k=60)
        diff_k5 = rrf_score(1, k=5) - rrf_score(2, k=5)
        assert diff_k5 > diff_k60

    def test_score_always_positive(self):
        """RRF scores are always positive."""
        for rank in [1, 5, 10, 100]:
            assert rrf_score(rank) > 0


class TestRRFFusion:
    """Test multi-agent result merging with RRF."""

    def test_single_agent_preserves_order(self):
        """Single agent results stay in original order."""
        results = merge_results_rrf([[
            {"id": "script_a", "content": "restart service"},
            {"id": "script_b", "content": "increase memory"},
            {"id": "script_c", "content": "check logs"},
        ]])
        assert [r["id"] for r in results] == ["script_a", "script_b", "script_c"]

    def test_common_result_boosted_by_multiple_agents(self):
        """Result appearing in all 4 agents should rank first."""
        agent1 = [{"id": "common"}, {"id": "unique_a"}]
        agent2 = [{"id": "common"}, {"id": "unique_b"}]
        agent3 = [{"id": "unique_c"}, {"id": "common"}]
        agent4 = [{"id": "unique_d"}, {"id": "common"}]

        results = merge_results_rrf([agent1, agent2, agent3, agent4])
        assert results[0]["id"] == "common"

    def test_empty_agent_does_not_crash(self):
        """An agent returning [] should not break fusion."""
        results = merge_results_rrf([
            [{"id": "script_a"}],
            [],
            [{"id": "script_b"}],
        ])
        assert len(results) == 2

    def test_all_empty_agents_returns_empty(self):
        results = merge_results_rrf([[], [], [], []])
        assert results == []

    def test_rrf_score_attached_to_each_result(self):
        """Every merged result has an rrf_score field."""
        results = merge_results_rrf([[{"id": "a"}, {"id": "b"}]])
        for r in results:
            assert "rrf_score" in r
            assert r["rrf_score"] > 0

    def test_common_result_score_is_sum_of_per_agent_scores(self):
        """RRF score for a result appearing in 2 agents = sum of both scores."""
        agent1 = [{"id": "common"}]   # rank 1 in agent1
        agent2 = [{"id": "common"}]   # rank 1 in agent2
        expected_score = rrf_score(1) + rrf_score(1)

        results = merge_results_rrf([agent1, agent2])
        assert abs(results[0]["rrf_score"] - expected_score) < 1e-10

    def test_metadata_preserved_from_last_agent(self):
        """Result metadata (beyond id) is preserved."""
        results = merge_results_rrf([[
            {"id": "script_a", "content": "restart postgres", "tags": ["db"]}
        ]])
        assert results[0]["content"] == "restart postgres"
        assert results[0]["tags"] == ["db"]


class TestConfidenceFiltering:
    """Test threshold-based filtering of low-confidence results."""

    def test_filters_below_threshold(self):
        results = [
            {"id": "a", "rrf_score": 0.8},
            {"id": "b", "rrf_score": 0.4},  # below 0.6
            {"id": "c", "rrf_score": 0.7},
        ]
        filtered = filter_by_confidence(results, threshold=0.6)
        assert len(filtered) == 2
        assert all(r["id"] != "b" for r in filtered)

    def test_empty_list_returns_empty(self):
        assert filter_by_confidence([], threshold=0.6) == []

    def test_all_above_threshold_returns_all(self):
        results = [{"id": str(i), "rrf_score": 0.9} for i in range(5)]
        assert len(filter_by_confidence(results)) == 5

    def test_exactly_at_threshold_is_included(self):
        results = [{"id": "a", "rrf_score": 0.6}]
        assert len(filter_by_confidence(results, threshold=0.6)) == 1
