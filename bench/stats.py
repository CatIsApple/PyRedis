"""지연 시간(ms) 표본의 통계. 벤치마크 CLI와 테스트가 같이 쓴다.

주요 함수: percentile(nearest-rank 백분위수), summarize(평균·최소·최대·p50·p95·p99).
"""

import math


def percentile(samples, p):
    """nearest-rank 백분위수: 정렬한 표본에서 ceil(p/100 * n)번째 값. 표본이 없으면 None."""
    if not samples:
        return None
    ordered = sorted(samples)
    rank = max(1, math.ceil((p / 100) * len(ordered)))
    return ordered[rank - 1]


def summarize(samples):
    if not samples:
        return {"count": 0, "mean": None, "min": None, "max": None, "p50": None, "p95": None, "p99": None}
    return {
        "count": len(samples),
        "mean": sum(samples) / len(samples),
        "min": min(samples),
        "max": max(samples),
        "p50": percentile(samples, 50),
        "p95": percentile(samples, 95),
        "p99": percentile(samples, 99),
    }
