"""Centralized SQL strings and query helpers. All reference the 'rollup' view (Parquet)."""

# Providers: distinct list
PROVIDERS = """
SELECT DISTINCT provider FROM rollup ORDER BY 1
"""

# Platforms: one row per unique_id with platform_name and provider
PLATFORMS = """
SELECT
    unique_id,
    MIN(platform_name) AS platform_name,
    MIN(provider) AS provider
FROM rollup
GROUP BY unique_id
ORDER BY unique_id
"""

# Platform details by id (param: unique_id)
PLATFORM_BY_ID = """
SELECT
    unique_id,
    MIN(platform_name) AS platform_name,
    MIN(provider) AS provider
FROM rollup
WHERE unique_id = ?
GROUP BY unique_id
"""

# Top platforms by point count in last N days (params: days, limit)
TOP_PLATFORMS = """
SELECT
    unique_id,
    MIN(platform_name) AS platform_name,
    SUM(point_count) AS points
FROM rollup
WHERE day >= current_date - (? * INTERVAL '1 day')
GROUP BY unique_id
ORDER BY points DESC, unique_id
LIMIT ?
"""

# Top providers (params: days, limit)
TOP_PROVIDERS = """
SELECT provider AS provider, SUM(point_count) AS points
FROM rollup
WHERE day >= current_date - (? * INTERVAL '1 day')
GROUP BY provider
ORDER BY points DESC
LIMIT ?
"""

# Daily stats for a platform by unique_id (params: id, days)
STATS_DAILY_PLATFORM = """
SELECT day AS date, SUM(point_count) AS points
FROM rollup
WHERE unique_id = ?
  AND day >= current_date - (? * INTERVAL '1 day')
GROUP BY day
ORDER BY 1
"""

# Daily stats for a provider (params: provider, days)
STATS_DAILY_PROVIDER = """
SELECT day AS date, SUM(point_count) AS points
FROM rollup
WHERE provider = ?
  AND day >= current_date - (? * INTERVAL '1 day')
GROUP BY day
ORDER BY 1
"""

# Daily stats split by provider for stacked charts (param: days)
STATS_DAILY_ALL_PROVIDERS = """
SELECT day AS date, provider, SUM(point_count) AS points
FROM rollup
WHERE day >= current_date - (? * INTERVAL '1 day')
GROUP BY day, provider
ORDER BY day, points DESC, provider
"""

# Daily stats split by platform for stacked charts (param: days)
STATS_DAILY_ALL_PLATFORMS = """
SELECT day AS date, unique_id, platform_name, SUM(point_count) AS points
FROM rollup
WHERE day >= current_date - (? * INTERVAL '1 day')
GROUP BY day, unique_id, platform_name
ORDER BY day, points DESC, unique_id
"""

# Global: total points, total files, unique platforms
STATS_GLOBAL = """
SELECT
    SUM(point_count) AS total_points,
    SUM(file_count) AS total_files,
    COUNT(DISTINCT unique_id) AS unique_platforms
FROM rollup
"""

# Discovery rate: new platforms per month (first appearance per unique_id), with id and name
STATS_DISCOVERY_RATE = """
WITH first_seen AS (
    SELECT unique_id, MIN(day) AS first_date
    FROM rollup
    GROUP BY unique_id
)
SELECT
    strftime(f.first_date, '%Y-%m') AS month,
    r.unique_id,
    MIN(r.platform_name) AS platform_name
FROM rollup r
INNER JOIN first_seen f ON r.unique_id = f.unique_id AND r.day = f.first_date
GROUP BY 1, 2
ORDER BY 1, 2
"""
