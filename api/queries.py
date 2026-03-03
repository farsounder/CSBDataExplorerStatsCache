"""Centralized SQL strings and query helpers. All reference the 'source' view (Parquet)."""

# Providers: distinct list
PROVIDERS = """
SELECT DISTINCT PROVIDER FROM source ORDER BY 1
"""

# Platforms: distinct UNIQUE_ID, PLATFORM_NAME
PLATFORMS = """
SELECT DISTINCT UNIQUE_ID, PLATFORM_NAME FROM source ORDER BY UNIQUE_ID
"""

# Top platforms by point count in last N days (params: days, limit)
TOP_PLATFORMS = """
SELECT PLATFORM_NAME AS platform_name, COUNT(*) AS points
FROM source
WHERE TIME >= current_timestamp - INTERVAL (?) DAYS
GROUP BY PLATFORM_NAME
ORDER BY points DESC
LIMIT ?
"""

# Top providers (params: days, limit)
TOP_PROVIDERS = """
SELECT PROVIDER AS provider, COUNT(*) AS points
FROM source
WHERE TIME >= current_timestamp - INTERVAL (?) DAYS
GROUP BY PROVIDER
ORDER BY points DESC
LIMIT ?
"""

# Daily stats for a platform by UNIQUE_ID (param: id)
STATS_DAILY_PLATFORM = """
SELECT CAST(TIME AS DATE) AS date, COUNT(*) AS points
FROM source
WHERE UNIQUE_ID = ?
GROUP BY CAST(TIME AS DATE)
ORDER BY 1
"""

# Global: total points, total files, unique platforms
STATS_GLOBAL = """
SELECT
    COUNT(*) AS total_points,
    COUNT(DISTINCT FILE_UUID) AS total_files,
    COUNT(DISTINCT UNIQUE_ID) AS unique_platforms
FROM source
"""

# Discovery rate: new platforms per month (first appearance of UNIQUE_ID), with id and name
STATS_DISCOVERY_RATE = """
WITH first_seen AS (
    SELECT UNIQUE_ID,
           MIN(CAST(TIME AS DATE)) AS first_date,
           ANY_VALUE(PLATFORM_NAME) AS PLATFORM_NAME
    FROM source
    GROUP BY UNIQUE_ID
)
SELECT strftime(first_date, '%Y-%m') AS month, UNIQUE_ID, PLATFORM_NAME
FROM first_seen
ORDER BY 1, 2
"""
