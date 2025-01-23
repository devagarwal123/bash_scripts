WITH ranked_resources AS (
    SELECT
        resourcevalue,
        pool,
        status,
        esa,
        szu,
        carrier,
        resourcevalue - ROW_NUMBER() OVER (PARTITION BY pool, status, esa, szu, carrier ORDER BY resourcevalue) AS grp
    FROM resource
    WHERE status = 'A' AND pool = 'x'
),
grouped_resources AS (
    SELECT
        MIN(resourcevalue) AS startvalue,
        MAX(resourcevalue) AS endrange,
        pool,
        status,
        esa,
        szu,
        carrier,
        COUNT(*) AS rangesize
    FROM ranked_resources
    GROUP BY grp, pool, status, esa, szu, carrier
)
SELECT
    startvalue,
    endrange,
    rangesize,
    pool,
    status,
    esa,
    szu,
    carrier
FROM grouped_resources
ORDER BY startvalue;
