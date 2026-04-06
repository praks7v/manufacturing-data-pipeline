WITH base AS (
    SELECT * FROM {{ ref('stg_production') }}
),

unpivoted AS (
    SELECT
        production_id,
        production_date,
        item_description,
        process,
        defect_type,
        defect_count
    FROM base
    UNPIVOT (
        defect_count FOR defect_type IN (
            b_quality,
            beading,
            cutting,
            dent,
            inside_sos,
            lining,
            loose_cap,
            nipple_not_fitting,
            plain_bottom,
            replating,
            repolish,
            scrap,
            tight_cap
        )
    )
)

SELECT
    item_description,
    process,
    SUM(defect_count) AS total_defects
FROM unpivoted
GROUP BY item_description, process