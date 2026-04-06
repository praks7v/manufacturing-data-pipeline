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
            "B-QUALITY",
            BEADING,
            CUTTING,
            DENT,
            "INSIDE SOS",
            LINING,
            "LOOSE CAP",
            "NIPPLE NOT FITTING",
            "PLAIN BOTTOM",
            REPLATING,
            REPOLISH,
            SCRAP,
            SCRAP,
            "TIGHT CAP"
        )
    )
)

SELECT
    item_description,
    process,
    SUM(defect_count) AS total_defects
FROM unpivoted
GROUP BY item_description, process