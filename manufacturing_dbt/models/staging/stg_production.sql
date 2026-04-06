SELECT
    production_id,
    DATE(date) AS production_date,
    item_description,
    process,
    ok_quantity,
    
    -- defects (add aliases for clarity and consistency)
    `B-QUALITY` AS b_quality,
    BEADING AS beading,
    CUTTING AS cutting,
    DENT AS dent,
    `INSIDE SOS` AS inside_sos,
    LINING AS lining,
    `LOOSE CAP` AS loose_cap,
    `NIPPLE NOT FITTING` AS nipple_not_fitting,
    `PLAIN BOTTOM` AS plain_bottom,
    REPLATING AS replating,
    REPOLISH AS repolish,
    SCRAP AS scrap,
    `TIGHT CAP` AS tight_cap

FROM {{ source('manufacturing', 'raw_production') }}