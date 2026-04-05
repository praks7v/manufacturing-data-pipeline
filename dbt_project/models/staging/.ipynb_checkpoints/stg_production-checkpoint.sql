SELECT
    production_id,
    DATE(date) AS production_date,
    item_description,
    process,
    ok_quantity,
    
    -- defects
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
    "TIGHT CAP"

FROM raw_production