SELECT * FROM {{ ref("data") }}

ORDER BY item_mrp DESC