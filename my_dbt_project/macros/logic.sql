{{ config(materialized='incremental') }}

SELECT
    item_mrp,
    item_type
FROM {{ ref("avg_price") }}
{% if is_incremental() %}
WHERE item_mrp > (SELECT MAX(item_mrp) FROM {{ avg_price }})
{% endif %}
