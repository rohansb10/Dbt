-- This test fails if any item_mrp is > 10000
SELECT *
FROM {{ ref('items_info') }}
WHERE item_mrp > 10000
