-- models/example/avg_price.sql
SELECT
  AVG(item_mrp) AS average_price
FROM {{ ref('items_info') }}  -- Correct reference to seed