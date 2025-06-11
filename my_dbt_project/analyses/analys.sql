{{ config(materialized='incremental') }}

SELECT
    item_mrp,
    item_type
FROM {{ ref("my_model") }}