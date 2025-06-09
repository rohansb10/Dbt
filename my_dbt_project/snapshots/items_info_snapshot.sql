{% snapshot items_info_snapshot %}
{{
    config(
      target_schema='snapshots',
      unique_key='item_id',
      strategy='check',
      check_cols=['item_type', 'item_mrp', 'store_id']
    )
}}

SELECT 
  item_id,
  item_type,
  item_mrp,
  store_id
FROM {{ ref('items_info') }}

{% endsnapshot %}