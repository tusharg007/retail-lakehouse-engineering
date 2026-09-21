select order_item_id from {{ ref('order_items_enriched') }} group by order_item_id having count(*) <> 1
