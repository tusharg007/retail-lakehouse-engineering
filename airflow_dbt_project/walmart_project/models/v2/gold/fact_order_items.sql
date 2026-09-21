select order_item_id, order_id, product_id, customer_id, store_id, cast(order_timestamp as date) order_date, order_status, quantity, unit_price, line_amount,
case when order_status='Completed' then line_amount else cast(0 as decimal(12,2)) end completed_line_amount
from {{ ref('order_items_enriched') }}
