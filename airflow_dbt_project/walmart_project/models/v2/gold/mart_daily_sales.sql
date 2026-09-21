select fi.order_date, fi.store_id, e.category, fi.order_status, count(distinct fi.order_id) order_count, count(*) order_item_count, sum(fi.line_amount) gross_line_amount, sum(fi.completed_line_amount) completed_line_amount
from {{ ref('fact_order_items') }} fi join {{ ref('order_items_enriched') }} e on fi.order_item_id=e.order_item_id
group by fi.order_date, fi.store_id, e.category, fi.order_status
