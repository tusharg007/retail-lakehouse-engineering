select oi.order_item_id, oi.order_id, oi.product_id, o.customer_id, o.store_id, o.order_timestamp, o.order_status, o.payment_method, o.total_amount order_total_amount, oi.quantity, oi.unit_price, oi.line_amount, p.product_name, p.category, p.brand, s.store_name, s.city store_city, s.province store_province, s.country store_country
from {{ ref('order_items') }} oi
join {{ ref('orders') }} o on oi.order_id=o.order_id
join {{ ref('products') }} p on oi.product_id=p.product_id
join {{ ref('stores') }} s on o.store_id=s.store_id
