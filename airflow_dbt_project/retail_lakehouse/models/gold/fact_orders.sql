select order_id, customer_id, store_id, cast(order_timestamp as date) order_date, order_status, payment_method, total_amount gross_order_amount,
case when order_status='Completed' then total_amount else cast(0 as decimal(12,2)) end completed_order_amount,
case when order_status in ('Cancelled','Returned') then total_amount else cast(0 as decimal(12,2)) end cancelled_or_returned_amount
from {{ ref('orders') }}
