select
    orders.order_id,
    orders.customer_id,
    payments.amount
from {{ ref('stg_payments') }} as payments
    inner join {{ ref('stg_orders') }} as orders
        on payments.order_id = orders.order_id