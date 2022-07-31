with customer_orders as (

    select
        orders.customer_id,

        min(orders.order_date) as first_order_date,
        max(orders.order_date) as most_recent_order_date,
        count(orders.order_id) as number_of_orders,
        sum(orders.amount) as lifetime_value

    from {{ ref('fct_orders')}} as orders

    group by 1

)

select
    customers.customer_id,
    customers.first_name,
    customers.last_name,
    customer_orders.first_order_date,
    customer_orders.most_recent_order_date,
    coalesce(customer_orders.number_of_orders, 0) as number_of_orders,
    coalesce(customer_orders.lifetime_value, 0) as lifetime_value

from {{ ref('stg_customers') }} as customers

left join customer_orders using (customer_id)