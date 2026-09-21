{% snapshot dim_customers_history %}
{{ config(target_schema='snapshots', unique_key='customer_id', strategy='check', check_cols=['first_name','last_name','email','phone','city','province','country','is_active']) }}
select * from {{ ref('customers') }}
{% endsnapshot %}

{% snapshot dim_stores_history %}
{{ config(target_schema='snapshots', unique_key='store_id', strategy='check', check_cols=['store_name','city','province','country','is_active']) }}
select * from {{ ref('stores') }}
{% endsnapshot %}

{% snapshot dim_products_history %}
{{ config(target_schema='snapshots', unique_key='product_id', strategy='check', check_cols=['product_name','category','brand','price','is_active']) }}
select * from {{ ref('products') }}
{% endsnapshot %}

{% snapshot dim_employees_history %}
{{ config(target_schema='snapshots', unique_key='employee_id', strategy='check', check_cols=['store_id','first_name','last_name','email','job_title','salary','is_active']) }}
select * from {{ ref('employees') }}
{% endsnapshot %}