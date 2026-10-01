select store_id, count(*) employee_count, sum(salary) active_employee_salary from {{ ref('employees') }} where is_active='Y' group by store_id
