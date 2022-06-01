with date_spine as (
    select  days
    from unnest(GENERATE_DATE_ARRAY('2016-01-01', date_add(current_date(), interval  1 month) )) as days
),
union_all as (
    select date, currency_code, currency_rate
    from {{ source('xero','bank_transaction') }}
    union all
    select date, currency_code, currency_rate
    from {{ source('xero','credit_note') }}
    union all
    select date, currency_code, currency_rate
    from {{ source('xero','invoice') }}
),
final as (

select 
    d.days,
    cc.currency_code,
    coalesce(u.currency_rate) as currency_rate,
    first_value(u.currency_rate IGNORE NULLS) over (partition by cc.currency_code order by d.days range between current row and unbounded following) as next_valid_value
from date_spine d
cross join (select distinct currency_code from union_all) as cc
left join union_all u on u.date = d.days and u.currency_code = cc.currency_code
)

select 
    days as date,
    currency_code,
    avg(coalesce(currency_rate, next_valid_value)) as currency_rate
from final
group by 1,2

