
/*
we get the distinct tacking_category_name available
*/

{% set tracking_category_sql %}
    select 
        distinct tracking_category_id,
        regexp_replace(lower(name), r'\W', '_')   as tracking_category_name
    from {{var('tracking_category')}}
    where status = 'ACTIVE'
{% endset %}

{% if execute %}
    {% set tracking_category = run_query(tracking_category_sql) %}
    {% set tracking_category_list = tracking_category.columns[1].values() %}
    {% set tc_length = tracking_category_list|length %}

{% else %}
    {% set tc_length = 1 %}
    {% set tracking_category_list = [] %} 

{% endif%}

with calendar as (

    select *
    from {{ ref('xero__calendar_spine') }}

), ledger as (

    select *
    from {{ ref('xero__general_ledger') }}

), joined as (

    select 
        {{ dbt_utils.surrogate_key(['calendar.date_month','ledger.account_id','ledger.source_relation']) }} as profit_and_loss_id,
        calendar.date_month, 
        ledger.account_id,
        ledger.account_name,
        ledger.account_code,
        ledger.account_type, 
        ledger.account_class,
        {% for n in range(0, tc_length )%}
        {{tracking_category_list[n]}} , 
        {% endfor %}
        ledger.source_relation, 
        coalesce(sum(ledger.net_amount * -1),0) as net_amount
    from calendar
    left join ledger
        on calendar.date_month = cast({{ dbt_utils.date_trunc('month', 'ledger.journal_date') }} as date)
    where ledger.account_class in ('REVENUE','EXPENSE')
    {{ dbt_utils.group_by(8 + tc_length) }}

)

select *
from joined