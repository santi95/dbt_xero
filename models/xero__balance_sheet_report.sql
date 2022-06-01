with calendar as (

    select *
    from {{ ref('xero__calendar_spine') }}

), ledger as (

    select *
    from {{ ref('xero__general_ledger') }}

), organization as (

    select *
    from {{ var('organization') }}

), bank_transfers as (

    select 
        bt.bank_transfer_id,
        bt.currency_rate as tansfer_currency_rate,
        btr.currency_code as from_currency_code,
        btr.currency_rate as from_currency_rate, 
        btr2.currency_code as to_currency_code, 
        btr2.currency_rate as to_currency_rate
    from {{ ref('stg_xero__bank_transfer') }} bt
    left join {{ ref('stg_xero__bank_transaction') }}  btr on bt.from_bank_transaction_id = btr.bank_transaction_id
    left join {{ ref('stg_xero__bank_transaction') }}  btr2 on bt.to_bank_transaction_id = btr2.bank_transaction_id

), year_end as (

    select 
        case
            when cast(extract(year from current_date) || '-' || financial_year_end_month || '-' || financial_year_end_day as date) >= current_date
            then cast(extract(year from current_date) || '-' || financial_year_end_month || '-' || financial_year_end_day as date)
            else cast(extract(year from {{ dbt_utils.dateadd('year', -1, 'current_date') }}) || '-' || financial_year_end_month || '-' || financial_year_end_day as date)
        end as current_year_end_date
    from organization

), joined as (

    select
        calendar.date_month,
        case
            when ledger.account_class in ('ASSET','EQUITY','LIABILITY') then ledger.account_name
            when ledger.journal_date <= {{ dbt_utils.dateadd('year', -1, 'year_end.current_year_end_date') }} then 'Retained Earnings'
            else 'Current Year Earnings'
        end as account_name,
        case
            when ledger.account_class in ('ASSET','EQUITY','LIABILITY') then ledger.account_code
            else null
        end as account_code,
        case
            when ledger.account_class in ('ASSET','EQUITY','LIABILITY') then ledger.account_id
            else null
        end as account_id,
        case
            when ledger.account_class in ('ASSET','EQUITY','LIABILITY') then ledger.account_type
            else null
        end as account_type,
        case
            when ledger.account_class in ('ASSET','EQUITY','LIABILITY') then ledger.account_class
            else 'EQUITY'
        end as account_class,
        ledger.account_currency_code,
        ledger.account_description,
        ledger.source_id,
        ledger.journal_date,
        ledger.currency_rate,
        ledger.currency_code,
        {% if var('xero__using_bank_transaction', True) %}
            ledger.bank_transfer_id,
            bt.tansfer_currency_rate as bank_tansfer_currency_rate,
            bt.from_currency_code as bank_transaction_from_currency_code,
            bt.from_currency_rate as bank_transaction_from_currency_rate,
            bt.to_currency_code as bank_transaction_to_currency_code,
            bt.to_currency_rate as bank_transaction_to_currency_rate,
        {% endif %}
        ledger.description,
        ledger.reference,
        ledger.source_relation,
        sum(ledger.net_amount) as net_amount
    from calendar
    inner join ledger
        on calendar.date_month >= cast({{ dbt_utils.date_trunc('month', 'ledger.journal_date') }} as date)
    cross join year_end
    {% if var('xero__using_bank_transaction', True) %}
    -- To replace for full bank_transfer with both bank_transaction currency rates and codes
        left join bank_transfers as bt on bt.bank_transfer_id = ledger.bank_transfer_id
        {{ dbt_utils.group_by(21) }}
    {% else %}
        {{ dbt_utils.group_by(15) }}
    {% endif %}

)

select *
from joined
where net_amount != 0