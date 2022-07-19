/*
in order to generate trancking_category_name as a column we set a dictionary
dictionary has the following appareance{'tracking_category_id' : [tuple of available id], 'tracking_category_name': [tuple of available names]}
position match for id and name since the dictionary was generated with a sql statement 
*/

{% set tracking_category_sql %}
    select 
        distinct tracking_category_id,
        regexp_replace(lower(name), r'\W', '_')   as tracking_category_name
    from {{var('tracking_category')}}
    where status = 'ACTIVE'
{% endset %}



{% if execute %}
    -- fivetran_utils depends on dbt_utils 
    {% set tc_dict = dbt_utils.get_query_results_as_dict(tracking_category_sql) %}
    {% set tc_length = tc_dict['tracking_category_id']|length %}

    -- since this case when is repetead for all source_type, we set it here as a macro and call it each time it is needed
    {% set case_when_tracking_category %}
        {% for n in range(0, tc_length )%}
            case when tracking_category_id = '{{tc_dict['tracking_category_id'][n]}}'
                -- we concate available options for that tracking_category_id 
                then string_agg(distinct option, ' | ') 
                    end as {{tc_dict['tracking_category_name'][n]}}  {% if not loop.last %} , {% endif %}

        {% endfor %} 
    {% endset %}

{% else %}
    {% set tc_length = 1 %}
    {% set case_when_tracking_category = '' %} 
{% endif%}


with journals as (

    select *
    from {{ var('journal') }}

), journal_lines as (

    select 
        j.journal_line_id,
        j.source_relation,
        j.account_code,
        j.account_id,
        j.account_name,
        j.account_type,
        jt.option,
        jt.tracking_category_id,
        j.description,
        j.gross_amount,
        j.journal_id,
        j.net_amount,
        j.tax_amount,
        j.tax_name,
        j.tax_type,
        {{case_when_tracking_category}}

    from {{ var('journal_line') }} j
    left join {{  var('journal_line_tracking') }} jt on
        j.journal_line_id = jt.journal_line_id
    {{ dbt_utils.group_by(15) }}

), accounts as (

    select *
    from {{ var('account') }}

), invoices as (

    select 
        i.invoice_id,
        i.source_relation,
        i.contact_id,
        i.invoice_date,
        i.updated_date,
        i.planned_payment_date,
        i.due_date,
        i.expected_payment_date,
        i.fully_paid_on_date,
        i._fivetran_synced,
        i.currency_code,
        i.currency_rate,
        i.invoice_number,
        i.reference,
        i.is_sent_to_contact,
        i.invoice_status,
        i.type,
        i.url,
        it.option,
        it.tracking_category_id,
        {{case_when_tracking_category}}

    from {{ var('invoice') }} i
    left join {{ var('invoice_line_item_tracking') }} it on
        i.invoice_id = it.invoice_id
    {{ dbt_utils.group_by(20) }}

{% if var('xero__using_bank_transaction', True) %}
), bank_transactions as (

    select         
        b.bank_transaction_id,
        b.source_relation,
        b.contact_id,
        bt.option,
        bt.tracking_category_id,
        {{case_when_tracking_category}}
    from {{ var('bank_transaction') }} b
    left join {{ var('bank_transaction_tracking') }} bt on
        b.bank_transaction_id = bt.bank_transaction_id
    {{ dbt_utils.group_by(5) }}


{% endif %}

{% if var('xero__using_credit_note', True) %}
), credit_notes as (

    select 
        c.credit_note_id,
        c.source_relation,
        c.contact_id, 
        ct.option,
        ct.tracking_category_id,
        {{case_when_tracking_category}}
    from {{ var('credit_note') }} c
    left join {{ var('credit_note_tracking') }} ct on
        c.credit_note_id = ct.credit_note_id
    {{ dbt_utils.group_by(5) }}

{% endif %}

), contacts as (

    select *
    from {{ var('contact') }}

), joined as (

    select 
        journals.journal_id,
        journals.created_date_utc,
        journals.journal_date,
        journals.journal_number,
        journals.reference,
        journals.source_id,
        journals.source_type,
        journals.source_relation,
        journal_lines.journal_line_id,
        accounts.account_code,
        accounts.account_id,
        accounts.account_name,
        accounts.account_type,
        journal_lines.description,
        journal_lines.option,
        {% for n in range(0, tc_length )%}
        journal_lines.{{tc_dict['tracking_category_name'][n]}} , 
        {% endfor %}
        journal_lines.gross_amount,
        journal_lines.net_amount,
        journal_lines.tax_amount,
        journal_lines.tax_name,
        journal_lines.tax_type,
        accounts.account_class,
        case when journals.source_type in ('ACCPAY', 'ACCREC') then journals.source_id end as invoice_id,
        case when journals.source_type in ('CASHREC','CASHPAID') then journals.source_id end as bank_transaction_id,
        case when journals.source_type in ('TRANSFER') then journals.source_id end as bank_transfer_id,
        case when journals.source_type in ('MANJOURNAL') then journals.source_id end as manual_journal_id,
        case when journals.source_type in ('APPREPAYMENT', 'APOVERPAYMENT', 'ACCPAYPAYMENT', 'ACCRECPAYMENT', 'ARCREDITPAYMENT', 'APCREDITPAYMENT') then journals.source_id end as payment_id,
        case when journals.source_type in ('ACCPAYCREDIT','ACCRECCREDIT') then journals.source_id end as credit_note_id

    from journals
    left join journal_lines
        on (journals.journal_id = journal_lines.journal_id
        and journals.source_relation = journal_lines.source_relation)
    left join accounts
        on (accounts.account_id = journal_lines.account_id
        and accounts.source_relation = journal_lines.source_relation)

), first_contact as (

    select 
        joined.journal_id,
        joined.created_date_utc,
        joined.journal_date,
        joined.journal_number,
        joined.reference,
        joined.source_id,
        joined.source_type,
        joined.source_relation,
        joined.journal_line_id,
        joined.account_code,
        joined.account_id,
        joined.account_name,
        joined.account_type,
        joined.description,
        joined.option,
        joined.gross_amount,
        joined.net_amount,
        joined.tax_amount,
        joined.tax_name,
        joined.tax_type,
        joined.account_class,
        joined.invoice_id,
        joined.bank_transaction_id,
        joined.bank_transfer_id,
        joined.manual_journal_id,
        joined.payment_id,
        joined.credit_note_id,

        {% if fivetran_utils.enabled_vars_one_true(['xero__using_bank_transaction','xero__using_credit_note']) %}
        
        coalesce(
            invoices.contact_id
            {% if var('xero__using_bank_transaction', True) %}
                , bank_transactions.contact_id
            {% endif %}

            {% if var('xero__using_credit_note', True) %}
            , credit_notes.contact_id
            {% endif %}
        )
        {% else %}
        invoices.contact_id
        {% endif %}

        as contact_id

        {% for n in range(0, tc_length )%}
        ,coalesce(
            joined.{{tc_dict['tracking_category_name'][n]}}, 
            invoices.{{tc_dict['tracking_category_name'][n]}}

            {% if var('xero__using_bank_transaction', True) %}
            , bank_transactions.{{tc_dict['tracking_category_name'][n]}}
            {% endif %}

            {% if var('xero__using_credit_note', True) %}
            , credit_notes.{{tc_dict['tracking_category_name'][n]}}
            {% endif %}

            ) as {{tc_dict['tracking_category_name'][n]}}
        {% endfor %}


    from joined
    left join invoices 
        on (joined.invoice_id = invoices.invoice_id
        and joined.source_relation = invoices.source_relation)
    {% if var('xero__using_bank_transaction', True) %}
    left join bank_transactions
        on (joined.bank_transaction_id = bank_transactions.bank_transaction_id
        and joined.source_relation = bank_transactions.source_relation)
    {% endif %}

    {% if var('xero__using_credit_note', True) %}
    left join credit_notes 
        on (joined.credit_note_id = credit_notes.credit_note_id
        and joined.source_relation = credit_notes.source_relation)
    {% endif %}
    {{ dbt_utils.group_by(28 + tc_length) }}

), second_contact as (

    select 
        first_contact.*,
        contacts.contact_name
    from first_contact
    left join contacts 
        on (first_contact.contact_id = contacts.contact_id
        and first_contact.source_relation = contacts.source_relation)

)

select *
from second_contact