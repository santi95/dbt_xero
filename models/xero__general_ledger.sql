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
        j.description,
        j.gross_amount,
        j.journal_id,
        j.net_amount,
        j.tax_amount,
        j.tax_name,
        j.tax_type
    from {{ var('journal_line') }} j
    left join {{  var('journal_line_tracking') }} jt on
        j.journal_line_id = jt.journal_line_id
    {{ dbt_utils.group_by(14) }}

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
        it.option
    from {{ var('invoice') }} i
    left join {{ var('invoice_line_item_tracking') }} it on
        i.invoice_id = it.invoice_id
    {{ dbt_utils.group_by(19) }}

{% if var('xero__using_bank_transaction', True) %}
), bank_transactions_pre as (

    select         
        b.bank_transaction_id,
        b.source_relation,
        b.contact_id,
        max(bt.option) as option,
        count(1) as options_no
    from {{ var('bank_transaction') }} b
    left join {{ var('bank_transaction_tracking') }} bt on
        b.bank_transaction_id = bt.bank_transaction_id
    {{ dbt_utils.group_by(3) }}
), bank_transactions as (

    select 
        bank_transaction_id,
        source_relation,
        contact_id,
        case when options_no > 1 then null else option end as option
    from bank_transactions_pre
    {{ dbt_utils.group_by(4) }}

{% endif %}

{% if var('xero__using_credit_note', True) %}
), credit_notes as (

    select 
        c.credit_note_id,
        c.source_relation,
        c.contact_id, 
        ct.option
    from {{ var('credit_note') }} c
    left join {{ var('credit_note_tracking') }} ct on
        c.credit_note_id = ct.credit_note_id
    {{ dbt_utils.group_by(4) }}

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
        joined.*,
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

        as contact_id,

        coalesce(
            joined.option
            ,invoices.option
            {% if var('xero__using_bank_transaction', True) %}
                , bank_transactions.option
            {% endif %}

            {% if var('xero__using_credit_note', True) %}
                , credit_notes.option
            {% endif %}
        )

        as full_option,

    from joined
    left join invoices 
        on (joined.invoice_id = invoices.invoice_id
        and joined.source_relation = invoices.source_relation)
    -- TODO: Take care of duplicated values, if multiple divisions then empty
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
    {{ dbt_utils.group_by(29) }}

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