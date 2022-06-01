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
        i.sub_total,
        i.total,
        i.total_tax,
        i.reference,
        i.is_sent_to_contact,
        i.invoice_status,
        i.type,
        i.url,
        case 
            when count(1) > 1 then 'Multiple Categories'
            else max(it.option)
        end as option
    from {{ var('invoice') }} i
    left join {{ var('invoice_line_item_tracking') }} it on
        i.invoice_id = it.invoice_id
    {{ dbt_utils.group_by(21) }}

{% if var('xero__using_bank_transaction', True) %}
), bank_transactions_pre as (

    select         
        b.bank_transaction_id,
        b.source_relation,
        b.contact_id,
        b.sub_total,
        b.total,
        b.total_tax,
        coalesce(b.currency_rate, 1) as currency_rate,
        currency_code,
        max(bt.option) as option,
        count(1) as options_no
    from {{ var('bank_transaction') }} b
    left join {{ var('bank_transaction_tracking') }} bt on
        b.bank_transaction_id = bt.bank_transaction_id
    {{ dbt_utils.group_by(8) }}
), bank_transactions as (

    select 
        bank_transaction_id,
        source_relation,
        contact_id,
        currency_rate,
        currency_code,
        sub_total,
        total,
        total_tax,
        case when options_no > 1 then 'Multiple Categories' else option end as option
    from bank_transactions_pre
    {{ dbt_utils.group_by(9) }}
), bank_transfers as (

    select 
        b.bank_transfer_id,
        case 
            when btf.currency_code = 'USD' then btf.total
            when btt.currency_code = 'USD' then btt.total
            else b.amount
        end as amount,
        case 
            when btf.currency_code = 'USD' then 'USD'
            when btt.currency_code = 'USD' then 'USD'
            else btf.currency_code
        end as bt_classification,
        b.currency_rate,
        b.date,
        b.from_bank_account_id,
        b.from_bank_transaction_id,
        b.has_attachments,
        b.to_bank_account_id,
        b.to_bank_transaction_id,
        b.source_relation
    from {{ var('bank_transfer') }} b
    left join bank_transactions btf on b.from_bank_transaction_id = btf.bank_transaction_id
    left join bank_transactions btt on b.to_bank_transaction_id = btt.bank_transaction_id
    {{ dbt_utils.group_by(11) }}

{% endif %}

{% if var('xero__using_credit_note', True) %}
), credit_notes_pre as (

    select 
        c.credit_note_id,
        c.source_relation,
        c.contact_id,
        coalesce(c.currency_rate, 1) as currency_rate,
        currency_code,
        c.sub_total,
        c.total,
        c.total_tax,
        max(ct.option) as option,
        count(1) as options_no
    from {{ var('credit_note') }} c
    left join {{ var('credit_note_tracking') }} ct on
        c.credit_note_id = ct.credit_note_id
    {{ dbt_utils.group_by(8) }}
), credit_notes as (

    select 
        c.credit_note_id,
        c.source_relation,
        c.contact_id,
        coalesce(c.currency_rate, 1) as currency_rate,
        currency_code,
        c.sub_total,
        c.total,
        c.total_tax,
        case when options_no > 1 then 'Multiple Categories' else option end as option
    from credit_notes_pre c
    {{ dbt_utils.group_by(9) }}

{% endif %}

), payments as (

    select 
        p.payment_id,
        p.currency_rate,
        p.invoice_id,
        p.date,
        p.status,
        p.amount,
        p.account_id,
        p.credit_note_id,
        p.bank_amount,
        p.source_relation, 
        coalesce(
            i.currency_code
            {% if var('xero__using_credit_note', True) %}
            , c.currency_code
            {%  endif %}
        ) as currency_code
    from {{ var('payment') }} as p
    left join {{ var('invoice') }} as i 
        on p.invoice_id = i.invoice_id
    {% if var('xero__using_credit_note', True) %}
    left join credit_notes c
        on p.credit_note_id = c.credit_note_id
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
        accounts.currency_code as account_currency_code,
        accounts.description as account_description,
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
        joined.journal_id,
        joined.created_date_utc,
        joined.journal_date,
        date_trunc(joined.journal_date,month) as month,
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
        joined.account_currency_code,
        joined.account_description,
        joined.description,
        joined.option,
        joined.gross_amount,
        abs(coalesce(

            {% if var('xero__using_bank_transaction', True) %}
                bank_transfers.amount,
            {% endif %}

            {% if var('xero__using_credit_note', True) %}
                credit_notes.total ,
            {% endif %}
            
            payments.amount
            -- , joined.net_amount
        )) * ( coalesce(safe_divide(joined.net_amount,abs(joined.net_amount)), 1)  ) as raw_net_amount,
        bank_transfers.bt_classification,
        joined.net_amount,

        invoices.total as invoices_total, 
        bank_transactions.total as bank_transactions_total, 
        bank_transfers.amount as bank_transfers_amount, 
        credit_notes.total     as credit_notes_total, 
        payments.amount as payments_amount, 
        joined.net_amount as joined_net_amount,

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
        -- Maybe add all other tables? Creating those models
        coalesce(
            invoices.currency_rate,
            payments.currency_rate
            {% if var('xero__using_bank_transaction', True) %}
                , bank_transactions.currency_rate
            {% endif %}

            {% if var('xero__using_credit_note', True) %}
                , credit_notes.currency_rate
            {% endif %}
        )

        as currency_rate,

        coalesce(
            invoices.currency_code,
            payments.currency_code
            {% if var('xero__using_bank_transaction', True) %}
                , bank_transactions.currency_code
            {% endif %}

            {% if var('xero__using_credit_note', True) %}
                , credit_notes.currency_code
            {% endif %}
        )

        as currency_code

    from joined
    left join invoices 
        on (joined.invoice_id = invoices.invoice_id
        and joined.source_relation = invoices.source_relation)
    left join payments
        on (joined.payment_id = payments.payment_id
        and joined.source_relation = payments.source_relation)
    {% if var('xero__using_bank_transaction', True) %}
    left join bank_transactions
        on (joined.bank_transaction_id = bank_transactions.bank_transaction_id
        and joined.source_relation = bank_transactions.source_relation)
    left join bank_transfers
        on (joined.bank_transfer_id = bank_transfers.bank_transfer_id
        and joined.source_relation = bank_transfers.source_relation)
    {% endif %}

    {% if var('xero__using_credit_note', True) %}
    left join credit_notes 
        on (joined.credit_note_id = credit_notes.credit_note_id
        and joined.source_relation = credit_notes.source_relation)
    {% endif %}
    {{ dbt_utils.group_by(42) }}

), second_contact as (

    select 
        first_contact.*,
        case 
            when coalesce(bank_transaction_id, invoice_id, credit_note_id) is not null
                then (first_contact.joined_net_amount / fxcad.currency_rate)  * fxus.currency_rate
            end as common_values,

        case
            when coalesce(bank_transfer_id, payment_id) is not null
                then (first_contact.raw_net_amount / fxall.currency_rate) * fxus.currency_rate
        end as bank_transfer_values,
        contacts.contact_name
    from first_contact
    left join {{ ref('xero__fx') }} fxcad on fxcad.date = first_contact.journal_date
    left join {{ ref('xero__fx') }} fxus on fxus.date = first_contact.journal_date
    left join {{ ref('xero__fx') }} fxall on  
            fxall.date = first_contact.journal_date and 
            fxall.currency_code = coalesce(first_contact.bt_classification, first_contact.currency_code, 'USD')
    left join contacts 
        on (first_contact.contact_id = contacts.contact_id
        and first_contact.source_relation = contacts.source_relation)
    where fxcad.currency_code = 'CAD' and fxus.currency_code = 'USD'
)

select *, coalesce(common_values, bank_transfer_values) as final_net_amount
from second_contact