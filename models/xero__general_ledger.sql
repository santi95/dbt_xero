

WITH ORGANIZATION AS (
    SELECT
        base_currency
    FROM
        {{ var('organization') }}
    LIMIT
        1
), journals AS (
    SELECT
        *
    FROM
        {{ var('journal') }}
),
journal_lines AS (
    SELECT
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
    FROM
        {{ var('journal_line') }} AS j --1062602
        LEFT JOIN {{ var('journal_line_tracking') }} AS jt
        ON --1062602
        j.journal_line_id = jt.journal_line_id {{ dbt_utils.group_by(14) }}
),
accounts AS (
    SELECT
        *
    FROM
        {{ var('account') }}
),
invoices AS (
    SELECT
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
        i.total AS amount,
        i.total_tax,
        i.reference,
        i.is_sent_to_contact,
        i.invoice_status,
        i.type,
        i.url,
        CASE
            WHEN COUNT(1) > 1 THEN 'Multiple Categories'
            ELSE MAX(
                it.option
            )
        END AS OPTION
    FROM
        {{ var('invoice') }} AS i --32244
        LEFT JOIN {{ var('invoice_line_item_tracking') }} AS it
        ON --48049
        i.invoice_id = it.invoice_id {{ dbt_utils.group_by(21) }}
),

{% if var('xero__using_bank_transaction',True) %}
    bank_transactions_pre AS (
        SELECT
            b.bank_transaction_id,
            b.source_relation,
            b.contact_id,
            b.sub_total,
            b.total,
            b.total_tax,
            COALESCE(
                b.currency_rate,
                1
            ) AS currency_rate,
            currency_code,
            CASE
                WHEN COUNT(1) > 1 THEN 'Multiple Categories'
                ELSE MAX(
                    bt.option
                )
            END AS OPTION
        FROM
            {{ var('bank_transaction') }} AS b --288,092
            LEFT JOIN {{ var('bank_transaction_tracking') }} AS bt
            ON --289,577
            b.bank_transaction_id = bt.bank_transaction_id {{ dbt_utils.group_by(8) }}
    ),
    bank_transactions AS (
        SELECT
            bank_transaction_id,
            source_relation,
            contact_id,
            currency_rate,
            currency_code,
            sub_total,
            total AS amount,
            total_tax,
            OPTION
        FROM
            bank_transactions_pre {{ dbt_utils.group_by(9) }}
    ),
    -- Bank transfers are just a connection between bank_transactions, so nesting this if statement safely
    {% if var('xero__using_bank_transfer',True) %}
        bank_transfers AS (
            SELECT
                b.bank_transfer_id,
                b.date,
                b.amount,
                b.currency_rate,
                b.from_bank_account_id,
                b.from_bank_transaction_id,
                btf.currency_code AS from_currency_code,
                btf.amount AS from_amount,
                b.to_bank_account_id,
                b.to_bank_transaction_id,
                btt.currency_code AS to_currency_code,
                btt.amount AS to_amount,
                b.has_attachments,
                b.source_relation,
                COALESCE(
                    btf.option,
                    btt.option
                ) AS OPTION,
                COALESCE(
                    btf.contact_id,
                    btt.contact_id
                ) AS contact_id
            FROM
                {{ var('bank_transfer') }} AS b --8,368
                LEFT JOIN bank_transactions AS btf
                ON b.from_bank_transaction_id = btf.bank_transaction_id
                LEFT JOIN bank_transactions AS btt
                ON b.to_bank_transaction_id = btt.bank_transaction_id
        ),
    {% endif %}
{% endif %}

{% if var('xero__using_credit_note',True) %}
    credit_notes AS (
        SELECT
            C.credit_note_id,
            C.source_relation,
            C.contact_id,
            COALESCE(
                C.currency_rate,
                1
            ) AS currency_rate,
            currency_code,
            C.sub_total,
            C.total AS amount,
            C.total_tax,
            CASE
                WHEN COUNT(1) > 1 THEN 'Multiple Categories'
                ELSE MAX(
                    ct.option
                )
            END AS OPTION
        FROM
            {{ var('credit_note') }} AS C
            LEFT JOIN {{ var('credit_note_tracking') }} AS ct
            ON C.credit_note_id = ct.credit_note_id {{ dbt_utils.group_by(8) }}
    ),
{% endif %}
-- There are some instances that don't have overpayument_line_tracking only, and because of that we are considering using_overpayment as false
{% if var('xero__using_overpayment',True) %}
    overpayments AS (
        SELECT
            o.overpayment_id,
            o.contact_id,
            o.currency_code,
            o.total as amount,
            CASE
                WHEN COUNT(1) > 1 THEN 'Multiple Categories'
                ELSE MAX(
                    olt.option
                )
            END AS OPTION
        FROM
            {{ var('overpayment') }} o
        left join {{ var('overpayment_line') }} as ol on ol.overpayment_id = o.overpayment_id
        left join {{ var('overpayment_line_tracking') }} as olt on olt.line_item_id = ol.line_item_id
        {{ dbt_utils.group_by(4) }}
    ),
{% endif %}
payments AS (
    SELECT
        p.payment_id,
        p.account_id,
        p.currency_rate,
        p.DATE,
        p.status,
        p.amount,
        p.bank_amount,
        coalesce(p.invoice_id,p.credit_note_id) as source_id,
    FROM
        {{ var('payment') }} p
), 
contacts AS (
    SELECT
        *
    FROM
        {{ var('contact') }}
),
raw_amounts AS (
    SELECT
        invoice_id AS source_id,
        amount,
        currency_code,
        OPTION,
        contact_id
    FROM
        invoices
    {% if var('xero__using_overpayment',True) %}
        UNION ALL
        select
            overpayment_id as source_id,
            amount,
            currency_code,
            option,
            contact_id
        from overpayments
    {% endif %}
    {% if var('xero__using_bank_transaction',True) %}
        UNION ALL
        SELECT
            bank_transaction_id AS source_id,
            amount,
            currency_code,
            OPTION,
            contact_id
        FROM
            bank_transactions
    {% endif %}
    {% if var('xero__using_credit_note',True) %}
        UNION ALL
        SELECT
            credit_note_id AS source_id,
            amount,
            currency_code,
            OPTION,
            contact_id
        FROM
            credit_notes
    {% endif %}
    UNION ALL
    select 
        payment_id,
        coalesce(bank_amount, amount) as amount,
        a.currency_code as currency_code,
        CAST(
            NULL AS STRING
        ) AS OPTION,
        CAST(
            NULL AS STRING
        ) AS contact_id,
    from payments as p
    left join accounts as a on 
        p.account_id = a.account_id
),
{% if var('xero__using_bank_transfer',True) %}
    bank_transfers_raw_amounts_pre as (
        SELECT
            bank_transfer_id AS source_id,
            from_amount as raw_amount,
            from_currency_code as currency_code,
            OPTION,
            contact_id
        FROM
            bank_transfers
        UNION ALL
        SELECT
            bank_transfer_id AS source_id,
            to_amount as raw_amount,
            to_currency_code as currency_code,
            OPTION,
            contact_id
        FROM
            bank_transfers
    ),
    bank_transfers_raw_amounts as (
        select *
        from bank_transfers_raw_amounts_pre
        {{ dbt_utils.group_by(5) }}
    ),
{% endif %}
enriched_journal AS (
    SELECT
        journals.journal_id,
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
        accounts.account_class,
        accounts.account_system_account,
        accounts.account_status,
        accounts.currency_code AS account_currency_code,
        accounts.description AS account_description,
        journal_lines.description,
        journal_lines.option,
        journal_lines.gross_amount,
        journal_lines.net_amount,
        journal_lines.tax_amount,
        journal_lines.tax_name,
        journal_lines.tax_type
    FROM
        journals
        LEFT JOIN journal_lines
        ON (
            journals.journal_id = journal_lines.journal_id
        )
        LEFT JOIN accounts
        ON (
            accounts.account_id = journal_lines.account_id
        )
),
first_contact AS (
    SELECT
        enriched_journal.journal_id,
        enriched_journal.journal_date,
        DATE_TRUNC(
            enriched_journal.journal_date,
            MONTH
        ) AS MONTH,
        enriched_journal.journal_number,
        enriched_journal.reference,
        enriched_journal.source_id,
        enriched_journal.source_type,
        CASE
            WHEN enriched_journal.source_type = "ACCREC" THEN "Receivable Invoice"
            WHEN enriched_journal.source_type = "ACCPAY" THEN "Payable Invoice"
            WHEN enriched_journal.source_type = "ACCRECCREDIT" THEN "Receivable Credit Note"
            WHEN enriched_journal.source_type = "ACCPAYCREDIT" THEN "Payable Credit Note"
            WHEN enriched_journal.source_type = "ACCRECPAYMENT" THEN "Receivable Payment"
            WHEN enriched_journal.source_type = "ACCPAYPAYMENT" THEN "Payable Payment"
            WHEN enriched_journal.source_type = "ARCREDITPAYMENT" THEN "Receivable Credit Note Payment"
            WHEN enriched_journal.source_type = "APCREDITPAYMENT" THEN "Payable Credit Note Payment"
            WHEN enriched_journal.source_type = "CASHREC" THEN "Receive Money"
            WHEN enriched_journal.source_type = "CASHPAID" THEN "Spend Money"
            WHEN enriched_journal.source_type = "TRANSFER" THEN "Bank Transfer"
            WHEN enriched_journal.source_type = "ARPREPAYMENT" THEN "Receivable Prepayment"
            WHEN enriched_journal.source_type = "APPREPAYMENT" THEN "Payable Prepayment"
            WHEN enriched_journal.source_type = "AROVERPAYMENT" THEN "Receivable Overpayment"
            WHEN enriched_journal.source_type = "APOVERPAYMENT" THEN "Payable Overpayment"
            WHEN enriched_journal.source_type = "EXPCLAIM" THEN "Expense Claim"
            WHEN enriched_journal.source_type = "EXPPAYMENT" THEN "Expense Claim Payment"
            WHEN enriched_journal.source_type = "MANJOURNAL" THEN "Manual Journal"
            WHEN enriched_journal.source_type = "PAYSLIP" THEN "Payslip"
            WHEN enriched_journal.source_type = "WAGEPAYABLE" THEN "Payroll Payable"
            WHEN enriched_journal.source_type = "INTEGRATEDPAYROLLPE" THEN "Payroll Expense"
            WHEN enriched_journal.source_type = "INTEGRATEDPAYROLLPT" THEN "Payroll Payment"
            WHEN enriched_journal.source_type = "EXTERNALSPENDMONEY" THEN "Payroll Employee Payment"
            WHEN enriched_journal.source_type = "INTEGRATEDPAYROLLPTPAYMENT" THEN "Payroll Tax Payment"
            WHEN enriched_journal.source_type = "INTEGRATEDPAYROLLCN" THEN "Payroll Credit Note"
            WHEN enriched_journal.source_type IS NULL THEN "Conversion Balance Journal"
        END AS source_type_category,
        enriched_journal.source_relation,
        enriched_journal.journal_line_id,
        enriched_journal.account_code,
        enriched_journal.account_id,
        enriched_journal.account_name,
        enriched_journal.account_type,
        enriched_journal.account_class,
        enriched_journal.account_system_account,
        enriched_journal.account_status,
        enriched_journal.account_currency_code,
        enriched_journal.account_description,
        enriched_journal.description,
        enriched_journal.option,
        enriched_journal.gross_amount,
        enriched_journal.net_amount,
        enriched_journal.tax_amount,
        enriched_journal.tax_name,
        enriched_journal.tax_type
    FROM
        enriched_journal
),
net_base_currency AS (
    SELECT
        journal_id,
        journal_date,
        MONTH,
        journal_number,
        REFERENCE,
        source_id,
        source_type,
        source_type_category,
        source_relation,
        journal_line_id,
        account_code,
        account_id,
        account_name,
        account_type,
        account_class,
        account_system_account,
        account_status,
        COALESCE(
            -- Instead of base_currency for system accounts -> grab account_id of the source transaction
            account_currency_code,
            o.base_currency
        ) AS account_currency_code,
        base_currency,
        account_description,
        description,
        OPTION,
        net_amount AS base_currency_net_amount,
        SUM(net_amount) over (
            PARTITION BY source_id,
            account_code
        ) AS base_currency_total_net_amount
    FROM
        first_contact AS fc
        CROSS JOIN ORGANIZATION AS o
),
raw_net_amount AS (
    SELECT
        nb.*,
        C.contact_name,
        coalesce(abs(ra.amount)
            {% if var('xero__using_bank_transfer',True) %}
                ,abs(btr.raw_amount)
            {% endif %}
        ) * -- Sign change
        COALESCE(
            safe_divide(
                base_currency_net_amount,
                ABS(base_currency_net_amount)
            ),
            1
        ) * -- Proportion with the multiple journal lines per source_id
        COALESCE(
            safe_divide(ABS(base_currency_net_amount), ABS(base_currency_total_net_amount)), 1) AS raw_amount,
        COALESCE(
            ra.currency_code,
            account_currency_code
        ) AS currency_code
    FROM
        net_base_currency nb
    LEFT JOIN raw_amounts AS ra
        ON nb.source_id = ra.source_id
    {% if var('xero__using_bank_transfer',True) %}
        left join bank_transfers_raw_amounts btr on 
            nb.source_id = btr.source_id and 
            nb.account_currency_code = btr.currency_code
    {% endif %}
    LEFT JOIN contacts AS C -- Relationship is 1 - 1 in this case
        ON ra.contact_id = C.contact_id
),
summary AS (
    SELECT
        journal_id,
        journal_date,
        MONTH,
        journal_number,
        REFERENCE,
        source_id,
        source_type,
        source_type_category,
        source_relation,
        journal_line_id,
        account_code,
        account_id,
        account_name,
        account_type,
        account_class,
        account_system_account,
        account_currency_code,
        base_currency,
        account_description,
        description,
        OPTION,
        contact_name,
        base_currency_net_amount AS net_amount,
        base_currency_total_net_amount,
        currency_code,
        raw_amount,
        CASE
            WHEN account_currency_code = base_currency and account_system_account is null
                    THEN base_currency_net_amount
            when currency_code = account_currency_code and account_system_account is not null
                    THEN base_currency_net_amount
            -- Mejuri
            WHEN currency_code = "CAD" THEN raw_amount * 1
            WHEN currency_code = "GBP" THEN raw_amount * 1.56
            WHEN currency_code = "USD" THEN raw_amount * 1.28926
            WHEN currency_code = "SEK" THEN raw_amount * 0.12
            WHEN currency_code = "AUD" THEN raw_amount * 0.89
            WHEN currency_code = "CHF" THEN raw_amount * 1.32
            WHEN currency_code = "EUR" THEN raw_amount * 1.32
            WHEN currency_code = "JPY" THEN raw_amount * 0.0095
            WHEN currency_code = "HKD" THEN raw_amount * 0.16
            -- 
            -- Agriwebb
            -- WHEN currency_code = "GBP" THEN raw_amount * 1.758779829
            -- WHEN currency_code = "USD" THEN raw_amount * 1.37927991 
            -- WHEN currency_code = "AUD" THEN raw_amount * 1
            -- WHEN currency_code = "EUR" THEN raw_amount * 1.53
            -- ELSE base_currency_net_amount
        END AS net_revalued
    FROM
        raw_net_amount -- TODO: match raw amount currency code and transform to today's currency rate
        -- Get Xe.com's currency rate
),
sign_change AS (
    SELECT
        *
    EXCEPT(net_revalued),
        CASE
            WHEN UPPER(account_class) IN (
                "EQUITY",
                "LIABILITY"
            ) THEN CASE
                WHEN source_type_category = "Payable Credit Note" THEN -1 * net_revalued
                WHEN source_type_category = "Payable Credit Note Payment" THEN -1 * net_revalued
                -- Changed 
                WHEN source_type_category = "Payable Overpayment" THEN 1 * net_revalued
                WHEN source_type_category = "Payable Payment" THEN -1 * net_revalued
                WHEN source_type_category = "Payable Invoice" THEN -1 * net_revalued
                WHEN source_type_category = "Receivable Invoice" THEN -1 * net_revalued
                WHEN source_type_category = "Receivable Credit Note" THEN 1 * net_revalued
                WHEN source_type_category = "Receivable Payment" THEN -1 * net_revalued
                WHEN source_type_category = "Receivable Credit Note Payment" THEN 1 * net_revalued
                WHEN source_type_category = "Receive Money" THEN -1 * net_revalued
                WHEN source_type_category = "Spend Money" THEN -1 * net_revalued
                WHEN source_type_category = "Bank Transfer" THEN -1 * net_revalued
                WHEN source_type_category = "Receivable Prepayment" THEN 1 * net_revalued
                WHEN source_type_category = "Payable Prepayment" THEN 1 * net_revalued
                WHEN source_type_category = "Receivable Overpayment" THEN 1 * net_revalued 
                WHEN source_type_category = "Expense Claim" THEN 1 * net_revalued
                WHEN source_type_category = "Expense Claim Payment" THEN 1 * net_revalued
                WHEN source_type_category = "Manual Journal" THEN -1 * net_revalued
                WHEN source_type_category = "Payslip" THEN 1 * net_revalued
                WHEN source_type_category = "Payroll Payable" THEN 1 * net_revalued
                WHEN source_type_category = "Payroll Expense" THEN 1 * net_revalued
                WHEN source_type_category = "Payroll Payment" THEN 1 * net_revalued
                WHEN source_type_category = "Payroll Employee Payment" THEN 1 * net_revalued
                WHEN source_type_category = "Payroll Tax Payment" THEN 1 * net_revalued
                WHEN source_type_category = "Payroll Credit Note" THEN 1 * net_revalued
                WHEN source_type_category = "Conversion Balance Journal" THEN -1 * net_revalued
                WHEN source_type_category = "End of Period" THEN 1 * net_revalued
                ELSE net_revalued
            END
            ELSE net_revalued
        END AS net_revalued
    FROM
        summary
)
    SELECT
        *
    FROM
        sign_change
    -- There are some conversion balances added manually, addint them here for now
