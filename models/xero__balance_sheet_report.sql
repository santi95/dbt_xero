WITH calendar AS (
    SELECT
        *
    FROM
        {{ ref('xero__calendar_spine') }}
),
ledger AS (
    SELECT
        *
    FROM
        {{ ref('xero__general_ledger') }}
),
ORGANIZATION AS (
    SELECT
        *
    FROM
        {{ var('organization') }}
),
year_end AS (
    SELECT
        CASE
            WHEN CAST(
                EXTRACT(
                    YEAR
                    FROM
                        CURRENT_DATE
                ) || '-' || financial_year_end_month || '-' || financial_year_end_day AS DATE
            ) >= CURRENT_DATE THEN CAST(
                EXTRACT(
                    YEAR
                    FROM
                        CURRENT_DATE
                ) || '-' || financial_year_end_month || '-' || financial_year_end_day AS DATE
            )
            ELSE CAST(
                EXTRACT(
                    YEAR
                    FROM
                        {{ dbt_utils.dateadd(
                            'year',
                            -1,
                            'current_date'
                        ) }}
                ) || '-' || financial_year_end_month || '-' || financial_year_end_day AS DATE
            )
        END AS current_year_end_date
    FROM
        ORGANIZATION
),
joined AS (
    SELECT
        calendar.date_month,
        CASE
            WHEN ledger.account_class IN (
                'ASSET',
                'EQUITY',
                'LIABILITY'
            ) THEN ledger.account_name
            WHEN ledger.journal_date <= {{ dbt_utils.dateadd(
                'year',
                -1,
                'year_end.current_year_end_date'
            ) }} THEN 'Retained Earnings'
            ELSE 'Current Year Earnings'
        END AS account_name,
        CASE
            WHEN ledger.account_class IN (
                'ASSET',
                'EQUITY',
                'LIABILITY'
            ) THEN ledger.account_code
            ELSE NULL
        END AS account_code,
        CASE
            WHEN ledger.account_class IN (
                'ASSET',
                'EQUITY',
                'LIABILITY'
            ) THEN ledger.account_id
            ELSE NULL
        END AS account_id,
        CASE
            WHEN ledger.account_class IN (
                'ASSET',
                'EQUITY',
                'LIABILITY'
            ) THEN ledger.account_type
            ELSE NULL
        END AS account_type,
        CASE
            WHEN ledger.account_class IN (
                'ASSET',
                'EQUITY',
                'LIABILITY'
            ) THEN ledger.account_class
            ELSE 'EQUITY'
        END AS account_class,
        ledger.account_currency_code,
        ledger.account_description,
        ledger.source_id,
        ledger.journal_date,
        ledger.currency_code,
        ledger.description,
        ledger.reference,
        ledger.source_relation,
        -- New Stuff
        source_type,
        source_type_category,
        ledger.net_revalued net_amount
    FROM
        calendar
        INNER JOIN ledger
        ON calendar.date_month >= CAST(
            {{ dbt_utils.date_trunc(
                'month',
                'ledger.journal_date'
            ) }} AS DATE
        )
        CROSS JOIN year_end
)
SELECT
    *
FROM
    joined
WHERE
    net_amount != 0 and account_code is not null
