{% macro delete_duplicates_invoice_summary(schema, table_name, temp_table_name) %}

-- Create a temporary table with distinct values and row numbers
CREATE TEMPORARY TABLE {{ temp_table_name }} AS
SELECT 
    _FILE,
    _LINE,
    _MODIFIED,
    TYPE,
    INVOICE_DATE,
    INVOICE_NUMBER,
    ORDER_NUMBER,
    CUSTOMER_NAME,
    PART_TOTAL,
    LABOR_TOTAL,
    SUBLET_TOTAL,
    FEE_TOTAL,
    SALES_TAX_TOTAL,
    TOTAL,
    OWNER,
    STATUS,
    CREATED,
    MODIFIED,
    UNIT_TOTAL,
    TRADE_IN_TOTAL,
    WARRANTY_PRODUCT_TOTAL,
    OTHER_PRODUCT_TOTAL,
    INVOICE_PROFIT,
    DEAL_PROFIT,
    TAXABLE_TOTAL,
    NON_TAXABLE_TOTAL,
    SALES_COMMISSION,
    SALESPERSON,
    ACCOUNT_TYPE,
    LIEN_PAYOUT,
    FINANCE_COMMISSION,
    _FIVETRAN_SYNCED,
    ROW_NUMBER() OVER (PARTITION BY INVOICE_NUMBER, INVOICE_DATE ORDER BY _MODIFIED DESC) AS row_num
FROM {{ source(schema, table_name) }};

-- Truncate the original table
TRUNCATE TABLE {{ source(schema, table_name) }};

-- Insert the records from the temporary table back into the main table, selecting only the most recent rows
INSERT INTO {{ source(schema, table_name) }} 
        (_FILE,
    _LINE,
    _MODIFIED,
    TYPE,
    INVOICE_DATE,
    INVOICE_NUMBER,
    ORDER_NUMBER,
    CUSTOMER_NAME,
    PART_TOTAL,
    LABOR_TOTAL,
    SUBLET_TOTAL,
    FEE_TOTAL,
    SALES_TAX_TOTAL,
    TOTAL,
    OWNER,
    STATUS,
    CREATED,
    MODIFIED,
    UNIT_TOTAL,
    TRADE_IN_TOTAL,
    WARRANTY_PRODUCT_TOTAL,
    OTHER_PRODUCT_TOTAL,
    INVOICE_PROFIT,
    DEAL_PROFIT,
    TAXABLE_TOTAL,
    NON_TAXABLE_TOTAL,
    SALES_COMMISSION,
    SALESPERSON,
    ACCOUNT_TYPE,
    LIEN_PAYOUT,
    FINANCE_COMMISSION,
    _FIVETRAN_SYNCED)

SELECT 
    _FILE,
    _LINE,
    _MODIFIED,
    TYPE,
    INVOICE_DATE,
    INVOICE_NUMBER,
    ORDER_NUMBER,
    CUSTOMER_NAME,
    PART_TOTAL,
    LABOR_TOTAL,
    SUBLET_TOTAL,
    FEE_TOTAL,
    SALES_TAX_TOTAL,
    TOTAL,
    OWNER,
    STATUS,
    CREATED,
    MODIFIED,
    UNIT_TOTAL,
    TRADE_IN_TOTAL,
    WARRANTY_PRODUCT_TOTAL,
    OTHER_PRODUCT_TOTAL,
    INVOICE_PROFIT,
    DEAL_PROFIT,
    TAXABLE_TOTAL,
    NON_TAXABLE_TOTAL,
    SALES_COMMISSION,
    SALESPERSON,
    ACCOUNT_TYPE,
    LIEN_PAYOUT,
    FINANCE_COMMISSION,
    _FIVETRAN_SYNCED
FROM {{ temp_table_name }}
WHERE row_num = 1;

-- Drop the temporary table
DROP TABLE {{ temp_table_name }};

{% endmacro %}