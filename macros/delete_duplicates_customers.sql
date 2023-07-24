{% macro delete_duplicates_customers(schema, table_name, temp_table_name) %}

-- Create a temporary table with distinct values and row numbers
CREATE TEMPORARY TABLE {{ temp_table_name }} AS
SELECT 
    _FILE,
    _LINE,
    _MODIFIED,
    TYPE,
    CUSTOMER_NUMBER,
    CUSTOMER_NAME,
    PHONE_NUMBER,
    EMAIL,
    OWNER,
    MODIFIED,
    FIRST_NAME,
    LAST_NAME,
    NICKNAME,
    BIRTH_DATE,
    JOB_TITLE,
    BILLING_ADDRESS,
    BILLING_CITY,
    BILLING_STATE,
    SHIPPING_ADDRESS,
    SHIPPING_CITY,
    SHIPPING_STATE,
    MOBILE_NUMBER,
    OTHER_EMAIL,
    WORK_EMAIL,
    PRICE_LEVEL,
    OPEN_ORDERS,
    ORDERS_LIFETIME,
    AVERAGE_SPEND_PER_ORDER,
    CREATED,
    POSTAL_CODE,
    ACCOUNT_TYPE,
    DRIVERS_LICENSE,
    _FIVETRAN_SYNCED,
    STORE_CREDITS,
    ROW_NUMBER() OVER (PARTITION BY CUSTOMER_NUMBER ORDER BY _MODIFIED DESC) AS row_num
FROM {{ source(schema, table_name) }};

-- Truncate the original table
TRUNCATE TABLE {{ source(schema, table_name) }};

-- Insert the records from the temporary table back into the main table, selecting only the most recent rows
INSERT INTO {{ source(schema, table_name) }} 
        (_FILE,
        _LINE,
        _MODIFIED,
        TYPE,
        CUSTOMER_NUMBER,
        CUSTOMER_NAME,
        PHONE_NUMBER,
        EMAIL,
        OWNER,
        MODIFIED,
        FIRST_NAME,
        LAST_NAME,
        NICKNAME,
        BIRTH_DATE,
        JOB_TITLE,
        BILLING_ADDRESS,
        BILLING_CITY,
        BILLING_STATE,
        SHIPPING_ADDRESS,
        SHIPPING_CITY,
        SHIPPING_STATE,
        MOBILE_NUMBER,
        OTHER_EMAIL,
        WORK_EMAIL,
        PRICE_LEVEL,
        OPEN_ORDERS,
        ORDERS_LIFETIME,
        AVERAGE_SPEND_PER_ORDER,
        CREATED,
        POSTAL_CODE,
        ACCOUNT_TYPE,
        DRIVERS_LICENSE,
        _FIVETRAN_SYNCED,
        STORE_CREDITS)

SELECT 
    _FILE,
    _LINE,
    _MODIFIED,
    TYPE,
    CUSTOMER_NUMBER,
    CUSTOMER_NAME,
    PHONE_NUMBER,
    EMAIL,
    OWNER,
    MODIFIED,
    FIRST_NAME,
    LAST_NAME,
    NICKNAME,
    BIRTH_DATE,
    JOB_TITLE,
    BILLING_ADDRESS,
    BILLING_CITY,
    BILLING_STATE,
    SHIPPING_ADDRESS,
    SHIPPING_CITY,
    SHIPPING_STATE,
    MOBILE_NUMBER,
    OTHER_EMAIL,
    WORK_EMAIL,
    PRICE_LEVEL,
    OPEN_ORDERS,
    ORDERS_LIFETIME,
    AVERAGE_SPEND_PER_ORDER,
    CREATED,
    POSTAL_CODE,
    ACCOUNT_TYPE,
    DRIVERS_LICENSE,
    _FIVETRAN_SYNCED,
    STORE_CREDITS
FROM {{ temp_table_name }}
WHERE row_num = 1;

-- Drop the temporary table
DROP TABLE {{ temp_table_name }};

{% endmacro %}