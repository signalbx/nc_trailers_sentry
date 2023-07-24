-- tests if there are any date entry errors by the VA
SELECT *, COALESCE(TRY_TO_DATE(DATE, 'MM/DD/YYYY'), 
                TRY_TO_DATE(DATE, 'M/D/YYYY'), 
                TRY_TO_DATE(DATE, 'MM-DD-YYYY'), 
                TRY_TO_DATE(DATE, 'M-D-YYYY'), 
                TRY_TO_DATE(DATE, 'YYYY-MM-DD'),
                TRY_TO_DATE(DATE, 'MM/DD/YYYY HH24:MI'),
                TRY_TO_DATE(DATE, 'MM/DD/YYYY HH12:MI AM')) 
        AS formatted_date
FROM {{ source('phone_ninjas_prod', 'phone_scores') }}
WHERE DATE(formatted_date) > DATE(CURRENT_DATE) OR DATE(formatted_date) < DATE(CURRENT_DATE - INTERVAL '2 DAY')