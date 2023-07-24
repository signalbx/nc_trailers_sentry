-- tests if there are any date entry errors by the VA
SELECT *, COALESCE(TRY_TO_DATE(DATE, 'MM/DD/YYYY'), 
                TRY_TO_DATE(DATE, 'M/D/YYYY'), 
                TRY_TO_DATE(DATE, 'MM-DD-YYYY'), 
                TRY_TO_DATE(DATE, 'M-D-YYYY'), 
                TRY_TO_DATE(DATE, 'YYYY-MM-DD'),
                TRY_TO_DATE(DATE, 'MM/DD/YYYY HH24:MI'),
                TRY_TO_DATE(DATE, 'MM/DD/YYYY HH12:MI AM')) 
        AS formatted_date
FROM {{ source('blackpurl_production', 'technician_performance_stage') }}
WHERE 
  (DATE(formatted_date) != 
    CASE 
      WHEN EXTRACT(DAY FROM CURRENT_DATE) >= 16 THEN DATE_TRUNC('MONTH', CURRENT_DATE) + INTERVAL '15 DAYS' -- If it's after the 16th, the current pay period started on the 16th of this month
      ELSE DATE_TRUNC('MONTH', CURRENT_DATE) -- If it's before the 16th, the current pay period started on the 1st of this month
    END) 
  AND 
  (DATE(formatted_date) !=
    CASE 
      WHEN EXTRACT(DAY FROM CURRENT_DATE) >= 16 THEN DATE_TRUNC('MONTH', CURRENT_DATE) -- If it's after the 16th, the last pay period started on the 1st of this month
      ELSE DATE_TRUNC('MONTH', CURRENT_DATE) - INTERVAL '1 MONTH' + INTERVAL '15 DAYS' -- If it's before the 16th, the last pay period started on the 16th of the last month
    END)
