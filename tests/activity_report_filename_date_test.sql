-- tests if the file name is saved appropriately to include the right date
SELECT *, COALESCE(try_to_date(substr(_FILE, 35, 8), 'YYYYMMDD'),
                try_to_date(substr(_FILE, 35, 8), 'MM/DD/YYYY'), 
                try_to_date(substr(_FILE, 35, 8), 'M/D/YYYY'), 
                try_to_date(substr(_FILE, 35, 8), 'MM-DD-YYYY'), 
                try_to_date(substr(_FILE, 35, 8), 'M-D-YYYY'), 
                try_to_date(substr(_FILE, 35, 8), 'YYYY-MM-DD'),
                try_to_date(substr(_FILE, 35, 8), 'MM/DD/YYYY HH24:MI'),
                try_to_date(substr(_FILE, 35, 8), 'MM/DD/YYYY HH12:MI AM')) 
        AS formatted_date
FROM {{ source('dp360_prod', 'activity_report') }}
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