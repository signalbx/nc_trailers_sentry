-- tests if there are any date entry errors by the VA
SELECT *
FROM {{ source('adp_prod', 'time_cards') }}
WHERE 
  (try_to_date(SUBSTR(_FILE, POSITION('ADP_TIMECARD_' IN UPPER(_FILE)) + LENGTH('ADP_TIMECARD_'), 8), 'YYYYMMDD') != 
    CASE 
      WHEN EXTRACT(DAY FROM CURRENT_DATE) >= 16 THEN DATE_TRUNC('MONTH', CURRENT_DATE) + INTERVAL '15 DAYS' -- If it's after the 16th, the current pay period started on the 16th of this month
      ELSE DATE_TRUNC('MONTH', CURRENT_DATE) -- If it's before the 16th, the current pay period started on the 1st of this month
    END) 
  AND 
  (try_to_date(SUBSTR(_FILE, POSITION('ADP_TIMECARD_' IN UPPER(_FILE)) + LENGTH('ADP_TIMECARD_'), 8), 'YYYYMMDD') !=
    CASE 
      WHEN EXTRACT(DAY FROM CURRENT_DATE) >= 16 THEN DATE_TRUNC('MONTH', CURRENT_DATE) -- If it's after the 16th, the last pay period started on the 1st of this month
      ELSE DATE_TRUNC('MONTH', CURRENT_DATE) - INTERVAL '1 MONTH' + INTERVAL '15 DAYS' -- If it's before the 16th, the last pay period started on the 16th of the last month
    END)
