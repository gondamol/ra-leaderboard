/* INTERVIEW DUE STATUS TRACKER
=============================================================================
Purpose: 
  Identifies households that are due or overdue for an interview based on 
  a 14-day cycle.

Logic:
  1. Finds the most recent interview date for every active household.
  2. Filters out households marked as "Dropped" (h.out = 1) or "Test".
  3. Calculates days elapsed between the last interview and today (Dynamic).
  4. Flags status:
     - "Overdue (Late)": >14 days since last interview.
     - "Due Today": Exactly 14 days.
     - "Upcoming (Soon)": 11-13 days (Warning zone).

Output:
  - Household Code, RA Name, Last Date, Due Date, Status Flag.
  - Sorted by Priority (Overdue -> Due -> Upcoming).
=============================================================================
*/


WITH LastInterviews AS (
    SELECT 
        h.id AS household_id,
        h.name AS household_code,
        MAX(i.interview_start_date) AS last_interview_date
    FROM households h
    JOIN interviews i ON h.id = i.household_id
    WHERE 
        h.status = 1 
        AND i.status = 1
        AND h.`out` = 0 
        
        -- Standard Exclusion Filters
        AND h.name NOT LIKE '%KTEST%' 
        AND h.name NOT LIKE '%Test%' 
        AND h.name NOT LIKE '%test%'
        AND h.name NOT LIKE '%TEAM%'
        AND h.name NOT LIKE '%TEST%'
    GROUP BY h.id, h.name
)

SELECT 
    li.household_code,
    -- 1. Get the RA Name here
    COALESCE(u.username, 'Unknown') AS ra_name,
    DATE(li.last_interview_date) AS last_interview_date,
    DATE(CURDATE()) AS today_date,
    
    DATEDIFF(CURDATE(), li.last_interview_date) AS days_elapsed,
    DATE_ADD(DATE(li.last_interview_date), INTERVAL 14 DAY) AS next_due_date,

    CASE 
        WHEN DATEDIFF(CURDATE(), li.last_interview_date) > 14 THEN 'Overdue (Late)'
        WHEN DATEDIFF(CURDATE(), li.last_interview_date) = 14 THEN 'Due Today'
        WHEN DATEDIFF(CURDATE(), li.last_interview_date) BETWEEN 11 AND 13 THEN 'Upcoming (Soon)'
        ELSE 'On Track'
    END AS status_flag,

    CASE 
        WHEN DATEDIFF(CURDATE(), li.last_interview_date) > 14 THEN 1
        WHEN DATEDIFF(CURDATE(), li.last_interview_date) = 14 THEN 2
        WHEN DATEDIFF(CURDATE(), li.last_interview_date) BETWEEN 11 AND 13 THEN 3
        ELSE 4
    END AS priority_sort

FROM LastInterviews li

-- 2. Join back to the interviews table to get the full record for that specific date
JOIN interviews i ON li.household_id = i.household_id 
                  AND li.last_interview_date = i.interview_start_date

-- 3. Join to core_users to translate the ID to a name
LEFT JOIN core_users u ON i.interviewer_id = u.id

WHERE DATEDIFF(CURDATE(), li.last_interview_date) >= 11

ORDER BY priority_sort ASC, household_code, days_elapsed DESC;