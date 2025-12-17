/* ============================================================
   MONTHLY COMPLETION CHECKS FOR DIARIES 
   Members to filter out from well-being:
     - KTRAN12 - member_id = 363175609956200000 
   Notes:
     - Few cashflows check excludes HHs: KMURR18, KMURR10, KMURR12, KMURI30, KMURI35, KKWAL48
   
   Date Parameters:
     - @start_date: First day of month
     - @end_date: Last day of month
   ============================================================ */


WITH 
-- ------------------------------------------------------------
-- Date window (defaults to last 30 days; override with @start_date/@end_date)
-- ------------------------------------------------------------
date_window AS (
    SELECT
        CASE
            WHEN @start_date IS NULL OR @start_date = '' THEN CURRENT_DATE() - INTERVAL 30 DAY
            ELSE CAST(@start_date AS DATE)
        END AS start_date,
        CASE
            WHEN @end_date IS NULL OR @end_date = '' THEN CURRENT_DATE() - INTERVAL 1 DAY
            ELSE CAST(@end_date AS DATE)
        END AS end_date
),

-- ------------------------------------------------------------
-- Base interviews to check
-- ------------------------------------------------------------
base_interviews AS (
    SELECT DISTINCT
        i.id AS interview_id,
        h.id AS hh_id,
        h.name AS household_code,
        i.interview_start_date,
        cu.username AS interviewer_name
    FROM interviews i
    JOIN households h ON h.id = i.household_id
    LEFT JOIN core_users cu ON cu.id = i.interviewer_id
    CROSS JOIN date_window dw
    WHERE h.project_id = 129
      AND h.status = 1
      AND h.out = 0             -- exclude households that are out
      AND i.status = 1
      AND i.interviewer_id IS NOT NULL  -- must have an interviewer
      AND DATE(i.interview_start_date) BETWEEN dw.start_date AND dw.end_date
      AND h.name NOT LIKE '%test%'
      AND h.name NOT LIKE '%TEST%'
      AND h.name NOT LIKE '%KTEST%'
),

-- ------------------------------------------------------------
-- 1. GOINGS-ON CHECK
-- ------------------------------------------------------------
goingson_check AS (
    WITH goq AS (
        SELECT 595743 AS question_id, 'School fees: child sent home' AS label
        UNION ALL SELECT 590748, 'Foregone care: needed doctor/medicine'
        UNION ALL SELECT 591918, 'Hunger: slept hungry'
        UNION ALL SELECT 590742, 'Police/Askaris problem'
        UNION ALL SELECT 590754, 'Unsafe in neighbourhood'
        UNION ALL SELECT 590760, 'Disconnection: water/electricity'
        UNION ALL SELECT 590766, 'Assets seized / lockout / eviction'
        UNION ALL SELECT 604026, 'Debt consequence'
        UNION ALL SELECT 590772, 'Missed appointment'
        UNION ALL SELECT 590778, 'Expected income did not come'
        UNION ALL SELECT 604032, 'Big fight/argument'
        UNION ALL SELECT 604227, 'New health issue'
    )
    SELECT 
        i.interview_id,
        CASE 
            WHEN COUNT(
                    DISTINCT CASE 
                        WHEN vti.value IS NOT NULL
                             AND NOT (gq.question_id = 604227 AND DATE(i.interview_start_date) < '2025-12-01')
                        THEN a.question_id 
                    END
                 ) < CASE 
                        WHEN DATE(i.interview_start_date) >= '2025-12-01' THEN 11 
                        ELSE 10 
                      END
            THEN 1 ELSE 0 
        END AS goingson_flag,
        GROUP_CONCAT(
            CASE 
                WHEN vti.value IS NULL
                     AND NOT (gq.question_id = 604227 AND DATE(i.interview_start_date) < '2025-12-01')
                THEN gq.label
            END 
            SEPARATOR '; '
        ) AS goingson_detail
    FROM base_interviews i
    CROSS JOIN goq gq
    LEFT JOIN answers a 
        ON a.interview_id = i.interview_id
       AND a.question_id = gq.question_id
    LEFT JOIN values_tinyint vti 
        ON a.history_id = vti.history_id
    GROUP BY i.interview_id
),

-- ------------------------------------------------------------
-- 2. WELL-BEING CHECK (member-level, includes non-roster adults)
-- ------------------------------------------------------------
wellbeing_member_check AS (
    SELECT 
        i.interview_id,
        m.id AS member_id,
        m.person_code,
        m.name AS member_name,
        SUM(CASE WHEN a.question_id = 590727 THEN 1 ELSE 0 END) AS has_q1,
        SUM(CASE WHEN a.question_id = 590730 THEN 1 ELSE 0 END) AS has_q2,
        SUM(CASE WHEN a.question_id = 590733 THEN 1 ELSE 0 END) AS has_q3,
        SUM(CASE WHEN a.question_id = 590736 THEN 1 ELSE 0 END) AS has_q4
    FROM base_interviews i
    JOIN members m 
        ON m.household_id = i.hh_id
        AND m.status = 1
        AND m.id <> 363175609956200000             -- manual exclusion
        -- adults only at interview date
        AND FLOOR(DATEDIFF(i.interview_start_date, m.birthdate) / 365.25) >= 18
        -- exclude members who are OUT on/before interview date
        AND NOT EXISTS (
            SELECT 1
            FROM status s
            WHERE s.entity_type = 51
              AND s.entity_id   = m.id
              AND s.type        = 2           -- out-of-household
              AND s.status_date <= DATE(i.interview_start_date)
        )
        -- exclude members who joined AFTER the interview date
        AND COALESCE(
                (SELECT MIN(s_join.status_date)
                 FROM status s_join
                 WHERE s_join.entity_id = m.id
                   AND s_join.entity_type = 51),
                DATE('1900-01-01')             -- if no status record, assume in HH
            ) <= DATE(i.interview_start_date)
    LEFT JOIN answers a 
        ON a.interview_id = i.interview_id 
       AND a.question_id IN (590727, 590730, 590733, 590736)
       AND (
            a.member_id = m.id 
            OR (a.entity_id = m.id AND a.entity_type = 51)
       )
    GROUP BY i.interview_id, m.id, m.person_code, m.name
),

wellbeing_check AS (
    SELECT 
        interview_id,
        CASE 
            WHEN COUNT(
                    CASE 
                        WHEN (has_q1 + has_q2 + has_q3 + has_q4) < 4 THEN 1 
                    END
                 ) > 0 
            THEN 1 ELSE 0 
        END AS wellbeing_flag,
        GROUP_CONCAT(
            CASE 
                WHEN (has_q1 + has_q2 + has_q3 + has_q4) < 4 
                THEN CONCAT(member_name, ' (', 
                            4 - (has_q1 + has_q2 + has_q3 + has_q4), 
                            ' well-being question(s) missing)')
            END
            SEPARATOR '; '
        ) AS wellbeing_detail
    FROM wellbeing_member_check
    GROUP BY interview_id
),

-- ------------------------------------------------------------
-- 3. CONSUMPTION (RURAL) CHECK 
-- ------------------------------------------------------------
consumption_check AS (
    SELECT
        i.interview_id,
        CASE
            WHEN vti_consumption.value = 1 
                 AND COUNT(DISTINCT a_consumption.entity_id) = 0 
            THEN 1
            ELSE 0
        END AS consumption_flag,
        CASE
            WHEN vti_consumption.value = 1 
                 AND COUNT(DISTINCT a_consumption.entity_id) = 0
            THEN 'Answered YES to consumption from HH production but no items recorded'
            ELSE ''
        END AS consumption_detail
    FROM base_interviews i
    LEFT JOIN answers a_604236
        ON a_604236.interview_id = i.interview_id
       AND a_604236.question_id = 604236
    LEFT JOIN values_tinyint vti_consumption
        ON a_604236.history_id = vti_consumption.history_id
    LEFT JOIN answers a_consumption
        ON a_consumption.interview_id = i.interview_id
       AND a_consumption.question_id IN (598626, 598629, 598632, 598635, 598731)
    WHERE (
        i.household_code LIKE 'KHOMA%' OR i.household_code LIKE 'KMIGR%' OR
        i.household_code LIKE 'KBUNR%' OR i.household_code LIKE 'KKWAG%' OR
        i.household_code LIKE 'KISIN%' OR i.household_code LIKE 'KISIB%' OR
        i.household_code LIKE 'KTRAN%' OR i.household_code LIKE 'KKWAL%' OR
        i.household_code LIKE 'KMURI%' OR i.household_code LIKE 'KMURR%' 
    )
    GROUP BY i.interview_id, vti_consumption.value
),

-- ------------------------------------------------------------
-- 4. MAJOR EVENTS CHECK (unchanged)
-- ------------------------------------------------------------
majorevents_check AS (
    SELECT 
        i.interview_id,
        CASE 
            WHEN COUNT(DISTINCT CASE WHEN vti.value IS NOT NULL THEN a.question_id END) < 15 
            THEN 1 ELSE 0 
        END AS majorevents_flag,
        CASE 
            WHEN COUNT(DISTINCT CASE WHEN vti.value IS NOT NULL THEN a.question_id END) < 15 
            THEN CONCAT(
                     15 - COUNT(DISTINCT CASE WHEN vti.value IS NOT NULL THEN a.question_id END), 
                     ' questions missing'
                 )
            ELSE ''
        END AS majorevents_detail
    FROM base_interviews i
    LEFT JOIN answers a 
        ON a.interview_id = i.interview_id 
       AND a.question_id IN (
            590793, 590796, 590799, 590802, 590805, 590808, 590814, 
            590820, 590823, 590826, 590829, 590832, 590835, 604056, 604059
       )
    LEFT JOIN values_tinyint vti 
        ON a.history_id = vti.history_id
    GROUP BY i.interview_id
),

-- ------------------------------------------------------------
-- 5. CHANGES SINCE LAST VISIT CHECK (unchanged)
-- ------------------------------------------------------------
change_check AS (
    WITH change_q AS (
        SELECT q.id AS question_id
        FROM core_questions q
        WHERE q.category_id = 59490
          AND q.status = 1
    ),
    change_counts AS (
        SELECT
            i.interview_id,
            (SELECT COUNT(*) FROM change_q) AS total_questions,
            COUNT(DISTINCT a.question_id) AS answered_questions
        FROM base_interviews i
        LEFT JOIN answers a 
            ON a.interview_id = i.interview_id 
           AND a.question_id IN (SELECT question_id FROM change_q)
        GROUP BY i.interview_id
    )
    SELECT
        interview_id,
        CASE 
            WHEN answered_questions < total_questions THEN 1 
            ELSE 0 
        END AS change_flag,
        CASE 
            WHEN answered_questions < total_questions
            THEN CONCAT(total_questions - answered_questions, ' questions missing')
            ELSE ''
        END AS change_detail
    FROM change_counts
),

-- ------------------------------------------------------------
-- 6. HEALTH ISSUE UPDATES CHECK (unchanged)
-- ------------------------------------------------------------
hi_update_check AS (
    WITH preexisting_hi AS (
        SELECT
            i.interview_id,
            ei.id                        AS hi_id,
            COALESCE(ei.name, ei.`desc`) AS hi_name,
            m.name                       AS member_name,
            DATEDIFF(DATE(i.interview_start_date), ei.open_date)
                                        AS days_since_open,
            DATEDIFF(DATE(i.interview_start_date), ei.close_date)
                                        AS days_since_close,
            (
                SELECT MAX(i2.interview_start_date)
                FROM interviews i2
                WHERE i2.household_id = i.hh_id
                  AND i2.status = 1
                  AND i2.interview_start_date < i.interview_start_date
            ) AS prev_interview_date
        FROM base_interviews i
        JOIN entity_items ei
              ON ei.household_id = i.hh_id
             AND ei.status = 1
             AND ei.type = 447              -- Health Issue entities
             AND ei.open_date IS NOT NULL
        LEFT JOIN members m
              ON m.id = ei.member_id
        WHERE 
              (
                  m.id IS NULL
                  OR (
                         m.status = 1
                     AND NOT EXISTS (
                         SELECT 1
                         FROM status s_out
                         WHERE s_out.entity_type = 51
                           AND s_out.entity_id   = m.id
                           AND s_out.type        = 2
                           AND s_out.status_date <= DATE(i.interview_start_date)
                     )
                     AND NOT EXISTS (
                         SELECT 1
                         FROM status s
                         WHERE s.entity_type = 51
                           AND s.entity_id   = m.id
                           AND s.type        = 2
                           AND s.status_date <= DATE(i.interview_start_date)
                     )
                  )
              )
          AND DATEDIFF(DATE(i.interview_start_date), ei.open_date) > 14
          AND (
                ei.close_date IS NULL
                OR DATEDIFF(DATE(i.interview_start_date), ei.close_date) <= 14
              )
          AND NOT (
                (
                    SELECT MAX(i2.interview_start_date)
                    FROM interviews i2
                    WHERE i2.household_id = i.hh_id
                      AND i2.status = 1
                      AND i2.interview_start_date < i.interview_start_date
                ) IS NOT NULL
                AND ei.open_date >= (
                    SELECT MAX(i2.interview_start_date)
                    FROM interviews i2
                    WHERE i2.household_id = i.hh_id
                      AND i2.status = 1
                      AND i2.interview_start_date < i.interview_start_date
                )
                AND ei.close_date IS NOT NULL
                AND DATE(ei.close_date) <= DATE(i.interview_start_date)
          )
    ),
    hi_updates AS (
        SELECT
            a.interview_id,
            a.entity_id              AS hi_id,
            COUNT(DISTINCT a.id)     AS hu_answer_count
        FROM answers a
        WHERE a.entity_type = 60
          AND a.question_id IN (604065, 600726)
        GROUP BY a.interview_id, a.entity_id
    )
    SELECT
        p.interview_id,
        CASE 
            WHEN COUNT(
                    CASE 
                        WHEN COALESCE(u.hu_answer_count, 0) = 0 THEN 1 
                    END
                 ) > 0 
            THEN 1 ELSE 0 
        END AS hi_update_flag,
        GROUP_CONCAT(
            CASE 
                WHEN COALESCE(u.hu_answer_count, 0) = 0 
                THEN CONCAT(
                     p.hi_name, ' (', 
                     p.member_name, ', ', 
                     p.days_since_open, ' days open',
                     CASE 
                         WHEN p.days_since_close IS NOT NULL 
                         THEN CONCAT(', closed ', p.days_since_close, ' days ago')
                         ELSE '' 
                     END, 
                     ')'
                )
            END
            SEPARATOR '; '
        ) AS hi_update_detail
    FROM preexisting_hi p
    LEFT JOIN hi_updates u
           ON u.interview_id = p.interview_id
          AND u.hi_id        = p.hi_id
    GROUP BY p.interview_id
),

-- ------------------------------------------------------------
-- 7. M-PESA BALANCE CHECK (unchanged)
-- ------------------------------------------------------------
mpesa_check AS (
    WITH mpesa_devices AS (
        SELECT DISTINCT
            fd.id AS account_id,
            fd.household_id,
            fd.`desc` AS account_name
        FROM financial_devices fd
        JOIN households h 
          ON h.id = fd.household_id
        LEFT JOIN core_categories cat 
          ON cat.code_id = fd.type
        LEFT JOIN core_categories_lang catl 
          ON catl.category_id = cat.id 
         AND catl.language_id = 1
        WHERE h.project_id = 129
          AND h.status = 1
          AND fd.status = 1
          AND (fd.close_date IS NULL OR fd.close_date > '2025-11-01')
          AND (
                cat.id = 59289
             OR LOWER(catl.`desc`) LIKE '%mobile money%' 
             OR LOWER(fd.`desc`)  LIKE '%mpesa%' 
             OR LOWER(fd.`desc`)  LIKE '%m-pesa%'
          )
    ),
    device_tx AS (
        SELECT
            cf.interview_id,
            cf.account_id,
            COUNT(DISTINCT cf.id) AS tx_count,
            COUNT(DISTINCT CASE WHEN cft.starting_balance = 1 THEN cf.id END) AS bal_count
        FROM cashflows cf
        LEFT JOIN core_cashflow_types cft 
          ON cft.id = cf.type
        WHERE cf.status = 1
        GROUP BY cf.interview_id, cf.account_id
    )
    SELECT
        i.interview_id,
        CASE 
            WHEN COUNT(
                    CASE 
                        WHEN tx.tx_count > 0 AND tx.bal_count = 0 THEN 1 
                    END
                 ) > 0 
            THEN 1 ELSE 0 
        END AS mpesa_flag,
        GROUP_CONCAT(
            CASE 
                WHEN tx.tx_count > 0 AND tx.bal_count = 0 
                THEN CONCAT(md.account_name, ' (', tx.tx_count, ' tx, no balance)')
            END
            SEPARATOR '; '
        ) AS mpesa_detail
    FROM base_interviews i
    LEFT JOIN mpesa_devices md 
           ON md.household_id = i.hh_id
    LEFT JOIN device_tx tx 
           ON tx.interview_id = i.interview_id 
          AND tx.account_id   = md.account_id
    GROUP BY i.interview_id
),

-- ------------------------------------------------------------
-- 8. JOURNAL CHECK (flags missing or short journals; counts all entries)
-- ------------------------------------------------------------
journal_check AS (
    WITH journal_entries AS (
        SELECT
            i.interview_id,
            COUNT(vt.value) AS entry_count,
            SUM(CHAR_LENGTH(COALESCE(vt.value, ''))) AS total_chars
        FROM base_interviews i
        LEFT JOIN core_questions q 
               ON q.category_id = 59151 
              AND q.status = 1
        LEFT JOIN answers a 
               ON a.interview_id = i.interview_id 
              AND a.question_id  = q.id
        LEFT JOIN values_text vt 
               ON a.history_id = vt.history_id
        GROUP BY i.interview_id
    )
    SELECT
        je.interview_id,
        CASE 
            WHEN COALESCE(je.entry_count, 0) = 0 THEN 1
            WHEN COALESCE(je.total_chars, 0) < 250 THEN 1
            ELSE 0
        END AS journal_flag,
        CASE 
            WHEN COALESCE(je.entry_count, 0) = 0 
                THEN 'No journal entries'
            WHEN COALESCE(je.total_chars, 0) < 250 
                THEN CONCAT('Journal length: ', COALESCE(je.total_chars, 0), ' chars (min 250)')
            ELSE ''
        END AS journal_detail
    FROM journal_entries je
),

-- ------------------------------------------------------------
-- 9. CASHFLOW MISSING CHECK (unchanged)
-- ------------------------------------------------------------
cashflow_missing_check AS (
    SELECT
        i.interview_id,
        CASE 
            WHEN COUNT(cf.id) = 0 THEN 1 
            ELSE 0 
        END AS cashflow_missing_flag,
        CASE 
            WHEN COUNT(cf.id) = 0 THEN 'No cashflows recorded' 
            ELSE '' 
        END AS cashflow_missing_detail
    FROM base_interviews i
    LEFT JOIN cashflows cf 
           ON cf.interview_id = i.interview_id 
          AND cf.status      = 1
    GROUP BY i.interview_id
),

-- ------------------------------------------------------------
-- 10. FEW CASHFLOWS CHECK (unchanged)
-- ------------------------------------------------------------
cashflow_few_check AS (
    SELECT
        i.interview_id,
        CASE 
            WHEN i.household_code IN ('KMURR18', 'KMURR10', 'KMURR12', 'KMURI30', 'KMURI35', 'KKWAL48') THEN 0
            WHEN COUNT(cf.id) > 0 AND COUNT(cf.id) < 20 THEN 1 
            ELSE 0 
        END AS cashflowfew_flag,
        CASE 
            WHEN i.household_code IN ('KMURR18', 'KMURR10', 'KMURR12', 'KMURI30', 'KMURI35', 'KKWAL48') THEN ''
            WHEN COUNT(cf.id) > 0 AND COUNT(cf.id) < 20 
            THEN CONCAT('Only ', COUNT(cf.id), ' cashflows recorded')
            ELSE ''
        END AS cashflowfew_detail
    FROM base_interviews i
    LEFT JOIN cashflows cf 
           ON cf.interview_id = i.interview_id 
          AND cf.status      = 1
    GROUP BY i.interview_id
),

-- ------------------------------------------------------------
-- 11. NO FOOD PURCHASE CHECK (unchanged)
-- ------------------------------------------------------------
nofood_check AS (
    SELECT
        i.interview_id,
        CASE 
            WHEN COUNT(DISTINCT cf.id) = 0 THEN 1 
            ELSE 0 
        END AS nofood_flag,
        CASE 
            WHEN COUNT(DISTINCT cf.id) = 0 
            THEN 'No food purchases recorded'
            ELSE ''
        END AS nofood_detail
    FROM base_interviews i
    LEFT JOIN cashflows cf 
           ON cf.interview_id = i.interview_id 
          AND cf.status      = 1
    LEFT JOIN core_cashflow_types cft 
           ON cft.id     = cf.type 
          AND cft.status = 1
    LEFT JOIN core_categories cat 
           ON cat.id = cft.category_id
          AND cat.id IN (58650, 58653, 58656, 59529)   -- food categories
    GROUP BY i.interview_id
),

-- ------------------------------------------------------------
-- 12. PRIMARY MEMBER NAME PER INTERVIEW (unchanged)
-- ------------------------------------------------------------
primary_member AS (
    SELECT
        bi.interview_id,
        m.name AS member_name,
        ROW_NUMBER() OVER (
            PARTITION BY bi.interview_id
            ORDER BY m.person_code
        ) AS rn
    FROM base_interviews bi
    JOIN members m
      ON m.household_id = bi.hh_id
     AND m.status = 1
     AND NOT EXISTS (
            SELECT 1
            FROM status s
            WHERE s.entity_type = 51
              AND s.entity_id   = m.id
              AND s.type        = 2
              AND s.status_date <= DATE(bi.interview_start_date)
        )
)

-- ============================================================
-- FINAL OUTPUT
-- ============================================================
SELECT 
    bi.household_code              AS 'Household Code',
    pm.member_name                 AS 'Member Name',
    bi.interviewer_name            AS 'Interviewer',
    DATE(bi.interview_start_date)  AS 'Interview Date',
    ''                             AS 'RA Comments',
    
    (COALESCE(go.goingson_flag,         0) +
     COALESCE(wb.wellbeing_flag,        0) + 
     COALESCE(cs.consumption_flag,      0) + 
     COALESCE(me.majorevents_flag,      0) +
     COALESCE(ch.change_flag,           0) + 
     COALESCE(hi.hi_update_flag,        0) +
     COALESCE(mp.mpesa_flag,            0) + 
     COALESCE(jo.journal_flag,          0) +
     COALESCE(cm.cashflow_missing_flag, 0) + 
     COALESCE(cf.cashflowfew_flag,      0) +
     COALESCE(nf.nofood_flag,           0)
    ) AS 'Total Issues',
    
    CASE 
        WHEN (COALESCE(go.goingson_flag,         0) +
              COALESCE(wb.wellbeing_flag,        0) + 
              COALESCE(cs.consumption_flag,      0) + 
              COALESCE(me.majorevents_flag,      0) +
              COALESCE(ch.change_flag,           0) + 
              COALESCE(hi.hi_update_flag,        0) +
              COALESCE(mp.mpesa_flag,            0) + 
              COALESCE(jo.journal_flag,          0) +
              COALESCE(cm.cashflow_missing_flag, 0) + 
              COALESCE(cf.cashflowfew_flag,      0) +
              COALESCE(nf.nofood_flag,           0)
             ) = 0 
        THEN 'Complete' 
        ELSE 'Issues Found' 
    END AS 'Status',
    
    COALESCE(go.goingson_flag,         0) AS 'Goings-on',
    COALESCE(wb.wellbeing_flag,        0) AS 'Well-being',
    COALESCE(cs.consumption_flag,      0) AS 'Consumption (Rural)',
    COALESCE(me.majorevents_flag,      0) AS 'Major Events',
    COALESCE(ch.change_flag,           0) AS 'Changes since last visit',
    COALESCE(hi.hi_update_flag,        0) AS 'Health Updates',
    COALESCE(mp.mpesa_flag,            0) AS 'M-Pesa Balance',
    COALESCE(jo.journal_flag,          0) AS 'Journal',
    COALESCE(cm.cashflow_missing_flag, 0) AS 'Cashflow Missing',
    COALESCE(cf.cashflowfew_flag,      0) AS 'Few Cashflows (<20)',
    COALESCE(nf.nofood_flag,           0) AS 'No Food Purchase',
    
    CONCAT_WS(' | ',
        NULLIF(COALESCE(go.goingson_detail,         ''), ''),
        NULLIF(COALESCE(wb.wellbeing_detail,        ''), ''),
        NULLIF(COALESCE(cs.consumption_detail,      ''), ''),
        NULLIF(COALESCE(me.majorevents_detail,      ''), ''),
        NULLIF(COALESCE(ch.change_detail,           ''), ''),
        NULLIF(COALESCE(hi.hi_update_detail,        ''), ''),
        NULLIF(COALESCE(mp.mpesa_detail,            ''), ''),
        NULLIF(COALESCE(jo.journal_detail,          ''), ''),
        NULLIF(COALESCE(cm.cashflow_missing_detail, ''), ''),
        NULLIF(COALESCE(cf.cashflowfew_detail,      ''), ''),
        NULLIF(COALESCE(nf.nofood_detail,           ''), '')
    ) AS 'Issue Details',
    
    COALESCE(go.goingson_detail,       '') AS goingson_detail,
    COALESCE(wb.wellbeing_detail,      '') AS wellbeing_detail,
    COALESCE(cs.consumption_detail,    '') AS consumption_detail,
    COALESCE(me.majorevents_detail,    '') AS majorevents_detail,
    COALESCE(ch.change_detail,         '') AS change_detail,
    COALESCE(hi.hi_update_detail,      '') AS hi_update_detail,
    COALESCE(mp.mpesa_detail,          '') AS mpesa_detail,
    COALESCE(jo.journal_detail,        '') AS journal_detail,
    COALESCE(nf.nofood_detail,         '') AS nofood_detail

FROM base_interviews bi
LEFT JOIN primary_member        pm ON pm.interview_id = bi.interview_id AND pm.rn = 1
LEFT JOIN goingson_check        go ON go.interview_id = bi.interview_id
LEFT JOIN wellbeing_check       wb ON wb.interview_id = bi.interview_id
LEFT JOIN consumption_check     cs ON cs.interview_id = bi.interview_id
LEFT JOIN majorevents_check     me ON me.interview_id = bi.interview_id
LEFT JOIN change_check          ch ON ch.interview_id = bi.interview_id
LEFT JOIN hi_update_check       hi ON hi.interview_id = bi.interview_id
LEFT JOIN mpesa_check           mp ON mp.interview_id = bi.interview_id
LEFT JOIN journal_check         jo ON jo.interview_id = bi.interview_id
LEFT JOIN cashflow_missing_check cm ON cm.interview_id = bi.interview_id
LEFT JOIN cashflow_few_check    cf ON cf.interview_id = bi.interview_id
LEFT JOIN nofood_check          nf ON nf.interview_id = bi.interview_id

ORDER BY bi.household_code, bi.interview_start_date;
