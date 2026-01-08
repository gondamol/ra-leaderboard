/* -----------------------------------------------------------------
  DASHBOARD Monthly Diaries Quality Checks

   INCLUDED CHECKS:
     - CF01 health expense without HI link
     - CF02 sources/uses balance (>5% diff)
     - CF03 in-kind value != 0 (exempt: repayment in kind)
     - CF04 paid-on-behalf = 0
     - CF10 unlinked M-Pesa
     - CF11 unlinked credit
     - CF12 unlinked bank/debit
     - HI01 bought medicine, no medicine cashflow
     - HI02 took medicine, name blank
     - HI03 provider visit yes but form missing
     - HI04 provider form missing HI selection
     - HI06 pregnancy ended but issue not closed
     - OT01 adults without cash-on-hand
     - CF14 in-kind missing description (Q6=588624) or monetary value (Q7=588627)
     - CF16 shop credit not linked to expenditure
     - CF18 cashflow date outside interview period (>21 days old or future)
     - HI08 duplicate pregnancy issues (same member within 90 days)
     - HI09 pregnancy marked as dormant (should never be)

-------------------------------------------------------------------- */

WITH
date_window AS (
  SELECT
    CURRENT_DATE()                                   AS today,
    CASE
     WHEN @end_date IS NULL OR @end_date = '' THEN CURRENT_DATE() - INTERVAL 1 DAY
      ELSE CAST(@end_date AS DATE)
    END                                              AS end_date,
    CASE
      WHEN @start_date IS NULL OR @start_date = '' THEN
        (
          CASE
            WHEN @end_date IS NULL OR @end_date = '' THEN CURRENT_DATE() - INTERVAL 1 DAY
            ELSE CAST(@end_date AS DATE)
          END
        ) - INTERVAL 29 DAY      -- default to a 30-day window (end_date minus 29)
      ELSE CAST(@start_date AS DATE)
    END                                              AS start_date
),
base_interviews AS (
  SELECT
    i.id AS interview_id,
    h.id AS hh_id,
    h.name AS household_code,
    i.interview_start_date,
    DATE(i.interview_start_date) AS interview_date,
    COALESCE(
      cu.username,
      NULLIF(TRIM(CONCAT_WS(' ', ud.name, ud.mname, ud.lname)), '')
    ) AS ra_name
  FROM interviews i
  JOIN households h ON h.id = i.household_id
  LEFT JOIN core_users cu ON cu.id = i.interviewer_id
  LEFT JOIN user_details ud ON ud.user_id = cu.id
  CROSS JOIN date_window dw
  WHERE h.project_id = 129
    AND h.status = 1
    AND h.out = 0
    AND i.status = 1
    AND DATE(i.interview_start_date) BETWEEN dw.start_date AND dw.end_date
    AND h.name NOT LIKE '%test%'
    AND h.name NOT LIKE '%TEST%'
    AND h.name NOT LIKE '%KTEST%'
),

/* ---------------------------------------------------------------
   Primary member (first active household member)
--------------------------------------------------------------- */
primary_member AS (
    SELECT
        bi.interview_id,
        m.id AS member_id,
        m.name AS member_name,
        ROW_NUMBER() OVER (
            PARTITION BY bi.interview_id
            ORDER BY m.person_code
        ) AS rn
    FROM base_interviews bi
    JOIN members m
      ON m.household_id = bi.hh_id
     AND m.status = 1
    WHERE NOT EXISTS (
        SELECT 1
        FROM status s
        WHERE s.entity_type = 51
          AND s.entity_id   = m.id
          AND s.type        = 2           -- out-of-household
          AND s.status_date <= bi.interview_date
    )
),

/* ---------------------------------------------------------------
   Cashflows enriched with labels and modes
--------------------------------------------------------------- */
cashflow_enriched AS (
    SELECT
        cf.id AS cashflow_id,
        cf.interview_id,
        cf.household_id,
        cf.member_id,
        m.name AS member_name,
        cf.value,
        cf.cashflow_date,
        cf.linked_to,
        cf.paid_with_desc,
        cf.quantity AS cf_desc,  -- quantity field holds in-kind descriptions
        CAST(cf.paid_with_desc AS UNSIGNED) AS paid_with_option_id,
        cf.paid_with AS paid_with_code,
        cft.cf_direction,
        cft.report_label,
        cft.starting_balance,
        cft.id AS type_id,
        COALESCE(cftl.`desc`, CONCAT('Type ', cft.id)) AS type_name,
        COALESCE(catl.`desc`, CONCAT('Category ', cft.category_id)) AS category_name,
        LOWER(COALESCE(cftl.`desc`, '')) AS type_name_l,
        LOWER(COALESCE(catl.`desc`, '')) AS category_name_l,
        LOWER(COALESCE(ol.`desc`, cf.paid_with_desc)) AS mode_label_l,
        COALESCE(ol.`desc`, cf.paid_with_desc) AS mode_label,
        CASE
            WHEN cft.id = 72861 THEN 1
            ELSE 0
        END AS is_cash_on_hand,
        CASE
            WHEN cft.starting_balance = 1 THEN 1
            WHEN CAST(cf.paid_with_desc AS UNSIGNED) IN (149712,149718,149721,152907) THEN 1
            WHEN LOWER(COALESCE(cftl.`desc`, '')) LIKE '%balance%' THEN 1
            WHEN COALESCE(cft.report_label, '') = 'Hide' THEN 1
            ELSE 0
        END AS is_balance
    FROM cashflows cf
    JOIN base_interviews bi ON bi.interview_id = cf.interview_id
    LEFT JOIN members m ON m.id = cf.member_id
    LEFT JOIN core_cashflow_types cft ON cft.id = cf.type
    LEFT JOIN core_cashflow_types_lang cftl
           ON cftl.cashflow_type_id = cft.id AND cftl.status = 1
    LEFT JOIN core_categories cat ON cat.id = cft.category_id
    LEFT JOIN core_categories_lang catl
           ON catl.category_id = cat.id AND catl.status = 1
    LEFT JOIN core_options_lang ol
           ON ol.option_id = CAST(cf.paid_with_desc AS UNSIGNED)
          AND ol.status = 1
    WHERE cf.status = 1
),

/* ---------------------------------------------------------------
   Sources/Uses aggregation per interview
--------------------------------------------------------------- */
cashflow_by_interview AS (
    SELECT
        cf.household_id,
        cf.interview_id,
        SUM(CASE
                WHEN cf.is_cash_on_hand = 1 THEN cf.value
                ELSE 0
            END) AS cash_on_hand_now,
        SUM(CASE
                WHEN cf.cf_direction = 'In'
                     AND cf.is_balance = 0
                     AND cf.is_cash_on_hand = 0
                THEN cf.value
                ELSE 0
            END) AS flow_in,
        SUM(CASE
                WHEN cf.cf_direction = 'Out'
                     AND cf.is_balance = 0
                     AND cf.is_cash_on_hand = 0
                THEN cf.value
                ELSE 0
            END) AS flow_out
    FROM cashflow_enriched cf
    GROUP BY cf.household_id, cf.interview_id
),

sources_uses AS (
    SELECT
        cfi.household_id,
        cfi.interview_id,
        /* Previous interview COH (full history, not just window) */
          COALESCE((
            SELECT SUM(cf2.value)
            FROM cashflows cf2
            WHERE cf2.status = 1
              AND cf2.interview_id = (
                  SELECT i2.id
                  FROM interviews i2
                  WHERE i2.household_id = cfi.household_id
                    AND i2.interview_start_date < bi.interview_start_date
                  ORDER BY i2.interview_start_date DESC
                  LIMIT 1
              )
              AND cf2.type = 72861
          ), 0) AS cash_on_hand_prev,
        cfi.cash_on_hand_now,
        cfi.flow_in,
        cfi.flow_out,
        bi.interview_start_date,
        bi.household_code,
        bi.ra_name
    FROM cashflow_by_interview cfi
    JOIN base_interviews bi ON bi.interview_id = cfi.interview_id
),

/* ---------------------------------------------------------------
   Health issue presence per interview (any health entity answers)
--------------------------------------------------------------- */
health_presence AS (
    SELECT
        a.interview_id,
        COUNT(*) AS hi_answers
    FROM answers a
    WHERE a.interview_id IN (SELECT interview_id FROM base_interviews)
      AND a.entity_type = 60
    GROUP BY a.interview_id
),

/* ---------------------------------------------------------------
   Health cashflows (uses) for CF01
--------------------------------------------------------------- */
health_cashflows AS (
    SELECT
        cf.*,
        bi.household_code,
        bi.interview_start_date AS interview_time,
        bi.ra_name
    FROM cashflow_enriched cf
    JOIN base_interviews bi ON bi.interview_id = cf.interview_id
    WHERE cf.cf_direction = 'Out'
      AND (
            cf.category_name_l LIKE '%clinic%'
         OR cf.category_name_l LIKE '%hospital%'
         OR cf.category_name_l LIKE '%health%'
         OR cf.category_name_l LIKE '%medicine%'
         OR cf.category_name_l LIKE '%drug%'
         OR cf.category_name_l LIKE '%pharmacy%'
          )
),

/* ---------------------------------------------------------------
   CF01: Health expense without health issue link (no HI answers)
--------------------------------------------------------------- */
cf01 AS (
    SELECT
        'CF01' AS issue_code,
        hc.household_code,
        hc.member_name,
        hc.interview_time AS interview_time,
        hc.ra_name,
        CONCAT(
            'Health expense (KSh ',
            FORMAT(hc.value, 0),
            ' for ',
            COALESCE(hc.category_name, hc.type_name),
            ') missing health issue link'
        ) AS issue_description,
        hc.cashflow_date,
        hc.value AS amount_ksh,
        hc.category_name AS category_item,
        hc.type_name,
        hc.mode_label AS mode,
        NULL AS sources,
        NULL AS uses,
        NULL AS pct_diff
    FROM health_cashflows hc
    LEFT JOIN health_presence hp ON hp.interview_id = hc.interview_id
    WHERE COALESCE(hp.hi_answers, 0) = 0
),

/* ---------------------------------------------------------------
   CF02: Source/Use imbalance (>5%)
--------------------------------------------------------------- */
cf02 AS (
    SELECT
        'CF02' AS issue_code,
        su.household_code,
        pm.member_name,
        su.interview_start_date AS interview_time,
        su.ra_name,
        CONCAT(
            'Source/Use imbalance: Sources KSh ',
            FORMAT(
                COALESCE(su.cash_on_hand_prev, 0) + COALESCE(su.flow_in, 0),
                0
            ),
            ', Uses KSh ',
            FORMAT(
                COALESCE(su.flow_out, 0) + COALESCE(su.cash_on_hand_now, 0),
                0
            ),
            ' (Difference: ',
            ROUND(
                ABS(
                    (COALESCE(su.cash_on_hand_prev, 0) + COALESCE(su.flow_in, 0))
                    - (COALESCE(su.flow_out, 0) + COALESCE(su.cash_on_hand_now, 0))
                ) /
                NULLIF(COALESCE(su.cash_on_hand_prev, 0) + COALESCE(su.flow_in, 0), 0)
                * 100,
                1
            ),
            '%)'
        ) AS issue_description,
        NULL AS cashflow_date,
        NULL AS amount_ksh,
        NULL AS category_item,
        NULL AS type_name,
        NULL AS mode,
        (COALESCE(su.cash_on_hand_prev, 0) + COALESCE(su.flow_in, 0)) AS sources,
        (COALESCE(su.flow_out, 0) + COALESCE(su.cash_on_hand_now, 0)) AS uses,
        ROUND(
            ABS(
                (COALESCE(su.cash_on_hand_prev, 0) + COALESCE(su.flow_in, 0))
                - (COALESCE(su.flow_out, 0) + COALESCE(su.cash_on_hand_now, 0))
            ) /
            NULLIF(COALESCE(su.cash_on_hand_prev, 0) + COALESCE(su.flow_in, 0), 0)
            * 100,
            1
        ) AS pct_diff
    FROM sources_uses su
    LEFT JOIN primary_member pm ON pm.interview_id = su.interview_id AND pm.rn = 1
    WHERE (
        ABS(
            (COALESCE(su.cash_on_hand_prev, 0) + COALESCE(su.flow_in, 0))
            - (COALESCE(su.flow_out, 0) + COALESCE(su.cash_on_hand_now, 0))
        ) /
        NULLIF(COALESCE(su.cash_on_hand_prev, 0) + COALESCE(su.flow_in, 0), 0)
        * 100
    ) > 5
),

/* ---------------------------------------------------------------
   CF03: In-kind payment with non-zero value
   
   EXEMPTION: "Repayment in kind" cash flow type is allowed to have 
   a non-zero value. This occurs when debts are repaid with goods/
   services (e.g., shop credit repaid with bread). The value field 
   represents the monetary worth of the in-kind repayment.
--------------------------------------------------------------- */
cf03 AS (
    SELECT
        'CF03' AS issue_code,
        bi.household_code,
        cf.member_name,
        bi.interview_start_date AS interview_time,
        bi.ra_name,
        CONCAT(
            'In-kind payment has value (KSh ',
            FORMAT(cf.value, 0),
            ' for ',
            cf.category_name,
            ') - should be zero'
        ) AS issue_description,
        cf.cashflow_date,
        cf.value AS amount_ksh,
        cf.category_name AS category_item,
        cf.type_name,
        cf.mode_label AS mode,
        NULL AS sources,
        NULL AS uses,
        NULL AS pct_diff
    FROM cashflow_enriched cf
    JOIN base_interviews bi ON bi.interview_id = cf.interview_id
    WHERE cf.paid_with_option_id IN (149691)   -- In-kind (trade/goods/service)
      AND cf.value <> 0
      /* EXEMPTION: Allow in-kind value for debt repayments
         - "Repayment in kind" cashflow type (e.g., shop credit repaid with bread)
         - Any type/category involving credit, loan, or repayment
         These legitimately have non-zero values representing the worth of repayment */
      AND NOT (
            COALESCE(cf.type_name_l, '') LIKE '%repayment in kind%'
         OR COALESCE(cf.category_name_l, '') REGEXP '(credit|loan|repay)'
         OR COALESCE(cf.type_name_l, '')     REGEXP '(credit|loan|repay)'
      )
),

/* ---------------------------------------------------------------
   CF04: Paid on behalf recorded as zero
--------------------------------------------------------------- */
cf04 AS (
    SELECT
        'CF04' AS issue_code,
        bi.household_code,
        cf.member_name,
        bi.interview_start_date AS interview_time,
        bi.ra_name,
        CONCAT(
            'Paid-on-behalf is zero for ',
            cf.category_name,
            ' - should have value'
        ) AS issue_description,
        cf.cashflow_date,
        cf.value AS amount_ksh,
        cf.category_name AS category_item,
        cf.type_name,
        cf.mode_label AS mode,
        NULL AS sources,
        NULL AS uses,
        NULL AS pct_diff
    FROM cashflow_enriched cf
    JOIN base_interviews bi ON bi.interview_id = cf.interview_id
    WHERE cf.paid_with_option_id = 149715    -- Paid on HH behalf
      AND COALESCE(cf.value, 0) = 0
),

/* ---------------------------------------------------------------
   CF10: Unlinked M-Pesa/mobile money transactions
--------------------------------------------------------------- */
cf10 AS (
    SELECT
        'CF10' AS issue_code,
        bi.household_code,
        COALESCE(cf.member_name, pm.member_name) AS member_name,
        bi.interview_start_date AS interview_time,
        bi.ra_name,
        CONCAT(
            'M-Pesa transaction (KSh ',
            FORMAT(cf.value, 0),
            ' for ',
            cf.category_name,
            ') not linked to matching cashflow'
        ) AS issue_description,
        cf.cashflow_date,
        cf.value AS amount_ksh,
        cf.category_name AS category_item,
        cf.type_name,
        cf.mode_label AS mode,
        NULL AS sources,
        NULL AS uses,
        NULL AS pct_diff
    FROM cashflow_enriched cf
    JOIN base_interviews bi ON bi.interview_id = cf.interview_id
    LEFT JOIN primary_member pm ON pm.interview_id = bi.interview_id AND pm.rn = 1
    WHERE cf.paid_with_option_id IN (149667,149670,149673)   -- mobile money modes
      AND cf.linked_to IS NULL
      AND cf.is_balance = 0
),

/* ---------------------------------------------------------------
   CF11: Unlinked credit transactions
--------------------------------------------------------------- */
cf11 AS (
    SELECT
        'CF11' AS issue_code,
        bi.household_code,
        COALESCE(cf.member_name, pm.member_name) AS member_name,
        bi.interview_start_date AS interview_time,
        bi.ra_name,
        CONCAT(
            'Credit transaction (KSh ',
            FORMAT(cf.value, 0),
            ' for ',
            cf.category_name,
            ') not linked - should link to payment/repayment'
        ) AS issue_description,
        cf.cashflow_date,
        cf.value AS amount_ksh,
        cf.category_name AS category_item,
        cf.type_name,
        cf.mode_label AS mode,
        NULL AS sources,
        NULL AS uses,
        NULL AS pct_diff
    FROM cashflow_enriched cf
    JOIN base_interviews bi ON bi.interview_id = cf.interview_id
    LEFT JOIN primary_member pm ON pm.interview_id = bi.interview_id AND pm.rn = 1
    WHERE cf.paid_with_option_id = 149685    -- 03=Credit
      AND cf.linked_to IS NULL
      AND cf.is_balance = 0
),

/* ---------------------------------------------------------------
   CF12: Unlinked bank transfer/debit
--------------------------------------------------------------- */
cf12 AS (
    SELECT
        'CF12' AS issue_code,
        bi.household_code,
        COALESCE(cf.member_name, pm.member_name) AS member_name,
        bi.interview_start_date AS interview_time,
        bi.ra_name,
        CONCAT(
            'Bank transfer/debit (KSh ',
            FORMAT(cf.value, 0),
            ' for ',
            cf.category_name,
            ') not linked to account movement'
        ) AS issue_description,
        cf.cashflow_date,
        cf.value AS amount_ksh,
        cf.category_name AS category_item,
        cf.type_name,
        cf.mode_label AS mode,
        NULL AS sources,
        NULL AS uses,
        NULL AS pct_diff
    FROM cashflow_enriched cf
    JOIN base_interviews bi ON bi.interview_id = cf.interview_id
    LEFT JOIN primary_member pm ON pm.interview_id = bi.interview_id AND pm.rn = 1
    WHERE cf.paid_with_option_id IN (149676,149679,149682,152643)  -- bank/debit/cheque
      AND cf.linked_to IS NULL
      AND cf.is_balance = 0
),

/* ---------------------------------------------------------------
   Health issue metadata (entity items)
--------------------------------------------------------------- */
health_issues AS (
    SELECT
        ei.id AS hi_id,
        ei.household_id,
        ei.member_id,
        ei.interview_id,
        ei.name AS hi_name,
        ei.`desc` AS hi_code,
        ei.open_date,
        ei.close_date
    FROM entity_items ei
    WHERE ei.status = 1
      AND ei.type = 447   -- HEIN
),

/* ---------------------------------------------------------------
   HI01: Said bought medicine but no medicine cashflow
--------------------------------------------------------------- */
hi_buy AS (
    SELECT
        a.interview_id,
        a.household_id,
        a.entity_id AS hi_id
    FROM answers a
    JOIN values_tinyint v ON v.history_id = a.history_id
    WHERE a.question_id = 600735    -- Bought medicine/herbal/home remedy
      AND v.value = 1
),

medicine_cashflows AS (
    SELECT DISTINCT
        cf.household_id,
        cf.interview_id
    FROM cashflow_enriched cf
    WHERE cf.category_name_l REGEXP '(medicine|drug|clinic|hospital|health|pharmacy)'
),

hi01 AS (
    SELECT
        'HI01' AS issue_code,
        bi.household_code,
        m.name AS member_name,
        bi.interview_start_date AS interview_time,
        bi.ra_name,
        'Said bought medicine but no medicine cashflow found - check for medicine/drug purchases' AS issue_description,
        NULL AS cashflow_date,
        NULL AS amount_ksh,
        NULL AS category_item,
        NULL AS type_name,
        NULL AS mode,
        NULL AS sources,
        NULL AS uses,
        NULL AS pct_diff
    FROM hi_buy hb
    JOIN base_interviews bi ON bi.interview_id = hb.interview_id
    LEFT JOIN medicine_cashflows mc
           ON mc.household_id = hb.household_id
          AND mc.interview_id = hb.interview_id
    LEFT JOIN health_issues hi ON hi.hi_id = hb.hi_id
    LEFT JOIN members m ON m.id = hi.member_id
    WHERE mc.household_id IS NULL
),

/* ---------------------------------------------------------------
   HI02: Took medicine but medicine list blank
--------------------------------------------------------------- */
hi_med_take AS (
    SELECT
        a.interview_id,
        a.household_id,
        a.entity_id AS hi_id
    FROM answers a
    LEFT JOIN values_tinyint vti ON vti.history_id = a.history_id
    LEFT JOIN values_int vint ON vint.history_id = a.history_id
    WHERE a.question_id = 600738   -- took medicines?
      AND COALESCE(vti.value, vint.value, 0) = 1
),

hi_med_list AS (
    SELECT DISTINCT
        a.interview_id,
        a.entity_id AS hi_id
    FROM answers a
    LEFT JOIN values_text vt ON vt.history_id = a.history_id
    LEFT JOIN values_varchar vv ON vv.history_id = a.history_id
    WHERE a.question_id = 600741   -- medicine list
      AND TRIM(COALESCE(vt.value, vv.value, '')) <> ''
),

hi02 AS (
    SELECT
        'HI02' AS issue_code,
        bi.household_code,
        m.name AS member_name,
        bi.interview_start_date AS interview_time,
        bi.ra_name,
        'Said took medicine but medicine list is blank' AS issue_description,
        NULL AS cashflow_date,
        NULL AS amount_ksh,
        NULL AS category_item,
        NULL AS type_name,
        NULL AS mode,
        NULL AS sources,
        NULL AS uses,
        NULL AS pct_diff
    FROM hi_med_take ht
    JOIN base_interviews bi ON bi.interview_id = ht.interview_id
    LEFT JOIN hi_med_list hl
           ON hl.interview_id = ht.interview_id
          AND hl.hi_id = ht.hi_id
    LEFT JOIN health_issues hi ON hi.hi_id = ht.hi_id
    LEFT JOIN members m ON m.id = hi.member_id
    WHERE hl.hi_id IS NULL
),

/* ---------------------------------------------------------------
   HI03: Provider visit reported but no provider details
   
   UPDATED: Now also checks for chronic illness recurrent visit forms
   (e.g., Diabetes, Hypertension therapy visits) which use question IDs:
   604131 (form intro), 604125 (health issue selection), 604128 (facility),
   604134 (provider type), 604137 (reason), 604140 (transport), etc.
--------------------------------------------------------------- */
hi_prov_visit AS (
    SELECT
        a.interview_id,
        a.household_id,
        a.entity_id AS hi_id
    FROM answers a
    JOIN values_tinyint v ON v.history_id = a.history_id
    WHERE a.question_id = 600753   -- visited provider since last visit?
      AND v.value = 1
),

/* Regular provider visit form answers */
hi_provider_detail AS (
    SELECT
        a.interview_id,
        COUNT(*) AS detail_count,
        MAX(CASE WHEN a.question_id = 599811 THEN 1 ELSE 0 END) AS has_hi_selection
    FROM answers a
    WHERE a.question_id IN (
        -- Regular provider visit form questions
        599811, 600774, 600777, 600780, 600783, 600786, 600789,
        600816, 600822, 603300, 604071, 604110
    )
    GROUP BY a.interview_id
),

/* Chronic illness recurrent visit form answers
   (RV Recurrent Visit - Chronic Illness Clinics or Therapy) */
hi_chronic_visit AS (
    SELECT
        a.interview_id,
        COUNT(*) AS chronic_detail_count,
        MAX(CASE WHEN a.question_id = 604125 THEN 1 ELSE 0 END) AS has_chronic_hi_selection
    FROM answers a
    WHERE a.question_id IN (
        -- Chronic illness recurrent visit form questions
        604131,  -- Form header/intro
        604125,  -- Which health issue is this for? (Diabetes, Hypertension, etc.)
        604128,  -- Which facility/provider visited
        604134,  -- What type of provider
        604137,  -- Why did you choose this provider
        604140,  -- How does member normally reach facility
        604143,  -- Transport cost
        604146,  -- Which medicines given
        604149,  -- How long to reach facility
        604152,  -- What normally happens during visit
        604155,  -- How long does visit take
        604158,  -- How much pay for visits
        604161   -- Any notes about recurrent visit
    )
    GROUP BY a.interview_id
),

hi03 AS (
    SELECT
        'HI03' AS issue_code,
        bi.household_code,
        COALESCE(m.name, hi.hi_name, '') AS member_name,
        bi.interview_start_date AS interview_time,
        bi.ra_name,
        CONCAT('Said visited provider but provider visit form missing for HI ', 
               COALESCE(hi.hi_name, ''), 
               ' - check Provider Visit or Chronic Illness Visit forms') AS issue_description,
        NULL AS cashflow_date,
        NULL AS amount_ksh,
        NULL AS category_item,
        NULL AS type_name,
        NULL AS mode,
        NULL AS sources,
        NULL AS uses,
        NULL AS pct_diff
    FROM hi_prov_visit hp
    JOIN base_interviews bi ON bi.interview_id = hp.interview_id
    LEFT JOIN hi_provider_detail hd ON hd.interview_id = hp.interview_id
    LEFT JOIN hi_chronic_visit hcv ON hcv.interview_id = hp.interview_id
    LEFT JOIN health_issues hi ON hi.hi_id = hp.hi_id
    LEFT JOIN members m ON m.id = hi.member_id
    /* Only flag if BOTH provider form AND chronic form are missing */
    WHERE COALESCE(hd.detail_count, 0) = 0
      AND COALESCE(hcv.chronic_detail_count, 0) = 0
),


/* ---------------------------------------------------------------
   HI04: Provider form present but missing HI selection
--------------------------------------------------------------- */
hi04 AS (
    SELECT
        'HI04' AS issue_code,
        bi.household_code,
        COALESCE(m.name, hi.hi_name, '') AS member_name,
        bi.interview_start_date AS interview_time,
        bi.ra_name,
        CONCAT('Provider visit form missing health issue selection for HI ', COALESCE(hi.hi_name, '')) AS issue_description,
        NULL AS cashflow_date,
        NULL AS amount_ksh,
        NULL AS category_item,
        NULL AS type_name,
        NULL AS mode,
        NULL AS sources,
        NULL AS uses,
        NULL AS pct_diff
    FROM hi_prov_visit hp
    JOIN base_interviews bi ON bi.interview_id = hp.interview_id
    LEFT JOIN hi_provider_detail hd ON hd.interview_id = hp.interview_id
    LEFT JOIN health_issues hi ON hi.hi_id = hp.hi_id
    LEFT JOIN members m ON m.id = hi.member_id
    WHERE COALESCE(hd.detail_count, 0) > 0
      AND COALESCE(hd.has_hi_selection, 0) = 0
),

/* ---------------------------------------------------------------
   HI06: Pregnancy ended but issue not closed
--------------------------------------------------------------- */
hi_preg_end AS (
    SELECT
        a.interview_id,
        a.household_id,
        a.entity_id AS hi_id
    FROM answers a
    JOIN values_tinyint v ON v.history_id = a.history_id
    WHERE a.question_id = 600711   -- end of pregnancy flag
      AND v.value = 1
),

hi06 AS (
    SELECT
        'HI06' AS issue_code,
        bi.household_code,
        m.name AS member_name,
        bi.interview_start_date AS interview_time,
        bi.ra_name,
        CONCAT(
            'Pregnancy ended but issue not closed: ',
            COALESCE(hi.hi_name, hi.hi_code, '')
        ) AS issue_description,
        NULL AS cashflow_date,
        NULL AS amount_ksh,
        NULL AS category_item,
        NULL AS type_name,
        NULL AS mode,
        NULL AS sources,
        NULL AS uses,
        NULL AS pct_diff
    FROM hi_preg_end hp
    JOIN base_interviews bi ON bi.interview_id = hp.interview_id
    LEFT JOIN health_issues hi ON hi.hi_id = hp.hi_id
    LEFT JOIN members m ON m.id = hi.member_id
    WHERE hi.close_date IS NULL
),

/* ---------------------------------------------------------------
   OT01: Adults without cash-on-hand balance
--------------------------------------------------------------- */
adult_members AS (
    SELECT
        bi.household_code,
        bi.hh_id,
        bi.interview_id,
        bi.interview_date,
        bi.interview_start_date AS interview_time,
        bi.ra_name,
        m.id AS member_id,
        m.name AS member_name,
        FLOOR(DATEDIFF(bi.interview_date, m.birthdate) / 365.25) AS age
    FROM base_interviews bi
    JOIN members m
      ON m.household_id = bi.hh_id
     AND m.status = 1
    WHERE m.birthdate IS NOT NULL
      AND FLOOR(DATEDIFF(bi.interview_date, m.birthdate) / 365.25) >= 18
      AND NOT EXISTS (
          SELECT 1
          FROM status s
          WHERE s.entity_type = 51
            AND s.entity_id   = m.id
            AND s.type        = 2
            AND s.status_date <= bi.interview_date
      )
),

member_cash_balances AS (
    SELECT DISTINCT
        cf.interview_id,
        cf.member_id
    FROM cashflow_enriched cf
    WHERE cf.is_cash_on_hand = 1
),

ot01 AS (
    SELECT
        'OT01' AS issue_code,
        am.household_code,
        am.member_name,
        am.interview_time,
        am.ra_name,
        CONCAT(
            'Adult (18+) without cash on hand balance: ',
            am.member_name,
            ' (age ',
            am.age,
            ')'
        ) AS issue_description,
        NULL AS cashflow_date,
        NULL AS amount_ksh,
        NULL AS category_item,
        NULL AS type_name,
        NULL AS mode,
        NULL AS sources,
        NULL AS uses,
        NULL AS pct_diff
    FROM adult_members am
    LEFT JOIN member_cash_balances mb
           ON mb.interview_id = am.interview_id
          AND mb.member_id = am.member_id
    WHERE mb.member_id IS NULL
),

/* ---------------------------------------------------------------
   NEW CHECK CF14: In-kind transaction issues
   (From Julie's review: "For all in kind transactions, you need to 
   complete this field. Tell us '# litres of camel milk'")
   
   In-kind data is stored in answers table, NOT in cf.quantity/cf.value:
   - Q6 "Units + Good/Service (if in-kind)" = question_id 588624
   - Q7 "Monetary Value (if in-kind)" = question_id 588627
   - Both use entity_type = 42 (cashflow) and entity_id = cf.id
   
   Checks:
   - If Q6 missing AND Q7 missing → both missing
   - If Q6 filled AND Q7 missing → monetary value missing  
   - If Q6 missing AND Q7 filled → description missing
--------------------------------------------------------------- */
inkind_q6_desc AS (
    -- Q6: Units + Good/Service (if in-kind)
    SELECT 
        a.entity_id AS cashflow_id,
        vv.value AS description
    FROM answers a
    JOIN values_varchar vv ON vv.history_id = a.history_id
    WHERE a.entity_type = 42
      AND a.question_id = 588624
),

inkind_q7_value AS (
    -- Q7: Monetary Value (if in-kind)
    SELECT 
        a.entity_id AS cashflow_id,
        vd.value AS monetary_value
    FROM answers a
    JOIN values_decimal vd ON vd.history_id = a.history_id
    WHERE a.entity_type = 42
      AND a.question_id = 588627
),

cf14 AS (
    SELECT
        'CF14' AS issue_code,
        bi.household_code,
        COALESCE(cf.member_name, pm.member_name) AS member_name,
        bi.interview_start_date AS interview_time,
        bi.ra_name,
        CASE
            -- Both description AND monetary value missing
            WHEN q6.description IS NULL AND q7.monetary_value IS NULL 
                THEN CONCAT('In-kind missing description and monetary value: ', 
                             ' on ', cf.cashflow_date)
            -- Has description but missing monetary value
            WHEN q6.description IS NOT NULL AND q7.monetary_value IS NULL
                THEN CONCAT('In-kind "', LEFT(q6.description, 30), '" missing monetary value: ', 
                            ' on ', cf.cashflow_date)
            -- Has monetary value but missing description
            WHEN q6.description IS NULL AND q7.monetary_value IS NOT NULL
                THEN CONCAT('In-kind missing description (KSh ', FORMAT(q7.monetary_value, 0), '): ', 
                           ' on ', cf.cashflow_date)
            ELSE CONCAT('In-kind issue: ', COALESCE(cf.category_name, cf.type_name))
        END AS issue_description,
        cf.cashflow_date,
        q7.monetary_value AS amount_ksh,
        cf.category_name AS category_item,
        cf.type_name,
        cf.mode_label AS mode,
        NULL AS sources,
        NULL AS uses,
        NULL AS pct_diff
    FROM cashflow_enriched cf
    JOIN base_interviews bi ON bi.interview_id = cf.interview_id
    LEFT JOIN primary_member pm ON pm.interview_id = bi.interview_id AND pm.rn = 1
    LEFT JOIN inkind_q6_desc q6 ON q6.cashflow_id = cf.cashflow_id
    LEFT JOIN inkind_q7_value q7 ON q7.cashflow_id = cf.cashflow_id
    WHERE cf.paid_with_option_id = 149691   -- In-kind mode (05)
      AND (
          -- Case 1: Both missing
          (q6.description IS NULL AND q7.monetary_value IS NULL)
          -- Case 2: Has description but missing monetary value
          OR (q6.description IS NOT NULL AND q7.monetary_value IS NULL)
          -- Case 3: Has monetary value but missing description
          OR (q6.description IS NULL AND q7.monetary_value IS NOT NULL)
      )
),

/* ---------------------------------------------------------------
   NEW CHECK HI08: Duplicate pregnancy issues (same member)
   (From Julie's review: "Nyambura's pregnancy is registered twice",
   "You registered Susan with two simultaneous pregnancies")
--------------------------------------------------------------- */
hi08 AS (
    SELECT
        'HI08' AS issue_code,
        h.name AS household_code,
        COALESCE(m.name, '') AS member_name,
        MIN(bi.interview_start_date) AS interview_time,
        MIN(bi.ra_name) AS ra_name,
        CONCAT(
            'Duplicate pregnancy for ',
            COALESCE(m.name, 'unknown'),
            ': "',
            ei1.name,
            '" (started ',
            ei1.open_date,
            ') and "',
            ei2.name,
            '" (started ',
            ei2.open_date,
            ')'
        ) AS issue_description,
        NULL AS cashflow_date,
        NULL AS amount_ksh,
        NULL AS category_item,
        NULL AS type_name,
        NULL AS mode,
        NULL AS sources,
        NULL AS uses,
        NULL AS pct_diff
    FROM entity_items ei1
    JOIN entity_items ei2 
        ON ei1.household_id = ei2.household_id
       AND ei1.member_id = ei2.member_id
       AND ei1.id < ei2.id
       AND ei1.type = 447
       AND ei2.type = 447
       AND ei1.status = 1
       AND ei2.status = 1
    JOIN households h ON h.id = ei1.household_id
    LEFT JOIN members m ON m.id = ei1.member_id
    JOIN base_interviews bi ON bi.hh_id = h.id
    WHERE h.project_id = 129
      AND h.status = 1
      AND (LOWER(ei1.name) LIKE '%pregnan%' AND LOWER(ei2.name) LIKE '%pregnan%')
      AND ABS(DATEDIFF(ei1.open_date, ei2.open_date)) <= 90
      AND (ei1.close_date IS NULL OR ei2.close_date IS NULL)
    GROUP BY h.name, m.name, ei1.name, ei1.open_date, ei2.name, ei2.open_date
),

/* ---------------------------------------------------------------
   NEW CHECK HI09: Pregnancy marked as dormant
   (From Julie's review: "Please note, a pregnancy is never a dormant issue",
   "N's acid reflux cannot be considered dormant")
--------------------------------------------------------------- */
hi09 AS (
    SELECT
        'HI09' AS issue_code,
        bi.household_code,
        COALESCE(m.name, ei.name) AS member_name,
        bi.interview_start_date AS interview_time,
        bi.ra_name,
        CONCAT(
            'Pregnancy incorrectly marked as dormant: ',
            COALESCE(ei.name, ''),
            ' for ',
            COALESCE(m.name, 'unknown'),
            ' - pregnancies should NEVER be dormant'
        ) AS issue_description,
        NULL AS cashflow_date,
        NULL AS amount_ksh,
        NULL AS category_item,
        NULL AS type_name,
        NULL AS mode,
        NULL AS sources,
        NULL AS uses,
        NULL AS pct_diff
    FROM base_interviews bi
    JOIN answers a ON a.interview_id = bi.interview_id
    JOIN values_tinyint vti ON vti.history_id = a.history_id
    JOIN entity_items ei ON ei.id = a.entity_id
    LEFT JOIN members m ON m.id = ei.member_id
    WHERE a.question_id = 603216   -- dormant question
      AND vti.value = 1            -- marked as dormant
      AND (
          LOWER(ei.name) LIKE '%pregnan%'
          OR LOWER(ei.`desc`) LIKE '%pregnan%'
      )
),

/* ---------------------------------------------------------------
   NEW CHECK CF16: Shop credit not linked to expenditure
   (From Julie's review: "For new purchases on credit, this should 
   be a food expenditure that is linked to the financial device of 
   credit at the shop.")
   
   Shop credit cashflow types: 71544, 3757
   Should be linked to an expenditure cashflow
--------------------------------------------------------------- */
cf16 AS (
    SELECT
        'CF16' AS issue_code,
        bi.household_code,
        COALESCE(cf.member_name, pm.member_name) AS member_name,
        bi.interview_start_date AS interview_time,
        bi.ra_name,
        CONCAT(
            'Shop credit not linked to expenditure: KSh ',
            FORMAT(cf.value, 0),
            ' on ',
            cf.cashflow_date,
            ' - add linked purchase'
        ) AS issue_description,
        cf.cashflow_date,
        cf.value AS amount_ksh,
        cf.category_name AS category_item,
        cf.type_name,
        cf.mode_label AS mode,
        NULL AS sources,
        NULL AS uses,
        NULL AS pct_diff
    FROM cashflow_enriched cf
    JOIN base_interviews bi ON bi.interview_id = cf.interview_id
    LEFT JOIN primary_member pm ON pm.interview_id = bi.interview_id AND pm.rn = 1
    WHERE cf.type_id IN (71544, 3757)  -- Shop credit - New purchases on credit
      AND cf.linked_to IS NULL
),

/* ---------------------------------------------------------------
   CF18: Cashflow date outside interview period (>21 days old or future)
   
   ONLY flag if:
   1. Cashflow date is in the future (after interview date), OR
   2. Cashflow is >21 days before interview AND falls BEFORE the previous interview
   
   If cashflow is >21 days old but still ON or AFTER the previous interview,
   don't flag - this means the RA was just late doing the interview.
--------------------------------------------------------------- */
cf18 AS (
    SELECT
        'CF18' AS issue_code,
        bi.household_code,
        COALESCE(cf.member_name, pm.member_name) AS member_name,
        bi.interview_start_date AS interview_time,
        bi.ra_name,
        CASE
            WHEN cf.cashflow_date > bi.interview_date THEN
                CONCAT('Cashflow dated after interview: ', cf.cashflow_date, ' (interview ', bi.interview_date, ')')
            WHEN DATEDIFF(bi.interview_date, cf.cashflow_date) > 21 
                 AND cf.cashflow_date < prev.prev_interview_date THEN
                CONCAT('Cashflow more than 21 days before interview: ', cf.cashflow_date, 
                       ' (interview ', bi.interview_date, ', prev interview ', prev.prev_interview_date, ')')
            ELSE 'Cashflow date outside interview window'
        END AS issue_description,
        cf.cashflow_date,
        cf.value AS amount_ksh,
        cf.category_name AS category_item,
        cf.type_name,
        cf.mode_label AS mode,
        NULL AS sources,
        NULL AS uses,
        NULL AS pct_diff
    FROM cashflow_enriched cf
    JOIN base_interviews bi ON bi.interview_id = cf.interview_id
    LEFT JOIN primary_member pm ON pm.interview_id = bi.interview_id AND pm.rn = 1
    -- Get previous interview for this household
    LEFT JOIN (
        SELECT 
            i.id AS interview_id,
            DATE((SELECT MAX(i2.interview_start_date) 
                  FROM interviews i2 
                  WHERE i2.household_id = i.household_id 
                    AND i2.interview_start_date < i.interview_start_date
                    AND i2.status = 1)) AS prev_interview_date
        FROM interviews i
        WHERE i.status = 1
    ) prev ON prev.interview_id = bi.interview_id
    WHERE cf.is_balance = 0
      AND (
            cf.cashflow_date > bi.interview_date  -- Future date (always flag)
         OR (
              DATEDIFF(bi.interview_date, cf.cashflow_date) > 21
              AND (
                    prev.prev_interview_date IS NULL  -- No previous interview, flag it
                 OR cf.cashflow_date < prev.prev_interview_date  -- Before previous interview, flag it
              )
         )
      )
),


/* ---------------------------------------------------------------
   Combine all issues
--------------------------------------------------------------- */
all_issues AS (
    SELECT * FROM cf01
    UNION ALL SELECT * FROM cf02
    UNION ALL SELECT * FROM cf03
    UNION ALL SELECT * FROM cf04
    UNION ALL SELECT * FROM cf10
    UNION ALL SELECT * FROM cf11
    UNION ALL SELECT * FROM cf12
    UNION ALL SELECT * FROM hi01
    UNION ALL SELECT * FROM hi02
    UNION ALL SELECT * FROM hi03
    UNION ALL SELECT * FROM hi04
    UNION ALL SELECT * FROM hi06
    UNION ALL SELECT * FROM ot01
    UNION ALL SELECT * FROM cf14
    UNION ALL SELECT * FROM cf16
    UNION ALL SELECT * FROM cf18
    UNION ALL SELECT * FROM hi08
    UNION ALL SELECT * FROM hi09
)

/* ---------------------------------------------------------------
   Final output
--------------------------------------------------------------- */
SELECT
    household_code,
    member_name,
    interview_time AS interview_datetime,
    ra_name,
    '' AS ra_comment,
    '' AS resolved,
    issue_description,
    cashflow_date,
    amount_ksh,
    category_item,
    type_name,
    mode,
    sources,
    uses,
    pct_diff
FROM all_issues
ORDER BY household_code, interview_datetime;
