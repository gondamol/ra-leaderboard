# RA Dashboard - Monthly Update Guide

## Quick Overview
Every month, follow these steps to update the dashboard with fresh data and enter manual scores.

---

## Monthly Workflow

### Step 1: Refresh Data from Database (Run Locally)

1. **Open terminal** in the dashboard folder:
   ```
   cd C:\Users\HFD 2\HFD_local\HealthFin_Diaries_Project\03_Diaries\05_ra_dashboard
   ```

2. **Start the app locally:**
   ```
   python -m streamlit run app.py
   ```

3. **In the browser** (http://localhost:8501):
   - Select the **Year** and **Month** you want to update (e.g., December 2025)
   - Click **"🔄 Refresh from Database"** button
   - Wait for data to load (you'll see a success message)

4. **Repeat for each month** you need to update

---

### Step 2: Enter Manual Scores

Manual scores can be entered **from anywhere** (local or cloud) now that Supabase is integrated!

1. Go to dashboard (local or https://ra-leaderboard.streamlit.app)
2. Navigate to **Admin** tab
3. Enter password: `hfd2025`
4. For each RA, rate:
   - **Journal Quality** (0-5): Quality of journal entries
   - **Feedback Response** (0-5): How well they respond to feedback
   - **Team Participation** (0-5): Contributions to team activities

5. Scores save automatically to Supabase!

---

### Step 3: Push Updated Data to Cloud

After refreshing data locally, push the cached files to GitHub:

```powershell
cd "C:\Users\HFD 2\HFD_local\HealthFin_Diaries_Project\03_Diaries\05_ra_dashboard"
git add data/
git commit -m "Update dashboard data for [Month Year]"
git push
```

The cloud dashboard will automatically update within a few minutes.

---

## Score Rubric Reference

### Automated Scores (calculated from data)
| Score | Interview Scheduling | Data Quality | Completion |
|-------|---------------------|--------------|------------|
| 5 | 90-100% within 14-16 days | 95-100% no flags | 90-100% complete |
| 4 | 80-89% | 90-94% | 80-89% |
| 3 | 70-79% | 85-89% | 70-79% |
| 2 | 60-69% | 80-84% | 60-69% |
| 1 | <60% | <80% | <60% |

### Manual Scores (entered by admin)
| Score | Description |
|-------|-------------|
| 5 | Outstanding - Exceeds expectations |
| 4 | Good - Meets expectations well |
| 3 | Basic Minimum - Acceptable |
| 2 | Needs Improvement - Below expectations |
| 1 | Very Poor - Significant issues |
| 0 | Not Rated / No Data |

---

## Troubleshooting

### "Database not available" error
- Make sure MySQL is running locally
- Check VPN connection if accessing remote database

### Manual scores not saving
- Check Supabase connection in sidebar (look for error messages)
- Verify Supabase secrets are configured in Streamlit Cloud

### Data not showing on cloud
- Make sure you pushed the `data/` folder after refreshing
- Wait 2-3 minutes for Streamlit Cloud to redeploy

---

## Important Dates

| Month | Data Collection Period | Dashboard Update By |
|-------|----------------------|---------------------|
| October 2025 | Oct 1-31 | Nov 5 |
| November 2025 | Nov 1-30 | Dec 5 |
| December 2025 | Dec 1-31 | Jan 5 |
| January 2026 | Jan 1-31 | Feb 5 |
| ... | ... | ... |

---

## Contacts

- **Dashboard Issues**: [Your name/email]
- **Data Questions**: [PI contact]
- **Supabase Access**: Check project settings

---

*Last updated: January 8, 2026*
