"""
RA of the Month Dashboard
=========================
A unified dashboard with:
- Public view: Leaderboard for RAs and team
- Admin view: Password-protected manual score entry

Features:
- Connects to database for automated scores
- Password-protected admin section for manual scores
- Saves manual scores to JSON file for persistence
- Refresh button to pull latest data

For Streamlit Cloud deployment:
- Push to GitHub
- Connect to Streamlit Cloud
- Set ADMIN_PASSWORD in Streamlit secrets
"""

import os
import json
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

# Try to import database libraries (may not be available on Streamlit Cloud)
try:
    from sqlalchemy import create_engine, text
    DB_AVAILABLE = True
except ImportError:
    DB_AVAILABLE = False

# --------------------------------------------------------------------
# Configuration
# --------------------------------------------------------------------
st.set_page_config(
    page_title="RA of the Month",
    page_icon="🏆",
    layout="wide",
    initial_sidebar_state="collapsed"
)

SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = SCRIPT_DIR / "data"
MANUAL_SCORES_FILE = DATA_DIR / "manual_scores.json"
CACHED_DATA_FILE = DATA_DIR / "cached_leaderboard.csv"

# Database connection (for local use)
DB_CONNECTION_STR = os.getenv("DB_CONN", "mysql+mysqlconnector://root@127.0.0.1/fd_production")

# Admin password (set in Streamlit secrets or environment)
def get_admin_password():
    """Get admin password from environment, secrets, or default."""
    if os.getenv("ADMIN_PASSWORD"):
        return os.getenv("ADMIN_PASSWORD")
    try:
        return st.secrets.get("ADMIN_PASSWORD", "hfd2025")
    except:
        return "hfd2025"

ADMIN_PASSWORD = get_admin_password()

# Scoring rubric - comprehensive descriptions
RUBRIC = {
    "schedule": {
        "title": "Two-week Schedule",
        "description": "% of interviews done within 14-16 days",
        "automated": True,
        "scores": {
            5: "90%+ done within 14-16 days",
            4: "80-89% within 14-16 days",
            3: "70-79% within 14-16 days",
            2: "60-69% within 14-16 days",
            1: "<60% within 14-16 days"
        }
    },
    "quality": {
        "title": "Data Quality",
        "description": "% of interviews with no quality flags",
        "automated": True,
        "scores": {
            5: "95%+ have no quality flags",
            4: "90-94% no quality flags",
            3: "85-89% no quality flags",
            2: "80-84% no quality flags",
            1: "<80% no quality flags"
        }
    },
    "journal": {
        "title": "Journal Quality",
        "description": "Office person will score 3 journals per RA",
        "automated": False,
        "scores": {
            5: "Outstanding - rich, detailed, insightful journals",
            4: "Good - informative and complete journals",
            3: "Basic minimum - adequate but brief",
            2: "Not good - lacking important details",
            1: "Missing or very little information"
        }
    },
    "feedback": {
        "title": "Responsive to Feedback",
        "description": "Office person will score based on data check responses",
        "automated": False,
        "scores": {
            5: "All data checks done, clear effort to improve",
            4: "Most data checks done, some effort to improve",
            3: "Incomplete data checks / low effort",
            2: "Minimal response to feedback (only if really bad)",
            1: "No response to feedback (only if really bad)"
        }
    },
    "team": {
        "title": "Team Contributions",
        "description": "Office person will score based on team participation",
        "automated": False,
        "scores": {
            5: "Led a discussion, contributed to discussions, active in WhatsApp, other contributions",
            4: "Some contributions to team discussions",
            3: "Minimal contributions to team",
            2: "Rarely contributes (only if really bad)",
            1: "No contributions (only if really bad)"
        }
    }
}


# --------------------------------------------------------------------
# SQL Query for RA Metrics
# --------------------------------------------------------------------
RA_METRICS_SQL = """
WITH 
date_window AS (
    SELECT
        CAST('{start_date}' AS DATE) AS start_date,
        CAST('{end_date}' AS DATE) AS end_date
),

base_interviews AS (
    SELECT DISTINCT
        i.id AS interview_id,
        h.id AS hh_id,
        h.name AS household_code,
        i.interview_start_date,
        DATE(i.interview_start_date) AS interview_date,
        i.household_id,
        i.interviewer_id,
        COALESCE(cu.username, 'Unknown') AS ra_name,
        DATEDIFF(
            i.interview_start_date,
            (
                SELECT MAX(i2.interview_start_date)
                FROM interviews i2
                WHERE i2.household_id = i.household_id
                  AND i2.interview_start_date < i.interview_start_date
                  AND i2.status = 1
            )
        ) AS gap_days
    FROM interviews i
    JOIN households h ON h.id = i.household_id
    LEFT JOIN core_users cu ON cu.id = i.interviewer_id
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

answer_counts AS (
    SELECT bi.interview_id, COUNT(a.id) AS total_answers
    FROM base_interviews bi
    LEFT JOIN answers a ON a.interview_id = bi.interview_id
    GROUP BY bi.interview_id
),

cashflow_counts AS (
    SELECT bi.interview_id, COUNT(cf.id) AS total_cashflows
    FROM base_interviews bi
    LEFT JOIN cashflows cf ON cf.interview_id = bi.interview_id AND cf.status = 1
    GROUP BY bi.interview_id
),

completion_flags AS (
    SELECT
        bi.interview_id,
        bi.ra_name,
        CASE 
            WHEN (
                SELECT COUNT(DISTINCT a.question_id)
                FROM answers a
                JOIN values_tinyint vti ON vti.history_id = a.history_id
                WHERE a.interview_id = bi.interview_id
                  AND a.question_id IN (595743, 590748, 591918, 590742, 590754, 590760, 590766, 604026, 590772, 590778, 604032)
            ) < 11
            THEN 1 ELSE 0 
        END AS flag_goingson,
        CASE 
            WHEN EXISTS (
                SELECT 1
                FROM members m
                LEFT JOIN answers a ON a.interview_id = bi.interview_id
                      AND (a.member_id = m.id OR a.entity_id = m.id)
                      AND a.question_id IN (590727, 590730, 590733, 590736)
                WHERE m.household_id = bi.hh_id AND m.status = 1
                  AND FLOOR(DATEDIFF(bi.interview_start_date, m.birthdate)/365.25) >= 18
                GROUP BY m.id HAVING COUNT(a.id) < 4
            )
            THEN 1 ELSE 0 
        END AS flag_wellbeing,
        CASE WHEN (SELECT COUNT(*) FROM cashflows cf WHERE cf.interview_id = bi.interview_id AND cf.status = 1) = 0 THEN 1 ELSE 0 END AS flag_no_cf,
        CASE WHEN (SELECT COUNT(*) FROM cashflows cf WHERE cf.interview_id = bi.interview_id AND cf.status = 1) BETWEEN 1 AND 19 THEN 1 ELSE 0 END AS flag_few_cf
    FROM base_interviews bi
)

SELECT
    bi.ra_name AS ra_name,
    COUNT(DISTINCT bi.interview_id) AS total_interviews,
    SUM(COALESCE(cfc.total_cashflows, 0)) AS total_cfs,
    ROUND(AVG(COALESCE(ac.total_answers, 0)), 0) AS avg_answers,
    SUM(CASE WHEN bi.gap_days BETWEEN 14 AND 16 THEN 1 ELSE 0 END) AS on_time_visits,
    SUM(CASE WHEN bi.gap_days IS NOT NULL THEN 1 ELSE 0 END) AS scheduled_visits,
    ROUND(
        SUM(CASE WHEN bi.gap_days BETWEEN 14 AND 16 THEN 1 ELSE 0 END) * 100.0 / 
        NULLIF(SUM(CASE WHEN bi.gap_days IS NOT NULL THEN 1 ELSE 0 END), 0), 1
    ) AS schedule_pct,
    SUM(CASE WHEN (cf.flag_goingson + cf.flag_wellbeing + cf.flag_no_cf + cf.flag_few_cf) = 0 THEN 1 ELSE 0 END) AS clean_interviews,
    ROUND(
        SUM(CASE WHEN (cf.flag_goingson + cf.flag_wellbeing + cf.flag_no_cf + cf.flag_few_cf) = 0 THEN 1 ELSE 0 END) * 100.0 / 
        NULLIF(COUNT(DISTINCT bi.interview_id), 0), 1
    ) AS quality_pct
FROM base_interviews bi
LEFT JOIN answer_counts ac ON ac.interview_id = bi.interview_id
LEFT JOIN cashflow_counts cfc ON cfc.interview_id = bi.interview_id
LEFT JOIN completion_flags cf ON cf.interview_id = bi.interview_id
GROUP BY bi.ra_name
ORDER BY bi.ra_name;
"""


# --------------------------------------------------------------------
# Data Functions
# --------------------------------------------------------------------
def get_date_range(month: int, year: int):
    """Get start and end dates for a month."""
    start = datetime(year, month, 1)
    today = datetime.now()
    if month == today.month and year == today.year:
        end = today - pd.Timedelta(days=1)
    else:
        if month == 12:
            end = datetime(year + 1, 1, 1) - pd.Timedelta(days=1)
        else:
            end = datetime(year, month + 1, 1) - pd.Timedelta(days=1)
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")


def fetch_from_database(start_date: str, end_date: str) -> pd.DataFrame:
    """Fetch RA metrics from database."""
    if not DB_AVAILABLE:
        return pd.DataFrame()
    
    try:
        sql = RA_METRICS_SQL.format(start_date=start_date, end_date=end_date)
        engine = create_engine(DB_CONNECTION_STR)
        with engine.connect() as conn:
            df = pd.read_sql(text(sql), conn)
        return df
    except Exception as e:
        st.error(f"Database error: {e}")
        return pd.DataFrame()


def load_cached_data() -> pd.DataFrame:
    """Load cached leaderboard data."""
    if CACHED_DATA_FILE.exists():
        return pd.read_csv(CACHED_DATA_FILE)
    return pd.DataFrame()


def save_cached_data(df: pd.DataFrame):
    """Save data to cache."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(CACHED_DATA_FILE, index=False)


def load_manual_scores() -> dict:
    """Load manual scores from JSON."""
    if MANUAL_SCORES_FILE.exists():
        with open(MANUAL_SCORES_FILE, 'r') as f:
            return json.load(f)
    return {}


def save_manual_scores(scores: dict):
    """Save manual scores to JSON."""
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(MANUAL_SCORES_FILE, 'w') as f:
        json.dump(scores, f, indent=2)


def calculate_automated_scores(df: pd.DataFrame) -> pd.DataFrame:
    """Calculate automated scores based on rubric."""
    df = df.copy()
    
    # Schedule Score (1-5)
    df['schedule_score'] = pd.cut(
        df['schedule_pct'].fillna(0),
        bins=[-1, 59.9, 69.9, 79.9, 89.9, 100],
        labels=[1, 2, 3, 4, 5]
    ).astype(int)
    
    # Quality Score (1-5)
    df['quality_score'] = pd.cut(
        df['quality_pct'].fillna(0),
        bins=[-1, 79.9, 84.9, 89.9, 94.9, 100],
        labels=[1, 2, 3, 4, 5]
    ).astype(int)
    
    return df


def combine_scores(auto_df: pd.DataFrame, manual_scores: dict, month_key: str) -> pd.DataFrame:
    """Combine automated and manual scores."""
    df = auto_df.copy()
    
    # Get manual scores for this month
    month_scores = manual_scores.get(month_key, {})
    
    # Add manual score columns
    df['journal_score'] = df['ra_name'].apply(lambda x: month_scores.get(x, {}).get('journal', 0))
    df['feedback_score'] = df['ra_name'].apply(lambda x: month_scores.get(x, {}).get('feedback', 0))
    df['team_score'] = df['ra_name'].apply(lambda x: month_scores.get(x, {}).get('team', 0))
    
    # Calculate total
    df['total_score'] = (
        df['schedule_score'] + df['quality_score'] + 
        df['journal_score'] + df['feedback_score'] + df['team_score']
    )
    
    # Rank
    df['rank'] = df['total_score'].rank(method='min', ascending=False).astype(int)
    
    return df.sort_values('rank')


# --------------------------------------------------------------------
# UI Components - Custom CSS
# --------------------------------------------------------------------
def inject_css():
    st.markdown("""
    <style>
    .header-container {
        background: linear-gradient(135deg, #1a1a2e 0%, #16213e 50%, #0f3460 100%);
        padding: 2rem; border-radius: 20px; margin-bottom: 2rem; text-align: center;
    }
    .header-container h1 { color: #ffd700; font-size: 2.5rem; margin-bottom: 0.5rem; }
    .header-container p { color: #e8e8e8; font-size: 1.1rem; }
    
    .winner-card {
        background: linear-gradient(135deg, #f5af19 0%, #f12711 100%);
        padding: 2rem; border-radius: 20px; text-align: center; color: white;
        box-shadow: 0 10px 40px rgba(245, 175, 25, 0.4);
    }
    .winner-card h1 { font-size: 3rem; margin: 0.5rem 0; }
    
    .podium-card { padding: 1.5rem; border-radius: 15px; text-align: center; color: white; margin-bottom: 1rem; }
    .podium-gold { background: linear-gradient(135deg, #ffd700 0%, #ffb347 100%); }
    .podium-silver { background: linear-gradient(135deg, #c0c0c0 0%, #a8a8a8 100%); }
    .podium-bronze { background: linear-gradient(135deg, #cd7f32 0%, #b87333 100%); }
    
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1.5rem; border-radius: 15px; text-align: center; color: white;
    }
    .metric-card h3 { font-size: 2rem; margin: 0; }
    .metric-card p { margin: 0.5rem 0 0 0; opacity: 0.9; }
    
    .admin-section {
        background: #fff3cd; padding: 1rem; border-radius: 10px;
        border-left: 4px solid #ffc107; margin: 1rem 0;
    }
    </style>
    """, unsafe_allow_html=True)


def render_header(month_name: str):
    st.markdown(f"""
        <div class="header-container">
            <h1>🏆 RA of the Month</h1>
            <p>{month_name} Leaderboard</p>
        </div>
    """, unsafe_allow_html=True)


def render_winner(df: pd.DataFrame):
    if df.empty:
        return
    
    winner = df.iloc[0]
    manual_complete = all([
        winner.get('journal_score', 0) > 0,
        winner.get('feedback_score', 0) > 0,
        winner.get('team_score', 0) > 0
    ])
    
    col1, col2, col3 = st.columns([1, 2, 1])
    with col2:
        if manual_complete:
            st.markdown(f"""
                <div class="winner-card">
                    <p>🎉 RA OF THE MONTH 🎉</p>
                    <h1>{winner['ra_name'].upper()}</h1>
                    <p style="font-size: 1.5rem;">Score: {int(winner['total_score'])}/25</p>
                </div>
            """, unsafe_allow_html=True)
        else:
            st.markdown(f"""
                <div class="winner-card" style="background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);">
                    <p>📊 Current Leader</p>
                    <h1>{winner['ra_name'].upper()}</h1>
                    <p>Score: {int(winner['total_score'])}/25</p>
                    <p style="font-size: 0.9rem; margin-top: 0.5rem;">⏳ Final results pending...</p>
                </div>
            """, unsafe_allow_html=True)


def render_podium(df: pd.DataFrame):
    if len(df) < 3:
        return
    
    st.markdown("### 🥇🥈🥉 Top Performers")
    cols = st.columns(3)
    
    for i, (col, medal, css) in enumerate(zip(cols, ["🥇", "🥈", "🥉"], ["podium-gold", "podium-silver", "podium-bronze"])):
        if i < len(df):
            ra = df.iloc[i]
            with col:
                st.markdown(f"""
                    <div class="podium-card {css}">
                        <h2>{medal}</h2>
                        <h3>{ra['ra_name'].title()}</h3>
                        <p style="font-size: 1.5rem;">{int(ra['total_score'])}/25</p>
                    </div>
                """, unsafe_allow_html=True)


def render_leaderboard(df: pd.DataFrame):
    st.markdown("### 📊 Full Leaderboard")
    
    display_df = df[['rank', 'ra_name', 'total_score', 'schedule_score', 'quality_score', 
                     'journal_score', 'feedback_score', 'team_score', 'total_interviews']].copy()
    display_df.columns = ['Rank', 'RA Name', 'Total', 'Schedule', 'Quality', 'Journal', 'Feedback', 'Team', 'Interviews']
    display_df['RA Name'] = display_df['RA Name'].str.title()
    
    def score_color(val):
        if pd.isna(val) or val == 0:
            return 'background-color: #f0f0f0; color: #999;'
        colors = {5: '#28a745', 4: '#17a2b8', 3: '#ffc107', 2: '#fd7e14', 1: '#dc3545'}
        return f'background-color: {colors.get(int(val), "#f0f0f0")}; color: white;'
    
    score_cols = ['Schedule', 'Quality', 'Journal', 'Feedback', 'Team']
    st.dataframe(
        display_df.style.applymap(score_color, subset=score_cols),
        use_container_width=True, height=400
    )


def render_admin_section(df: pd.DataFrame, manual_scores: dict, month_key: str):
    """Render the admin section for manual score entry."""
    st.markdown('<div class="admin-section">', unsafe_allow_html=True)
    st.markdown("### ✏️ Admin: Enter Manual Scores")
    st.info("Score each RA from 1-5 on Journal Quality, Responsiveness to Feedback, and Team Contributions.")
    
    # Show comprehensive rubric
    with st.expander("📋 View Complete Scoring Rubric", expanded=False):
        st.markdown("### Scoring Rubric (Total: 25 points)")
        st.markdown("---")
        
        # Create rubric table
        st.markdown("#### 🤖 Automated Scores (calculated from database)")
        
        col1, col2 = st.columns(2)
        with col1:
            st.markdown(f"**{RUBRIC['schedule']['title']}**")
            st.caption(RUBRIC['schedule']['description'])
            for score in [5, 4, 3, 2, 1]:
                st.markdown(f"- **{score}**: {RUBRIC['schedule']['scores'][score]}")
        
        with col2:
            st.markdown(f"**{RUBRIC['quality']['title']}**")
            st.caption(RUBRIC['quality']['description'])
            for score in [5, 4, 3, 2, 1]:
                st.markdown(f"- **{score}**: {RUBRIC['quality']['scores'][score]}")
        
        st.markdown("---")
        st.markdown("#### ✏️ Manual Scores (office team enters)")
        
        col1, col2, col3 = st.columns(3)
        with col1:
            st.markdown(f"**{RUBRIC['journal']['title']}**")
            st.caption(RUBRIC['journal']['description'])
            for score in [5, 4, 3, 2, 1]:
                st.markdown(f"- **{score}**: {RUBRIC['journal']['scores'][score]}")
        
        with col2:
            st.markdown(f"**{RUBRIC['feedback']['title']}**")
            st.caption(RUBRIC['feedback']['description'])
            for score in [5, 4, 3, 2, 1]:
                st.markdown(f"- **{score}**: {RUBRIC['feedback']['scores'][score]}")
        
        with col3:
            st.markdown(f"**{RUBRIC['team']['title']}**")
            st.caption(RUBRIC['team']['description'])
            for score in [5, 4, 3, 2, 1]:
                st.markdown(f"- **{score}**: {RUBRIC['team']['scores'][score]}")
    
    st.divider()
    
    # Get existing scores for this month
    month_scores = manual_scores.get(month_key, {})
    updated = False
    
    for _, row in df.iterrows():
        ra_name = row['ra_name']
        ra_scores = month_scores.get(ra_name, {})
        
        st.markdown(f"#### {ra_name.title()}")
        cols = st.columns(3)
        
        with cols[0]:
            journal = st.selectbox(
                "Journal Quality", options=[0, 1, 2, 3, 4, 5],
                index=ra_scores.get('journal', 0),
                key=f"j_{ra_name}",
                format_func=lambda x: f"{x} - {RUBRIC['journal']['scores'].get(x, 'Not scored')}" if x > 0 else "Not scored"
            )
        with cols[1]:
            feedback = st.selectbox(
                "Responsive to Feedback", options=[0, 1, 2, 3, 4, 5],
                index=ra_scores.get('feedback', 0),
                key=f"f_{ra_name}",
                format_func=lambda x: f"{x} - {RUBRIC['feedback']['scores'].get(x, 'Not scored')}" if x > 0 else "Not scored"
            )
        with cols[2]:
            team = st.selectbox(
                "Team Contributions", options=[0, 1, 2, 3, 4, 5],
                index=ra_scores.get('team', 0),
                key=f"t_{ra_name}",
                format_func=lambda x: f"{x} - {RUBRIC['team']['scores'].get(x, 'Not scored')}" if x > 0 else "Not scored"
            )
        
        new_scores = {'journal': journal, 'feedback': feedback, 'team': team}
        if new_scores != ra_scores:
            if month_key not in manual_scores:
                manual_scores[month_key] = {}
            manual_scores[month_key][ra_name] = new_scores
            updated = True
        
        st.divider()
    
    if updated:
        save_manual_scores(manual_scores)
        st.success("Scores saved!")
        st.rerun()
    
    st.markdown('</div>', unsafe_allow_html=True)


def render_download(df: pd.DataFrame):
    st.divider()
    col1, col2 = st.columns(2)
    with col1:
        st.download_button("📥 Download CSV", df.to_csv(index=False), "ra_leaderboard.csv", "text/csv", use_container_width=True)
    with col2:
        from io import BytesIO
        output = BytesIO()
        df.to_excel(output, index=False, engine='openpyxl')
        output.seek(0)
        st.download_button("📥 Download Excel", output, "ra_leaderboard.xlsx", use_container_width=True)


# --------------------------------------------------------------------
# Main App
# --------------------------------------------------------------------
def main():
    inject_css()
    
    # Sidebar
    with st.sidebar:
        st.header("⚙️ Settings")
        
        today = datetime.now()
        year = st.selectbox("Year", [2024, 2025], index=1)
        month = st.selectbox("Month", list(range(1, 13)), index=today.month - 1,
                            format_func=lambda x: datetime(2000, x, 1).strftime('%B'))
        
        month_key = f"{year}_{month:02d}"
        month_name = datetime(year, month, 1).strftime("%B %Y")
        start_date, end_date = get_date_range(month, year)
        
        st.info(f"📅 {start_date} to {end_date}")
        
        # Refresh button
        if st.button("🔄 Refresh from Database", type="primary", use_container_width=True):
            with st.spinner("Fetching data..."):
                df = fetch_from_database(start_date, end_date)
                if not df.empty:
                    df = calculate_automated_scores(df)
                    save_cached_data(df)
                    st.success("Data refreshed!")
                    st.rerun()
                else:
                    st.error("Could not fetch data from database")
        
        st.divider()
        
        # Admin login
        st.markdown("### 🔐 Admin Access")
        password = st.text_input("Password", type="password")
        is_admin = password == ADMIN_PASSWORD
        
        if password and is_admin:
            st.success("✅ Admin access granted")
        elif password and not is_admin:
            st.error("Invalid password")
    
    # Load data
    df = load_cached_data()
    manual_scores = load_manual_scores()
    
    if df.empty:
        render_header(month_name)
        st.warning("No data available. Click 'Refresh from Database' in the sidebar.")
        st.info("Make sure you're connected to the database and have data for this period.")
        return
    
    # Recalculate automated scores if needed
    if 'schedule_score' not in df.columns:
        df = calculate_automated_scores(df)
    
    # Combine with manual scores
    df = combine_scores(df, manual_scores, month_key)
    
    # Render public view
    render_header(month_name)
    render_winner(df)
    st.divider()
    render_podium(df)
    st.divider()
    
    # Tabs - always create all tabs, but control content visibility
    tab1, tab2, tab3 = st.tabs(["📊 Leaderboard", "✏️ Enter Scores (Admin)", "📈 Charts"])
    
    with tab1:
        render_leaderboard(df)
    
    with tab2:
        if is_admin:
            render_admin_section(df, manual_scores, month_key)
        else:
            st.warning("🔐 **Admin Access Required**")
            st.info("Enter the admin password in the sidebar to access manual score entry.")
    
    with tab3:
        # Score breakdown chart
        score_cols = ['schedule_score', 'quality_score', 'journal_score', 'feedback_score', 'team_score']
        chart_df = df[['ra_name'] + score_cols].melt(id_vars=['ra_name'], var_name='Category', value_name='Score')
        chart_df['Category'] = chart_df['Category'].str.replace('_score', '').str.title()
        
        fig = px.bar(chart_df, x='ra_name', y='Score', color='Category', barmode='group',
                    title='Score Breakdown by Category')
        fig.update_layout(xaxis_tickangle=-45, xaxis_title='', yaxis_title='Score (1-5)')
        st.plotly_chart(fig, use_container_width=True)
    
    render_download(df)
    
    # Footer
    st.markdown(f"""
        <div style="text-align: center; color: #666; margin-top: 2rem;">
            <p>📅 {month_name} | Last updated: {datetime.now().strftime('%Y-%m-%d %H:%M')}</p>
        </div>
    """, unsafe_allow_html=True)


if __name__ == "__main__":
    main()
