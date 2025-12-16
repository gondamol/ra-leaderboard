"""
RA of the Month Dashboard
=========================
Reads metrics from TWO SQL scripts and aggregates in Python:
- 01_monthly_completion_checks.sql → Per-interview completion status
- 02_monthly_quality_checks_V1.sql → Per-interview quality issues

Dashboard aggregates both to RA level and displays all metrics.
"""

import os
import json
import socket
from datetime import datetime
from pathlib import Path

import pandas as pd
import streamlit as st
import plotly.express as px
import plotly.graph_objects as go

# Database libraries
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
    initial_sidebar_state="expanded"
)

SCRIPT_DIR = Path(__file__).resolve().parent
DATA_DIR = SCRIPT_DIR / "data"
SQL_DIR = SCRIPT_DIR / "sql"
MANUAL_SCORES_FILE = DATA_DIR / "manual_scores.json"
CACHED_DATA_FILE = DATA_DIR / "cached_leaderboard.csv"

# SQL files - use the PARENT scripts directly
SQL_COMPLETION_FILE = SQL_DIR / "01_monthly_completion_checks.sql"
SQL_QUALITY_FILE = SQL_DIR / "02_monthly_quality_checks_V1.sql"

def _is_port_open(host: str, port: int, timeout: float = 1.0) -> bool:
    """Return True if a TCP port is reachable."""
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


def get_db_connection_str() -> str:
    """
    Resolve the DB connection string.
    - Prefer environment variable DB_CONN
    - Then Streamlit secrets
    - Finally, fall back to the local default only if MySQL is reachable
    """
    env_conn = os.getenv("DB_CONN")
    if env_conn:
        return env_conn
    try:
        secret_conn = st.secrets.get("DB_CONN")
        if secret_conn:
            return secret_conn
    except Exception:
        pass
    
    default_conn = "mysql+mysqlconnector://root@127.0.0.1/fd_production"
    if _is_port_open("127.0.0.1", 3306):
        return default_conn
    return ""


DB_CONNECTION_STR = get_db_connection_str()
DB_REFRESH_ENABLED = DB_AVAILABLE and bool(DB_CONNECTION_STR)

# RAs to exclude from dashboard (e.g., PI, supervisors)
EXCLUDED_RAS = ['julie', 'cate']


def get_admin_password():
    if os.getenv("ADMIN_PASSWORD"):
        return os.getenv("ADMIN_PASSWORD")
    try:
        return st.secrets.get("ADMIN_PASSWORD", "hfd2025")
    except:
        return "hfd2025"

ADMIN_PASSWORD = get_admin_password()

# Comprehensive scoring rubric
RUBRIC = {
    "schedule": {"title": "Two-week Schedule", "automated": True},
    "quality": {"title": "Data Quality", "automated": True},
    "journal": {"title": "Journal Quality", "automated": False},
    "feedback": {"title": "Responsive to Feedback", "automated": False},
    "team": {"title": "Team Contributions", "automated": False}
}

# Quality Issues Reference
QUALITY_ISSUES = {
    "Cashflow Issues (CF)": [
        {"code": "CF01", "desc": "Health expense without HI link"},
        {"code": "CF02", "desc": "Sources/uses balance >5% diff"},
        {"code": "CF03", "desc": "In-kind value != 0"},
        {"code": "CF04", "desc": "Paid-on-behalf = 0"},
        {"code": "CF05", "desc": "Contextual outliers"},
        {"code": "CF06", "desc": "No food purchases"},
        {"code": "CF09", "desc": "No airtime purchases"},
        {"code": "CF10", "desc": "Unlinked M-Pesa"},
        {"code": "CF11", "desc": "Unlinked credit"},
        {"code": "CF12", "desc": "Unlinked bank/debit"},
        {"code": "CF14", "desc": "In-kind missing description"},
        {"code": "CF16", "desc": "Shop credit not linked"},
        {"code": "CF18", "desc": "Cashflow date outside period"},
    ],
    "Health Issues (HI)": [
        {"code": "HI01", "desc": "Bought medicine, no cashflow"},
        {"code": "HI02", "desc": "Took medicine, name blank"},
        {"code": "HI03", "desc": "Provider visit yes, form missing"},
        {"code": "HI04", "desc": "Provider form missing HI selection"},
        {"code": "HI06", "desc": "Pregnancy ended, issue not closed"},
        {"code": "HI07", "desc": "Health issue without update"},
        {"code": "HI08", "desc": "Duplicate pregnancy issues"},
        {"code": "HI09", "desc": "Pregnancy marked dormant"},
        {"code": "HI10", "desc": "HI closed but forms missing"},
        {"code": "HI11", "desc": "Provider form blank"},
    ],
    "Other Issues": [
        {"code": "OT01", "desc": "Adults without cash-on-hand"},
        {"code": "OT02", "desc": "Member left, form blank"},
        {"code": "FD01", "desc": "VSLA/ROSCA no activity"},
    ]
}


# --------------------------------------------------------------------
# Data Functions
# --------------------------------------------------------------------
def get_date_range(month: int, year: int):
    """
    Get date range for a month.
    - Current month: 1st of month to TODAY
    - Past months: 1st of month to last day of month
    """
    start = datetime(year, month, 1)
    today = datetime.now()
    
    if month == today.month and year == today.year:
        # Current month: use TODAY (not yesterday)
        end = today
    else:
        # Past months: use last day of that month
        if month == 12:
            end = datetime(year + 1, 1, 1) - pd.Timedelta(days=1)
        else:
            end = datetime(year, month + 1, 1) - pd.Timedelta(days=1)
    
    return start.strftime("%Y-%m-%d"), end.strftime("%Y-%m-%d")


def run_sql_file(sql_path: Path, start_date: str, end_date: str, label: str = "") -> pd.DataFrame:
    """Read and execute a SQL file with date parameters."""
    if not sql_path.exists():
        st.error(f"SQL file not found: {sql_path}")
        return pd.DataFrame()
    
    with open(sql_path, 'r', encoding='utf-8') as f:
        sql = f.read()
    
    # Replace date parameters
    sql = sql.replace('@start_date', f"'{start_date}'")
    sql = sql.replace('@end_date', f"'{end_date}'")
    
    if not DB_REFRESH_ENABLED:
        st.sidebar.warning("Database refresh is disabled. Configure DB_CONN in secrets or environment to enable.")
        return pd.DataFrame()
    
    try:
        engine = create_engine(DB_CONNECTION_STR)
        with engine.connect() as conn:
            df = pd.read_sql(text(sql), conn)
        st.sidebar.success(f"✅ {label}: {len(df)} rows")
        return df
    except Exception as e:
        st.error(f"Error running {sql_path.name}: {e}")
        return pd.DataFrame()


def normalize_columns(df: pd.DataFrame) -> pd.DataFrame:
    """Normalize column names to snake_case."""
    new_cols = []
    for c in df.columns:
        c = str(c).lower()
        c = c.replace('%%', 'pct')
        c = c.replace('%', 'pct')
        c = c.replace('<', 'lt')
        c = c.replace('>', 'gt')
        c = c.replace(' ', '_')
        c = c.replace('-', '_')
        c = c.replace("'", "")
        while '__' in c:
            c = c.replace('__', '_')
        c = c.strip('_')
        new_cols.append(c)
    df.columns = new_cols
    return df


def fetch_all_metrics(start_date: str, end_date: str) -> pd.DataFrame:
    """
    Run both SQL scripts and aggregate to RA level.
    
    Returns DataFrame with columns:
    - ra_name, total_interviews, total_cfs, avg_answers
    - pct_complete (from completion script)
    - pct_no_quality_flags (from quality script)
    - pct_lt5pct_imbalance (from CF02 issues)
    - pct_within_14_16_days (from interview intervals)
    """
    if not DB_AVAILABLE:
        st.error("Database libraries not available")
        return pd.DataFrame()
    
    if not DB_REFRESH_ENABLED:
        st.error("Database refresh is disabled. Add DB_CONN to Streamlit secrets or environment, or run with MySQL accessible.")
        return pd.DataFrame()
    
    # Run completion checks
    st.sidebar.info("📄 Running completion checks...")
    completion_df = run_sql_file(SQL_COMPLETION_FILE, start_date, end_date, "Completion")
    
    if completion_df.empty:
        st.error("No completion data")
        return pd.DataFrame()
    
    completion_df = normalize_columns(completion_df)
    
    # Debug info
    st.sidebar.info(f"📅 Date range: {start_date} to {end_date}")
    st.sidebar.info(f"📋 Total interview rows: {len(completion_df)}")
    
    # Find RA column
    ra_col = next((c for c in completion_df.columns if 'interviewer' in c.lower()), None)
    if not ra_col:
        ra_col = next((c for c in completion_df.columns if 'ra' in c.lower()), None)
    if not ra_col:
        st.error(f"Could not find RA column. Columns: {list(completion_df.columns)}")
        return pd.DataFrame()
    
    # Exclude non-RA users (PI, supervisors)
    completion_df = completion_df[~completion_df[ra_col].str.lower().isin(EXCLUDED_RAS)]
    
    # Find status column
    status_col = next((c for c in completion_df.columns if c == 'status'), None)
    
    # Find household column for counting
    hh_col = next((c for c in completion_df.columns if 'household' in c.lower() and 'code' in c.lower()), None)
    
    # Debug: Show unique RAs
    unique_ras = completion_df[ra_col].nunique()
    st.sidebar.info(f"👤 Unique RAs: {unique_ras}")
    
    # Aggregate completion by RA
    if status_col:
        completion_agg = completion_df.groupby(ra_col).agg(
            total_interviews=(hh_col if hh_col else ra_col, 'count'),
            complete_count=(status_col, lambda x: (x == 'Complete').sum())
        ).reset_index()
        completion_agg['pct_complete'] = (completion_agg['complete_count'] / completion_agg['total_interviews'] * 100).round(0).astype(int)
    else:
        completion_agg = completion_df.groupby(ra_col).size().reset_index(name='total_interviews')
        completion_agg['pct_complete'] = 0
    
    completion_agg = completion_agg.rename(columns={ra_col: 'ra_name'})
    
    # Run quality checks
    st.sidebar.info("📄 Running quality checks...")
    quality_df = run_sql_file(SQL_QUALITY_FILE, start_date, end_date, "Quality Issues")
    
    if not quality_df.empty:
        quality_df = normalize_columns(quality_df)
        
        # Find RA column in quality
        ra_col_q = next((c for c in quality_df.columns if 'ra' in c.lower()), None)
        hh_col_q = next((c for c in quality_df.columns if 'household' in c.lower() and 'code' in c.lower()), None)
        issue_col = next((c for c in quality_df.columns if 'issue' in c.lower() and 'desc' in c.lower()), None)
        
        # Find interview datetime column for proper counting
        int_date_col = next((c for c in quality_df.columns if 'interview' in c.lower() and 'date' in c.lower()), None)
        
        st.sidebar.info(f"🔍 Quality issue rows: {len(quality_df)}")
        
        if ra_col_q and hh_col_q:
            # Create unique interview identifier (household_code + interview_datetime)
            # This ensures we count EACH interview, not just households
            if int_date_col:
                quality_df['interview_key'] = quality_df[hh_col_q].astype(str) + '_' + quality_df[int_date_col].astype(str)
                count_col = 'interview_key'
                st.sidebar.info(f"🔑 Counting unique: household + date")
            else:
                count_col = hh_col_q
                st.sidebar.info(f"⚠️ Fallback: counting by household only")
            
            # Count unique INTERVIEWS with issues per RA
            issues_per_ra = quality_df.groupby(ra_col_q)[count_col].nunique().reset_index(name='interviews_with_issues')
            issues_per_ra = issues_per_ra.rename(columns={ra_col_q: 'ra_name'})
            
            # Total unique interviews with any issue
            total_interviews_with_issues = quality_df[count_col].nunique()
            st.sidebar.info(f"❌ Total interviews with issues: {total_interviews_with_issues}")
            
            # Calculate % with <5% imbalance from CF02 issues
            if issue_col:
                cf02_issues = quality_df[quality_df[issue_col].str.contains('imbalance', case=False, na=False)]
                cf02_per_ra = cf02_issues.groupby(ra_col_q)[count_col].nunique().reset_index(name='interviews_with_imbalance')
                cf02_per_ra = cf02_per_ra.rename(columns={ra_col_q: 'ra_name'})
            else:
                cf02_per_ra = pd.DataFrame({'ra_name': completion_agg['ra_name'], 'interviews_with_imbalance': 0})
            
            # Merge with completion data
            result = completion_agg.merge(issues_per_ra, on='ra_name', how='left')
            result['interviews_with_issues'] = result['interviews_with_issues'].fillna(0).astype(int)
            
            # % No Quality Flags = (Total Interviews - Interviews with Issues) / Total Interviews × 100
            result['pct_no_quality_flags'] = ((result['total_interviews'] - result['interviews_with_issues']) / result['total_interviews'] * 100).round(0).astype(int)
            
            # Summary
            total_int = result['total_interviews'].sum()
            total_issues = result['interviews_with_issues'].sum()
            st.sidebar.success(f"✅ Clean interviews: {total_int - total_issues} / {total_int}")
            
            # Merge imbalance stats
            result = result.merge(cf02_per_ra, on='ra_name', how='left')
            result['interviews_with_imbalance'] = result['interviews_with_imbalance'].fillna(0).astype(int)
            result['pct_lt5pct_imbalance'] = ((result['total_interviews'] - result['interviews_with_imbalance']) / result['total_interviews'] * 100).round(0).astype(int)
        else:
            result = completion_agg.copy()
            result['pct_no_quality_flags'] = 100
            result['pct_lt5pct_imbalance'] = 100
    else:
        result = completion_agg.copy()
        result['pct_no_quality_flags'] = 100
        result['pct_lt5pct_imbalance'] = 100
    
    # Calculate Total CFs and Avg Answers from additional queries
    try:
        st.sidebar.info("📄 Fetching cashflow stats...")
        # Query cashflows separately (not joined with answers to avoid multiplication)
        cf_query = f"""
        SELECT 
            cu.username AS ra_name,
            COUNT(cf.id) AS total_cfs
        FROM interviews i
        JOIN households h ON h.id = i.household_id
        LEFT JOIN core_users cu ON cu.id = i.interviewer_id
        LEFT JOIN cashflows cf ON cf.interview_id = i.id AND cf.status = 1
        WHERE h.project_id = 129
          AND h.status = 1
          AND h.out = 0
          AND i.status = 1
          AND i.interviewer_id IS NOT NULL
          AND DATE(i.interview_start_date) BETWEEN '{start_date}' AND '{end_date}'
          AND h.name NOT LIKE '%test%'
        GROUP BY cu.username
        """
        engine = create_engine(DB_CONNECTION_STR)
        with engine.connect() as conn:
            cf_stats = pd.read_sql(text(cf_query), conn)
        cf_stats = normalize_columns(cf_stats)
        
        if not cf_stats.empty:
            result = result.merge(cf_stats[['ra_name', 'total_cfs']], on='ra_name', how='left')
            result['total_cfs'] = result['total_cfs'].fillna(0).astype(int)
        else:
            result['total_cfs'] = 0
            
        # Query answers separately
        st.sidebar.info("📄 Fetching answer stats...")
        ans_query = f"""
        SELECT 
            cu.username AS ra_name,
            COUNT(a.id) AS total_answers
        FROM interviews i
        JOIN households h ON h.id = i.household_id
        LEFT JOIN core_users cu ON cu.id = i.interviewer_id
        LEFT JOIN answers a ON a.interview_id = i.id
        WHERE h.project_id = 129
          AND h.status = 1
          AND h.out = 0
          AND i.status = 1
          AND i.interviewer_id IS NOT NULL
          AND DATE(i.interview_start_date) BETWEEN '{start_date}' AND '{end_date}'
          AND h.name NOT LIKE '%test%'
        GROUP BY cu.username
        """
        with engine.connect() as conn:
            ans_stats = pd.read_sql(text(ans_query), conn)
        ans_stats = normalize_columns(ans_stats)
        
        if not ans_stats.empty:
            result = result.merge(ans_stats[['ra_name', 'total_answers']], on='ra_name', how='left')
            result['avg_answers'] = (result['total_answers'].fillna(0) / result['total_interviews']).round(0).astype(int)
        else:
            result['avg_answers'] = 0
    except Exception as e:
        st.sidebar.warning(f"Could not fetch CF/answer stats: {e}")
        result['total_cfs'] = 0
        result['avg_answers'] = 0
    
    # Calculate % within 14-16 days from interview intervals
    try:
        st.sidebar.info("📄 Fetching schedule stats...")
        # Use subquery instead of LATERAL (not supported in MySQL)
        interval_query = f"""
        SELECT 
            cu.username AS ra_name,
            SUM(CASE 
                WHEN gap_days BETWEEN 14 AND 16 THEN 1 
                ELSE 0 
            END) AS on_schedule,
            COUNT(*) AS total_interviews
        FROM (
            SELECT 
                i.id AS interview_id,
                i.interviewer_id,
                DATEDIFF(i.interview_start_date, 
                    (SELECT MAX(i2.interview_start_date) 
                     FROM interviews i2 
                     WHERE i2.household_id = i.household_id 
                       AND i2.interview_start_date < i.interview_start_date
                       AND i2.status = 1)
                ) AS gap_days
            FROM interviews i
            JOIN households h ON h.id = i.household_id
            WHERE h.project_id = 129
              AND h.status = 1
              AND h.out = 0
              AND i.status = 1
              AND i.interviewer_id IS NOT NULL
              AND DATE(i.interview_start_date) BETWEEN '{start_date}' AND '{end_date}'
              AND h.name NOT LIKE '%test%'
        ) sub
        JOIN core_users cu ON cu.id = sub.interviewer_id
        WHERE sub.gap_days IS NOT NULL
        GROUP BY cu.username
        """
        with engine.connect() as conn:
            interval_stats = pd.read_sql(text(interval_query), conn)
        interval_stats = normalize_columns(interval_stats)
        
        if not interval_stats.empty:
            interval_stats['pct_within_14_16_days'] = (interval_stats['on_schedule'] / interval_stats['total_interviews'] * 100).round(0).astype(int)
            result = result.merge(interval_stats[['ra_name', 'pct_within_14_16_days']], on='ra_name', how='left')
            result['pct_within_14_16_days'] = result['pct_within_14_16_days'].fillna(0).astype(int)
        else:
            result['pct_within_14_16_days'] = 0
    except Exception as e:
        st.sidebar.warning(f"Could not fetch schedule stats: {e}")
        result['pct_within_14_16_days'] = 0
    
    st.sidebar.info(f"Columns: {list(result.columns)}")
    
    return result


def load_cached_data() -> pd.DataFrame:
    if CACHED_DATA_FILE.exists():
        return pd.read_csv(CACHED_DATA_FILE)
    return pd.DataFrame()


def save_cached_data(df: pd.DataFrame):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    df.to_csv(CACHED_DATA_FILE, index=False)


def load_manual_scores() -> dict:
    if MANUAL_SCORES_FILE.exists():
        with open(MANUAL_SCORES_FILE, 'r') as f:
            return json.load(f)
    return {}


def save_manual_scores(scores: dict):
    DATA_DIR.mkdir(parents=True, exist_ok=True)
    with open(MANUAL_SCORES_FILE, 'w') as f:
        json.dump(scores, f, indent=2)


def calculate_scores(df: pd.DataFrame) -> pd.DataFrame:
    df = df.copy()
    
    # Schedule Score (based on % within 14-16 days)
    schedule_col = next((c for c in df.columns if 'within' in c.lower() or '14_16' in c.lower()), None)
    if schedule_col and schedule_col in df.columns:
        df['schedule_score'] = pd.cut(df[schedule_col].fillna(0),
            bins=[-1, 59.9, 69.9, 79.9, 89.9, 100], labels=[1, 2, 3, 4, 5]).astype(float).fillna(0).astype(int)
    else:
        df['schedule_score'] = 0
    
    # Quality Score (based on % no quality flags)
    quality_col = next((c for c in df.columns if 'quality' in c.lower() and 'flag' in c.lower()), None)
    if quality_col and quality_col in df.columns:
        df['quality_score'] = pd.cut(df[quality_col].fillna(0),
            bins=[-1, 79.9, 84.9, 89.9, 94.9, 100], labels=[1, 2, 3, 4, 5]).astype(float).fillna(0).astype(int)
    else:
        df['quality_score'] = 0
    
    # Completion Score (based on % complete) - NEW
    completion_col = next((c for c in df.columns if 'pct_complete' in c.lower() or c == 'pct_complete'), None)
    if completion_col and completion_col in df.columns:
        df['completion_score'] = pd.cut(df[completion_col].fillna(0),
            bins=[-1, 59.9, 69.9, 79.9, 89.9, 100], labels=[1, 2, 3, 4, 5]).astype(float).fillna(0).astype(int)
    else:
        df['completion_score'] = 0
    
    return df


def combine_scores(auto_df: pd.DataFrame, manual_scores: dict, month_key: str) -> pd.DataFrame:
    df = auto_df.copy()
    month_scores = manual_scores.get(month_key, {})
    df['journal_score'] = df['ra_name'].apply(lambda x: month_scores.get(str(x) if x else '', {}).get('journal', 0))
    df['feedback_score'] = df['ra_name'].apply(lambda x: month_scores.get(str(x) if x else '', {}).get('feedback', 0))
    df['team_score'] = df['ra_name'].apply(lambda x: month_scores.get(str(x) if x else '', {}).get('team', 0))
    # Total now includes completion_score (max 30 points)
    df['total_score'] = (df['schedule_score'] + df['quality_score'] + df['completion_score'] + 
                         df['journal_score'] + df['feedback_score'] + df['team_score'])
    df['rank'] = df['total_score'].rank(method='min', ascending=False).astype(int)
    return df.sort_values('rank')


# --------------------------------------------------------------------
# Custom CSS
# --------------------------------------------------------------------
def inject_css():
    st.markdown("""
    <style>
    .header-container {
        background: linear-gradient(135deg, #1a1a2e 0%, #16213e 50%, #0f3460 100%);
        padding: 0.8rem 1.5rem; border-radius: 12px; margin-bottom: 0.5rem; 
        display: flex; align-items: center; justify-content: center; gap: 1rem;
    }
    .header-container h1 { color: #ffd700; font-size: 1.5rem; margin: 0; }
    .header-container p { color: #e8e8e8; font-size: 0.95rem; margin: 0; }
    
    .winner-card {
        background: linear-gradient(135deg, #f5af19 0%, #f12711 100%);
        padding: 2rem; border-radius: 20px; text-align: center; color: white;
        box-shadow: 0 10px 40px rgba(245, 175, 25, 0.4);
    }
    .winner-card h1 { font-size: 3rem; margin: 0.5rem 0; }
    
    .metric-card {
        background: linear-gradient(135deg, #667eea 0%, #764ba2 100%);
        padding: 1.5rem; border-radius: 15px; text-align: center; color: white;
    }
    .metric-card h3 { font-size: 2rem; margin: 0; }
    
    .quality-card {
        background: #f8f9fa; padding: 1rem; border-radius: 10px;
        border-left: 4px solid #28a745; margin: 0.5rem 0;
    }
    </style>
    """, unsafe_allow_html=True)


# --------------------------------------------------------------------
# Tab Renderers
# --------------------------------------------------------------------
def render_data_summary(df: pd.DataFrame, month_name: str):
    st.markdown(f"## 📊 RA Performance Summary - {month_name}")
    st.markdown("**Green** = Good (≥90%), **Yellow** = OK (70-89%), **Red** = Needs Improvement (<70%)")
    
    column_map = {
        'ra_name': 'RA Name',
        'total_interviews': 'Total Interviews',
        'total_cfs': 'Total CFs',
        'avg_answers': 'Avg Answers',
        'pct_complete': '% Complete',
        'pct_no_quality_flags': '% No Quality Flags',
        'pct_lt5pct_imbalance': '% <5% Imbalance',
        'pct_within_14_16_days': '% Within 14-16 Days'
    }
    
    display_df = df.copy()
    
    # Format all numeric columns to integers
    numeric_cols = ['total_cfs', 'avg_answers', 'pct_complete', 'pct_no_quality_flags', 'pct_lt5pct_imbalance', 'pct_within_14_16_days']
    for col in numeric_cols:
        if col in display_df.columns:
            display_df[col] = display_df[col].fillna(0).round(0).astype(int)
    
    display_df = display_df.rename(columns=column_map)
    
    desired_cols = list(column_map.values())
    display_cols = [c for c in desired_cols if c in display_df.columns]
    display_df = display_df[display_cols]
    
    if 'RA Name' in display_df.columns:
        display_df['RA Name'] = display_df['RA Name'].str.title()
    
    pct_cols = ['% Complete', '% No Quality Flags', '% <5% Imbalance', '% Within 14-16 Days']
    
    def highlight_metrics(val, col_name):
        if pd.isna(val) or col_name not in pct_cols:
            return ''
        try:
            val = float(val)
            if val >= 90: return 'background-color: #28a745; color: white;'
            elif val >= 70: return 'background-color: #ffc107; color: black;'
            else: return 'background-color: #dc3545; color: white;'
        except:
            return ''
    
    styled = display_df.style.apply(lambda x: [highlight_metrics(v, x.name) for v in x], axis=0)
    st.dataframe(styled, use_container_width=True, height=450, hide_index=True)
    
    # Visualizations
    st.markdown("### 📈 Performance Insights")
    
    viz_col1, viz_col2 = st.columns(2)
    
    with viz_col1:
        # Quality comparison bar chart
        st.markdown("#### Quality Score Distribution")
        if 'pct_no_quality_flags' in df.columns:
            chart_df = df[['ra_name', 'pct_no_quality_flags']].copy()
            chart_df['ra_name'] = chart_df['ra_name'].str.title()
            chart_df = chart_df.sort_values('pct_no_quality_flags', ascending=True)
            
            # Color based on score
            chart_df['Color'] = chart_df['pct_no_quality_flags'].apply(
                lambda x: '#dc3545' if x < 70 else ('#ffc107' if x < 90 else '#28a745')
            )
            
            fig = px.bar(chart_df, x='pct_no_quality_flags', y='ra_name', orientation='h',
                        color='pct_no_quality_flags', 
                        color_continuous_scale=['#dc3545', '#ffc107', '#28a745'],
                        range_color=[50, 100])
            fig.update_layout(
                xaxis_title="% No Quality Flags", yaxis_title="",
                xaxis_range=[0, 100], height=350, showlegend=False,
                coloraxis_showscale=False
            )
            st.plotly_chart(fig, use_container_width=True)
    
    with viz_col2:
        # Schedule compliance bar chart
        st.markdown("#### Schedule Compliance (14-16 Days)")
        if 'pct_within_14_16_days' in df.columns:
            chart_df = df[['ra_name', 'pct_within_14_16_days']].copy()
            chart_df['ra_name'] = chart_df['ra_name'].str.title()
            chart_df = chart_df.sort_values('pct_within_14_16_days', ascending=True)
            
            fig = px.bar(chart_df, x='pct_within_14_16_days', y='ra_name', orientation='h',
                        color='pct_within_14_16_days',
                        color_continuous_scale=['#dc3545', '#ffc107', '#28a745'],
                        range_color=[50, 100])
            fig.update_layout(
                xaxis_title="% Within Schedule", yaxis_title="",
                xaxis_range=[0, 100], height=350, showlegend=False,
                coloraxis_showscale=False
            )
            st.plotly_chart(fig, use_container_width=True)
    
    # Team Totals
    st.markdown("### 📈 Team Totals")
    cols = st.columns(4)
    
    def safe_sum(col_names):
        for c in col_names:
            if c in df.columns:
                return int(df[c].sum())
        return 0
    
    def safe_mean(col_names):
        for c in col_names:
            if c in df.columns:
                return f"{int(df[c].mean())}%"
        return "N/A"
    
    metrics = [
        (str(safe_sum(['total_interviews'])), "Total Interviews", "📋"),
        (str(safe_sum(['total_cfs'])), "Total Cashflows", "💰"),
        (safe_mean(['pct_complete']), "Avg Completion", "✅"),
        (safe_mean(['pct_no_quality_flags']), "Avg Quality", "🎯"),
    ]
    for col, (value, label, icon) in zip(cols, metrics):
        with col:
            st.markdown(f'<div class="metric-card"><p>{icon}</p><h3>{value}</h3><p>{label}</p></div>', unsafe_allow_html=True)


def render_ra_of_month(df: pd.DataFrame, month_name: str):
    st.markdown(f"## 🏆 RA of the Month - {month_name}")
    
    if df.empty or len(df) < 1:
        st.warning("No data available")
        return
    
    # Top 3 Podium Display
    st.markdown("### 🎖️ Top Performers")
    
    medals = ["🥇", "🥈", "🥉"]
    medal_colors = ["#FFD700", "#C0C0C0", "#CD7F32"]
    
    cols = st.columns(3)
    for i, col in enumerate(cols):
        if i < len(df):
            ra = df.iloc[i]
            ra_name = str(ra['ra_name']).title() if 'ra_name' in ra else 'TBD'
            score = int(ra['total_score']) if 'total_score' in ra else 0
            
            with col:
                st.markdown(f"""
                    <div style="text-align: center; padding: 1.5rem; border-radius: 15px; 
                                background: linear-gradient(135deg, {medal_colors[i]}20, {medal_colors[i]}40);
                                border: 3px solid {medal_colors[i]};">
                        <p style="font-size: 3rem; margin: 0;">{medals[i]}</p>
                        <h2 style="margin: 0.5rem 0;">{ra_name}</h2>
                        <p style="font-size: 1.5rem; font-weight: bold;">{score}/30</p>
                    </div>
                """, unsafe_allow_html=True)
    
    st.divider()
    
    # Score Breakdown Visualization
    st.markdown("### 📊 Score Breakdown by Category")
    
    # Prepare data for visualization - now includes completion
    score_cols = ['schedule_score', 'completion_score', 'quality_score', 'journal_score', 'feedback_score', 'team_score']
    score_labels = ['Schedule', 'Completion', 'Quality', 'Journal', 'Feedback', 'Team']
    
    chart_data = []
    for _, row in df.iterrows():
        ra_name = str(row['ra_name']).title() if 'ra_name' in row else 'Unknown'
        for col, label in zip(score_cols, score_labels):
            if col in row:
                chart_data.append({'RA': ra_name, 'Category': label, 'Score': int(row[col])})
    
    if chart_data:
        chart_df = pd.DataFrame(chart_data)
        fig = px.bar(chart_df, x='RA', y='Score', color='Category', 
                     barmode='group', color_discrete_sequence=px.colors.qualitative.Set2,
                     title="")
        fig.update_layout(
            xaxis_title="", yaxis_title="Score (0-5)", 
            yaxis_range=[0, 5.5],
            legend_title="Category",
            height=400
        )
        st.plotly_chart(fig, use_container_width=True)
    
    # Areas Needing Attention - Heatmap Visualization
    st.markdown("### ⚠️ Areas Needing Attention")
    
    # Create matrix data for heatmap
    heatmap_data = []
    for _, row in df.iterrows():
        ra_name = str(row['ra_name']).title() if 'ra_name' in row else 'Unknown'
        row_data = {'RA': ra_name}
        for col, label in zip(score_cols, score_labels):
            if col in row:
                row_data[label] = int(row[col])
        heatmap_data.append(row_data)
    
    if heatmap_data:
        heatmap_df = pd.DataFrame(heatmap_data)
        heatmap_df = heatmap_df.set_index('RA')
        
        # Create heatmap using plotly
        fig = go.Figure(data=go.Heatmap(
            z=heatmap_df.values,
            x=heatmap_df.columns,
            y=heatmap_df.index,
            colorscale=[[0, '#dc3545'], [0.4, '#ffc107'], [0.6, '#ffc107'], [1, '#28a745']],
            zmin=0, zmax=5,
            text=heatmap_df.values,
            texttemplate="%{text}",
            textfont={"size": 14, "color": "white"},
            hovertemplate="RA: %{y}<br>Category: %{x}<br>Score: %{z}<extra></extra>"
        ))
        fig.update_layout(
            height=max(300, len(heatmap_df) * 35),
            xaxis_title="",
            yaxis_title="",
            yaxis={'categoryorder': 'total ascending'}
        )
        st.plotly_chart(fig, use_container_width=True)
        
        # Summary of critical areas
        critical_count = sum(1 for _, row in df.iterrows() 
                            for col in score_cols if col in row and int(row[col]) <= 1)
        warning_count = sum(1 for _, row in df.iterrows() 
                           for col in score_cols if col in row and int(row[col]) == 2)
        
        if critical_count > 0 or warning_count > 0:
            st.markdown(f"**Summary:** 🔴 {critical_count} critical (score ≤1), 🟡 {warning_count} needs work (score = 2)")
        else:
            st.success("✅ All RAs performing well across all categories!")
    
    st.divider()
    
    # Full Leaderboard - now includes completion
    st.markdown("### 📋 Full Leaderboard")
    score_cols_display = ['rank', 'ra_name', 'total_score', 'schedule_score', 'completion_score', 'quality_score', 'journal_score', 'feedback_score', 'team_score']
    display_cols = [c for c in score_cols_display if c in df.columns]
    display_df = df[display_cols].copy()
    display_df.columns = ['Rank', 'RA Name', 'Total', 'Schedule', 'Completion', 'Quality', 'Journal', 'Feedback', 'Team'][:len(display_cols)]
    if 'RA Name' in display_df.columns:
        display_df['RA Name'] = display_df['RA Name'].str.title()
    st.dataframe(display_df, use_container_width=True, height=400, hide_index=True)


def render_quality_guidelines():
    st.markdown("## 📋 Quality Data Guidelines")
    st.markdown("*Follow these standards to ensure your interviews are high quality and won't be flagged for issues.*")
    
    st.markdown("---")
    
    # Journal Standards
    st.markdown("### 📓 Journal Quality")
    st.markdown("""
    **Standard:** Journals share new information that updates our understanding of the household.
    
    ✅ **What to do:**
    - Complete journal on the **same day** as the interview
    - Write at least **150 words** with specific details
    - Include **new information** since the last visit (not old info)
    - Describe **current situation**, issues faced, and how they're dealing with health issues
    - Explain any data that doesn't make sense
    """)
    
    # Data Completion
    st.markdown("### ✅ Data Completion")
    st.markdown("""
    **Standard:** All parts of the interview are complete with no skipped sections.
    
    ✅ **What to do:**
    - Fill **all well-being** questions for every adult
    - Complete **all goings-on** questions
    - Fill **all fields** for health updates and provider visits
    - Record **all cash flows** and asset changes
    - Complete **major events** section
    - Fill **change since last visit** section
    - Record **cash on hand** for all adults (18+)
    - Don't skip questions in the middle of sections!
    """)
    
    # Data Quality
    st.markdown("### 🎯 Data Quality")
    st.markdown("""
    **Standard:** All data entered is correct and makes sense.
    
    ✅ **Cashflow Checks:**
    - **Balance sources and uses** (difference should be <5%)
    - **Link all M-Pesa transactions** to corresponding cashflows
    - **Link credit/shop transactions** to expenditures
    - Record **in-kind items** with description and value
    - Go through **all financial devices** and ensure there really were no cashflows
    
    ✅ **Health Issue Checks:**
    - Fill **update forms** for all active health issues
    - When medicine is bought, **link to health issue**
    - Record **medicine names** when taken
    - Fill **close form** when health issue ends
    - When health visit is reported, complete **provider visit form**
    - Don't create **duplicate pregnancies**
    - Never mark pregnancy as **dormant**
    """)
    
    # Interview Timing
    st.markdown("### ⏰ Interview Timing")
    st.markdown("""
    **Standard:** Interviews are done within the scheduled window.
    
    ✅ **What to do:**
    - Conduct interviews within **14-16 days** of the previous interview
    - Don't let interviews go past the schedule window
    - Plan ahead to avoid delays
    """)
    
    st.markdown("---")
    st.info("💡 **Tip:** If something doesn't make sense in the data, explain it in the journal!")


def render_quality_issues(quality_df: pd.DataFrame, month_name: str):
    """Render quality issues analysis with RA filter for training insights."""
    st.markdown(f"## 🔍 Quality Issues Analysis - {month_name}")
    st.markdown("*Use this to identify patterns and areas to emphasize in team calls.*")
    
    if quality_df.empty:
        st.info("No quality issues data available. Click 'Refresh from Database' to load.")
        return
    
    quality_df = normalize_columns(quality_df)
    
    # Find columns
    ra_col = next((c for c in quality_df.columns if 'ra' in c.lower() and 'name' in c.lower()), None)
    issue_col = next((c for c in quality_df.columns if 'issue' in c.lower() and 'desc' in c.lower()), None)
    
    if not ra_col:
        st.warning("Could not find RA column in quality data")
        return
    
    # RA Filter
    all_ras = ['All RAs'] + sorted(quality_df[ra_col].dropna().unique().tolist())
    selected_ra = st.selectbox("🔎 Filter by RA", all_ras, key="quality_ra_filter")
    
    if selected_ra != 'All RAs':
        filtered_df = quality_df[quality_df[ra_col] == selected_ra].copy()
    else:
        filtered_df = quality_df.copy()
    
    st.markdown(f"**Total Issues Found:** {len(filtered_df)}")
    
    if len(filtered_df) == 0:
        st.success("✅ No quality issues for this selection!")
        return
    
    # Extract issue categories by pattern matching
    if issue_col:
        def categorize_issue(desc):
            desc_lower = str(desc).lower()
            if 'not linked' in desc_lower or 'unlinked' in desc_lower or 'linking' in desc_lower:
                return 'Transactions Not Linked'
            elif 'imbalance' in desc_lower or 'source' in desc_lower and 'use' in desc_lower:
                return 'Source/Use Imbalance'
            elif 'cash on hand' in desc_lower or 'cash-on-hand' in desc_lower:
                return 'Cash on Hand Missing'
            elif 'in-kind' in desc_lower or 'in kind' in desc_lower:
                return 'In-Kind Issues'
            elif '21 days' in desc_lower or 'old' in desc_lower:
                return 'Old Transactions'
            elif 'medicine' in desc_lower or 'health' in desc_lower:
                return 'Health/Medicine Issues'
            elif 'pregnancy' in desc_lower:
                return 'Pregnancy Issues'
            elif 'update' in desc_lower or 'form' in desc_lower:
                return 'Missing Forms/Updates'
            elif 'mpesa' in desc_lower or 'm-pesa' in desc_lower:
                return 'M-Pesa Issues'
            elif 'credit' in desc_lower or 'shop' in desc_lower:
                return 'Credit/Shop Issues'
            else:
                return 'Other Issues'
        
        filtered_df['issue_category'] = filtered_df[issue_col].apply(categorize_issue)
    
    col1, col2 = st.columns(2)
    
    with col1:
        # Issue Categories Chart
        st.markdown("### 📊 Issues by Category")
        if 'issue_category' in filtered_df.columns:
            category_counts = filtered_df['issue_category'].value_counts().reset_index()
            category_counts.columns = ['Category', 'Count']
            
            fig = px.bar(category_counts, x='Count', y='Category', orientation='h',
                        color='Count', color_continuous_scale='Reds')
            fig.update_layout(
                height=350, showlegend=False, coloraxis_showscale=False,
                yaxis={'categoryorder':'total ascending'},
                xaxis_title="Number of Issues", yaxis_title=""
            )
            st.plotly_chart(fig, use_container_width=True)
    
    with col2:
        # Issues by RA (if showing all) or pie chart (if specific RA)
        if selected_ra == 'All RAs':
            st.markdown("### 👤 Issues per RA")
            ra_counts = filtered_df[ra_col].value_counts().reset_index()
            ra_counts.columns = ['RA', 'Issue Count']
            ra_counts['RA'] = ra_counts['RA'].str.title()
            ra_counts = ra_counts.sort_values('Issue Count', ascending=True)
            
            fig = px.bar(ra_counts, x='Issue Count', y='RA', orientation='h',
                        color='Issue Count', color_continuous_scale='RdYlGn_r')
            fig.update_layout(height=350, showlegend=False, coloraxis_showscale=False,
                            xaxis_title="Number of Issues", yaxis_title="")
            st.plotly_chart(fig, use_container_width=True)
        else:
            st.markdown("### 📋 Category Breakdown")
            if 'issue_category' in filtered_df.columns:
                category_counts = filtered_df['issue_category'].value_counts().reset_index()
                category_counts.columns = ['Category', 'Count']
                
                fig = px.pie(category_counts, values='Count', names='Category', 
                           color_discrete_sequence=px.colors.qualitative.Set3)
                fig.update_layout(height=350)
                st.plotly_chart(fig, use_container_width=True)
    
    # Key Quality Areas (simpler than training recommendations)
    st.markdown("### 🎯 Key Quality Areas to Improve")
    
    if 'issue_category' in filtered_df.columns:
        top_categories = filtered_df['issue_category'].value_counts().head(3)
        
        cols = st.columns(min(3, len(top_categories)))
        icons = ['1️⃣', '2️⃣', '3️⃣']
        
        for i, (cat, count) in enumerate(top_categories.items()):
            if i < len(cols):
                with cols[i]:
                    pct = round(count / len(filtered_df) * 100)
                    st.metric(
                        label=f"{icons[i]} {cat}",
                        value=f"{count} issues",
                        delta=f"{pct}% of total"
                    )


def render_admin_section(df: pd.DataFrame, manual_scores: dict, month_key: str, is_admin: bool):
    if not is_admin:
        st.warning("🔐 **Admin Access Required** - Enter password in sidebar")
        return
    
    # Scoring Rubric
    st.markdown("### 📖 Scoring Rubric")
    st.markdown("""
    Each category is scored **0-5 points**. Total max score = **30 points** (6 categories).
    
    | Score | Meaning |
    |-------|---------|
    | 5 | Excellent - Consistently exceeds expectations |
    | 4 | Very Good - Often exceeds expectations |
    | 3 | Good - Meets expectations |
    | 2 | Fair - Sometimes meets expectations |
    | 1 | Needs Improvement - Rarely meets expectations |
    | 0 | Not Rated / No data |
    """)
    
    with st.expander("📊 **Automated Scores (calculated from data)**"):
        st.markdown("""
        - **Schedule Score**: Based on % of interviews within 14-16 day window
          - 90-100% → 5 pts | 80-89% → 4 pts | 70-79% → 3 pts | 60-69% → 2 pts | <60% → 1 pt
        - **Quality Score**: Based on % of interviews without quality flags
          - 95-100% → 5 pts | 90-94% → 4 pts | 85-89% → 3 pts | 80-84% → 2 pts | <80% → 1 pt
        """)
    
    with st.expander("✏️ **Manual Scores (entered by admin)**"):
        st.markdown("""
        - **Journal**: Quality of journal entries (detail, relevance, insight)
        - **Feedback**: Responsiveness to feedback from supervisors
        - **Team**: Contributions to team (helping others, meetings, initiatives)
        """)
    
    st.divider()
    
    # Reset button
    col1, col2 = st.columns([3, 1])
    with col1:
        st.markdown("### ✏️ Enter Manual Scores")
    with col2:
        if st.button("🔄 Reset All Scores", type="secondary"):
            if month_key in manual_scores:
                del manual_scores[month_key]
                save_manual_scores(manual_scores)
                st.success("Scores reset for this month!")
                st.rerun()
    
    month_scores = manual_scores.get(month_key, {})
    updated = False
    
    for _, row in df.iterrows():
        ra_name = str(row['ra_name']) if 'ra_name' in row else 'Unknown'
        ra_scores = month_scores.get(ra_name, {})
        
        st.markdown(f"#### {ra_name.title()}")
        cols = st.columns(3)
        
        # Score options with descriptions
        journal_options = {
            0: "0 - Not Rated",
            1: "1 - Missing/Very Little",
            2: "2 - Not Good",
            3: "3 - Basic Minimum",
            4: "4 - Good",
            5: "5 - Outstanding"
        }
        
        feedback_options = {
            0: "0 - Not Rated",
            1: "1 - Very Poor",
            2: "2 - Low Effort",
            3: "3 - Some Effort",
            4: "4 - Most Checks Done",
            5: "5 - All Checks Done"
        }
        
        team_options = {
            0: "0 - Not Rated",
            1: "1 - Very Poor",
            2: "2 - Minimal",
            3: "3 - Some Contribution",
            4: "4 - Good Contribution",
            5: "5 - Led Discussion/Active"
        }
        
        with cols[0]:
            journal_val = st.selectbox(
                "Journal Quality", 
                list(journal_options.keys()),
                format_func=lambda x: journal_options[x],
                index=ra_scores.get('journal', 0), 
                key=f"j_{ra_name}"
            )
        with cols[1]:
            feedback_val = st.selectbox(
                "Feedback Response", 
                list(feedback_options.keys()),
                format_func=lambda x: feedback_options[x],
                index=ra_scores.get('feedback', 0), 
                key=f"f_{ra_name}"
            )
        with cols[2]:
            team_val = st.selectbox(
                "Team Contribution", 
                list(team_options.keys()),
                format_func=lambda x: team_options[x],
                index=ra_scores.get('team', 0), 
                key=f"t_{ra_name}"
            )
        
        new_scores = {'journal': journal_val, 'feedback': feedback_val, 'team': team_val}
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


# --------------------------------------------------------------------
# Main App
# --------------------------------------------------------------------
def main():
    inject_css()
    
    with st.sidebar:
        st.markdown("### ⚙️ Settings")
        today = datetime.now()
        year = st.selectbox("Year", [2024, 2025], index=1)
        month = st.selectbox("Month", list(range(1, 13)), index=today.month - 1,
                            format_func=lambda x: datetime(2000, x, 1).strftime('%B'))
        
        month_key = f"{year}_{month:02d}"
        month_name = datetime(year, month, 1).strftime("%B %Y")
        start_date, end_date = get_date_range(month, year)
        st.info(f"📅 {start_date} to {end_date}")
        
        refresh_disabled = not DB_REFRESH_ENABLED
        if refresh_disabled:
            st.info("Live database refresh is disabled. Using cached data. Add DB_CONN in Streamlit secrets or environment to enable.")
        
        if st.button("🔄 Refresh from Database", type="primary", use_container_width=True,
                     disabled=refresh_disabled,
                     help="Requires DB_CONN (env or Streamlit secrets) and a reachable MySQL database."):
            with st.spinner("Running SQL scripts..."):
                df = fetch_all_metrics(start_date, end_date)
                if not df.empty:
                    df = calculate_scores(df)
                    save_cached_data(df)
                    
                    # Also fetch quality data for the issues analysis tab
                    quality_df = run_sql_file(SQL_QUALITY_FILE, start_date, end_date, "Quality Issues")
                    st.session_state['quality_df'] = quality_df
                    
                    st.success(f"✅ Loaded {len(df)} RAs")
                    st.rerun()
                else:
                    st.error("Could not fetch data from database. Showing cached data if available.")
        
        st.divider()
        st.markdown("### 🔐 Admin Access")
        
        # Initialize admin state
        if 'is_admin' not in st.session_state:
            st.session_state.is_admin = False
        
        password = st.text_input("Password", type="password", key="admin_password")
        
        # Check password and update session state
        if password == ADMIN_PASSWORD:
            st.session_state.is_admin = True
        
        if st.session_state.is_admin:
            st.success("✅ Admin access granted")
            if st.button("🚪 Logout", type="secondary"):
                st.session_state.is_admin = False
                st.rerun()
        elif password:
            st.error("Invalid password")
        
        is_admin = st.session_state.is_admin
    
    df = load_cached_data()
    manual_scores = load_manual_scores()
    quality_df = st.session_state.get('quality_df', pd.DataFrame())
    
    if df.empty:
        st.markdown(f'<div class="header-container"><h1>🏆 RA of the Month</h1><p>| {month_name}</p></div>', unsafe_allow_html=True)
        st.warning("No data available. Click 'Refresh from Database' in the sidebar.")
        return
    
    if 'schedule_score' not in df.columns:
        df = calculate_scores(df)
    df = combine_scores(df, manual_scores, month_key)
    
    st.markdown(f'<div class="header-container"><h1>🏆 RA Performance Dashboard</h1><p>| {month_name}</p></div>', unsafe_allow_html=True)
    
    tab1, tab2, tab3, tab4, tab5 = st.tabs([
        "📊 Data Summary", 
        "🏆 RA of the Month", 
        "🔍 Quality Issues",
        "📋 Quality Guidelines", 
        "✏️ Admin"
    ])
    
    with tab1:
        render_data_summary(df, month_name)
    with tab2:
        render_ra_of_month(df, month_name)
    with tab3:
        render_quality_issues(quality_df, month_name)
    with tab4:
        render_quality_guidelines()
    with tab5:
        render_admin_section(df, manual_scores, month_key, is_admin)
    
    st.divider()
    st.download_button("📥 Download CSV", df.to_csv(index=False), "ra_leaderboard.csv", "text/csv", use_container_width=True)


if __name__ == "__main__":
    main()
