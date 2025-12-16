"""Generate cached data for cloud deployment."""
import sys
sys.path.insert(0, '.')

from app import fetch_all_metrics, calculate_scores, save_cached_data, get_date_range
from datetime import datetime

# Get current month data
today = datetime.now()
month = today.month
year = today.year

start_date, end_date = get_date_range(month, year)
print(f"Fetching data for {start_date} to {end_date}...")

df = fetch_all_metrics(start_date, end_date)
if not df.empty:
    df = calculate_scores(df)
    save_cached_data(df)
    print(f"SUCCESS: Cached {len(df)} RAs to data/cached_leaderboard.csv")
    print(df[['ra_name', 'total_interviews', 'pct_complete', 'pct_no_quality_flags']].to_string())
else:
    print("ERROR: No data returned")
