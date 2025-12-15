# RA of the Month Dashboard

## Overview

A unified dashboard for tracking RA performance with:
- **Public view**: Leaderboard visible to all RAs and team
- **Admin view**: Password-protected section for entering manual scores

## Features

- 🔄 **Database refresh**: Click to pull latest automated scores
- 🔐 **Admin section**: Password-protected manual score entry
- 📊 **Leaderboard**: Rankings with color-coded scores
- 📈 **Charts**: Visual score breakdowns
- 📥 **Export**: Download as CSV or Excel

## Scoring (Total: 25 points)

| Category | Max | Source |
|----------|-----|--------|
| Schedule | 5 | Automated (% within 14-16 days) |
| Quality | 5 | Automated (% no flags) |
| Journal | 5 | Manual (admin enters) |
| Feedback | 5 | Manual (admin enters) |
| Team | 5 | Manual (admin enters) |

## Files

```
05_ra_dashboard/
├── app.py              # Main dashboard
├── requirements.txt    # Python dependencies
├── README.md           # This file
└── data/               # Auto-created
    ├── cached_leaderboard.csv    # Cached DB data
    └── manual_scores.json        # Manual scores
```

## Local Usage

```bash
# Install dependencies
pip install -r requirements.txt

# Run dashboard
streamlit run app.py
```

## Admin Access

Default password: `hfd2025`

To change: Set `ADMIN_PASSWORD` environment variable or in Streamlit secrets.

## Streamlit Cloud Deployment

1. Push this folder to GitHub
2. Go to [share.streamlit.io](https://share.streamlit.io)
3. Connect your repository
4. Set secrets:
   ```toml
   ADMIN_PASSWORD = "your_secure_password"
   ```
5. Deploy!

## Notes

- **Database connection**: Works locally when MySQL is running
- **Streamlit Cloud**: Uses cached data (refresh won't work without cloud DB)
- **Manual scores**: Stored in `data/manual_scores.json` - commit to persist
