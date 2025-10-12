# 🏛️ Knesset Research Platform - Quick Start Guide

*Simple guide for researchers with no coding experience*

---

## 🎯 What This Tool Does

Access Israeli parliamentary data through a web browser:
- 📊 **Browse data tables** - MKs, bills, queries, committees
- 🔍 **Run analytical queries** - Pre-built searches
- 📈 **Create visualizations** - Automated charts and graphs
- 📥 **Export data** - Download to Excel or CSV
- 🕒 **Track historical trends** - Analyze patterns over time

**No coding required** - simple web interface!

---

## 🚀 First Time Setup (5 minutes)

1. **Run setup**: Double-click `setup_for_researcher.py` or run `python setup_for_researcher.py`
2. **Wait**: Auto-installs software, downloads sample data, creates desktop shortcut
3. **Verify**: ✅ All steps completed, desktop shortcut created, new folders appeared

---

## 🖥️ Daily Usage

**Launch Options:**
- **Desktop Shortcut**: Double-click "Knesset Research Platform" → Click "🚀 Start Research Platform"
- **Manual**: Double-click `researcher_launcher.py` → Click "🚀 Start Research Platform"
- **Command Line**: `python launch_knesset.py`

Browser opens automatically to `http://localhost:8501`

---

## 🌐 Web Interface Sections

**📊 Data Tables**: Browse raw data, filter by Knesset/faction, export results
**🔍 Predefined Queries**: Pre-built searches (Bills, Queries, Agenda Items with full details)
**📈 Visualizations**: Ready-made charts (Query Analytics, Bills Analytics, Agenda Analytics, Network Analysis)
**💻 Custom SQL**: Write custom queries (advanced users)

---

## 📋 Common Tasks

### Finding Bill Information
1. Go to "Predefined Queries" → "Bills + Full Details"
2. Choose Knesset number (25 = current)
3. Execute Query → Use filters → Export to Excel

### Analyzing Ministry Response Times
1. "Predefined Visualizations" → "Query Analytics" → "Response Times by Ministry"
2. Filter by date range or coalition status → View interactive charts

### Tracking MK Activity
1. "Data Tables" → "KNS_PersonToPosition"
2. Filter by KnessetNum and FactionID → Export results

### Committee Session Analysis
1. "Predefined Queries" → "Bills + Full Details"
2. Filter where "BillCommitteeSessions" > 0
3. Sort by session count to find most-discussed bills

---

## 📥 Exporting Data

**Query Results**: Run query → Scroll to results → Click "Download CSV" or "Download Excel"
**Visualizations**: Open chart → Click camera icon → Choose PNG/SVG/PDF → Save

---

## 🔧 Troubleshooting

**Platform won't start**
- Run `python setup_for_researcher.py` again
- Verify Python 3.8+ installed

**Browser doesn't open**
- Manually go to `http://localhost:8501`
- Restart launcher

**No data showing**
- Go to "Data Refresh" section and update tables
- Large downloads take 10-30 minutes

**Connection errors**
- Check stable internet connection
- Check firewall settings for localhost
- Restart platform

---

## 💡 Research Tips

**Data Organization**: Export regularly with descriptive names ("Knesset25_Bills_Analysis.xlsx"), document filters
**Analysis Workflow**: Start broad → narrow with filters → cross-reference sources → visualize patterns
**Time Management**: Download overnight, save interim results, bookmark useful queries

---

## 🆘 Getting Help

**Built-in Help**: Launcher "❓ Help & Documentation", web interface help icons
**Data Source**: Knesset OData API (`http://knesset.gov.il/Odata/ParliamentInfo.svc`)
**Documentation**: `docs/KnessetOdataManual.pdf`

### FAQ

**Q: How current is the data?**
A: Fetched from official Knesset API, updated when you run data refresh

**Q: Can I use offline?**
A: Yes for basic analysis after download. New data requires internet.

**Q: Is this data official?**
A: Yes, directly from official Israeli Knesset database

**Q: Can I share results?**
A: Yes, exported files are shareable. Tool requires setup on each computer.

---

## 🔒 Data Privacy

- **Local processing** - Analysis on your computer
- **No data upload** - Research stays private
- **Official source** - Public Knesset APIs only
- **No tracking** - No usage statistics collected

---

## ⚙️ Technical Notes

**Requirements**: Python 3.8+, 4GB RAM, 2GB disk space, internet for updates

**File Structure**:
```
📁 Project Folder/
├── 🐍 researcher_launcher.py     # Desktop launcher
├── ⚙️ setup_for_researcher.py    # Setup script
├── 🚀 launch_knesset.py          # CLI launcher
├── 📁 data/                      # Downloaded data
└── 📁 src/                       # Application code
```

**URLs**: Main interface `http://localhost:8501`, Stop: Close launcher or Ctrl+C

---

**Happy researching! 🎓📊**
