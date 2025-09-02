# 🏛️ Knesset Research Platform - Quick Start Guide

*A simple guide for researchers with no coding experience*

---

## 🎯 What This Tool Does

The Knesset Research Platform gives you easy access to Israeli parliamentary data through a web browser. You can:

- 📊 **Browse data tables** - View information about MKs, bills, queries, and committees
- 🔍 **Run analytical queries** - Use pre-built searches to find specific information  
- 📈 **Create visualizations** - Generate charts and graphs automatically
- 📥 **Export data** - Download results to Excel or CSV files
- 🕒 **Track historical trends** - Analyze patterns over time

**No coding required** - everything works through a simple web interface!

---

## 🚀 First Time Setup (5 minutes)

### Step 1: Run the Setup
1. **Find the setup file**: Look for `setup_for_researcher.py` in your project folder
2. **Double-click it** OR open Terminal/Command Prompt and type:
   ```
   python setup_for_researcher.py
   ```
3. **Wait for completion**: The setup will automatically:
   - Check your Python installation
   - Install required software
   - Download sample data
   - Create a desktop shortcut

### Step 2: Verify Setup
You should see:
- ✅ All steps completed successfully
- 🖥️ Desktop shortcut created (if possible)
- 📁 New folders appeared in your project directory

---

## 🖥️ Daily Usage - Starting the Platform

### Option A: Desktop Shortcut (Easiest)
1. **Double-click** "Knesset Research Platform" on your desktop
2. **Click** "🚀 Start Research Platform" in the window that opens
3. **Wait** for your web browser to open automatically

### Option B: Manual Launch
1. **Find** `researcher_launcher.py` in your project folder
2. **Double-click it** OR run: `python researcher_launcher.py`
3. **Click** "🚀 Start Research Platform"

### Option C: Command Line (Alternative)
1. **Open Terminal/Command Prompt**
2. **Navigate** to your project folder
3. **Type**: `python launch_knesset.py`

---

## 🌐 Using the Web Interface

Once started, your browser will open to `http://localhost:8501`

### Main Sections:

#### 📊 **Data Tables**
- Browse raw parliamentary data
- Filter by Knesset term, faction, etc.
- Export filtered results

#### 🔍 **Predefined Queries** 
Pre-built searches for common research needs:
- **Bills + Full Details** - Complete bill information with committee sessions
- **Queries + Full Details** - Parliamentary questions with ministry responses
- **Agenda Items + Full Details** - Meeting agenda analysis

#### 📈 **Visualizations**
Ready-made charts organized by topic:
- **Query Analytics** - Response times, ministry performance
- **Bills Analytics** - Bill status, faction activity
- **Agenda Analytics** - Meeting patterns, classifications  
- **Network Analysis** - Collaboration between MKs

#### 💻 **Custom SQL** (Advanced)
- Write your own database queries
- For experienced users only

---

## 📋 Common Research Tasks

### Finding Bill Information
1. **Go to** "Predefined Queries"
2. **Select** "Bills + Full Details"
3. **Choose** Knesset number (25 = current)
4. **Click** "Execute Query"
5. **Use filters** to narrow results
6. **Export** to Excel for further analysis

### Analyzing Ministry Response Times
1. **Go to** "Predefined Visualizations"
2. **Select** "Query Analytics" topic
3. **Choose** "Response Times by Ministry"
4. **Filter** by date range or coalition status
5. **View** interactive charts

### Tracking MK Activity
1. **Go to** "Data Tables"
2. **Browse** "KNS_PersonToPosition" table
3. **Filter** by KnessetNum and FactionID
4. **Export** results for external analysis

### Committee Session Analysis
1. **Go to** "Predefined Queries"  
2. **Select** "Bills + Full Details"
3. **Look for** "BillCommitteeSessions" column
4. **Filter** where sessions > 0
5. **Sort** by session count to find most-discussed bills

---

## 📥 Exporting Your Data

### From Query Results:
1. **Run any query** in "Predefined Queries"
2. **Scroll down** to see results table
3. **Click** "Download CSV" or "Download Excel"
4. **Choose** file location to save

### From Visualizations:
1. **Open any chart**
2. **Click** the camera icon (top-right of chart)
3. **Choose** PNG, SVG, or PDF format
4. **Save** to your computer

---

## 🔧 Troubleshooting

### "Platform won't start"
- ✅ **Check:** Did you run the setup script first?
- ✅ **Try:** Run `python setup_for_researcher.py` again
- ✅ **Verify:** Python 3.8+ is installed on your computer

### "Browser doesn't open automatically"
- 🌐 **Manual:** Go to `http://localhost:8501` in any browser
- 🔄 **Restart:** Close and reopen the launcher

### "No data showing"
- 📊 **First time:** Platform may need to download data
- 🔄 **Refresh:** Go to "Data Refresh" section and update tables
- ⏰ **Wait:** Large downloads can take 10-30 minutes

### "Connection errors"
- 🌐 **Internet:** Ensure stable internet connection
- 🔒 **Firewall:** Check if firewall blocks localhost connections
- 🔄 **Restart:** Stop and restart the platform

---

## 💡 Tips for Effective Research

### Data Organization
- **Export regularly** - Save your findings to files
- **Use descriptive names** - "Knesset25_Bills_Analysis.xlsx"
- **Document filters** - Note which settings you used

### Analysis Workflow  
1. **Start broad** - Look at overall patterns first
2. **Narrow down** - Use filters to focus on specific areas
3. **Cross-reference** - Compare multiple data sources
4. **Visualize** - Charts often reveal hidden patterns

### Time Management
- **Download overnight** - Large data updates can take time
- **Save interim results** - Don't lose work by closing browser
- **Bookmark queries** - Note useful filter combinations

---

## 🆘 Getting Help

### Built-in Help
- **In the launcher:** Click "❓ Help & Documentation"
- **In web interface:** Look for help icons and tooltips
- **Activity log:** Check launcher window for error messages

### Data Sources
- **Official source:** Knesset OData API (`http://knesset.gov.il/Odata/ParliamentInfo.svc`)
- **Documentation:** Check `docs/KnessetOdataManual.pdf` in project folder

### Common Questions

**Q: How current is the data?**  
A: Data is fetched from the official Knesset API and updated when you run data refresh

**Q: Can I use this offline?**  
A: Once data is downloaded, basic analysis works offline. New data requires internet connection.

**Q: Is this data official?**  
A: Yes, all data comes directly from the official Israeli Knesset database

**Q: Can I share my results?**  
A: Yes, exported files can be shared freely. The tool itself requires setup on each computer.

---

## 🔒 Data Privacy & Security

- **Local processing:** All analysis happens on your computer
- **No data upload:** Your research stays private
- **Official source:** Data comes only from public Knesset APIs
- **No tracking:** The platform doesn't collect usage statistics

---

## ⚙️ Technical Notes

### System Requirements
- **Python 3.8+** (usually pre-installed on Mac/Linux)
- **4GB RAM** recommended for large datasets
- **2GB free disk space** for data storage
- **Internet connection** for data updates

### File Structure (Reference)
```
📁 Your Project Folder/
├── 🐍 researcher_launcher.py     # Desktop launcher (main)
├── ⚙️ setup_for_researcher.py    # One-time setup script  
├── 🚀 launch_knesset.py          # Command line launcher
├── 📖 RESEARCHER_GUIDE.md        # This guide
├── 📁 data/                      # Downloaded data storage
└── 📁 src/                       # Application code (don't modify)
```

### URL Reference
- **Main interface:** `http://localhost:8501`
- **Stop platform:** Close launcher window or press Ctrl+C in terminal

---

*This guide covers the essential features for non-technical researchers. The platform includes many advanced capabilities accessible through the web interface.*

**Happy researching! 🎓📊**