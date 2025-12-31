# CAP Bill Annotation System
## Democratic Erosion Bill Classification (קידוד הצעות חוק: דעיכה דמוקרטית)

This module provides a research annotation system for classifying Knesset bills according to the Democratic Erosion codebook. It allows authorized researchers to systematically code legislation based on its potential impact on democratic institutions and rights.

## Overview

The system enables researchers to:
- **Browse uncoded bills** from the Knesset database
- **Classify bills** using a structured taxonomy
- **Track coding direction** (+1 strengthening, -1 weakening, 0 neutral)
- **Export annotations** for analysis
- **Monitor progress** through a statistics dashboard

## Codebook Structure

The Democratic Erosion codebook has three major categories:

### 1. Government Institutions (מוסדות שלטון) - Codes 100-108
| Code | Hebrew | English |
|------|--------|---------|
| 100 | כללי | General |
| 101 | כנסת | Knesset (Parliament) |
| 102 | ממשלה | Government/Executive |
| 103 | מערכת המשפט | Judicial System |
| 104 | יועמ"ש / פרקליטות | Attorney General / Prosecution |
| 105 | שירות המדינה | Civil Service |
| 106 | כוחות הביטחון | Security Forces |
| 107 | רשויות מקומיות | Local Authorities |
| 108 | מפלגות | Political Parties |

### 2. Civil Institutions (מוסדות אזרחיים) - Codes 200-204
| Code | Hebrew | English |
|------|--------|---------|
| 200 | כללי | General |
| 201 | תקשורת | Media |
| 202 | עמותות | NGOs/Civil Society |
| 203 | חינוך ואקדמיה | Education & Academia |
| 204 | מוסדות תרבות | Cultural Institutions |

### 3. Rights (זכויות) - Codes 300-306
| Code | Hebrew | English |
|------|--------|---------|
| 300 | כללי | General |
| 301 | זכות לבחור ולהיבחר | Right to Vote/Be Elected |
| 302 | חופש הביטוי והמחאה | Freedom of Expression/Protest |
| 303 | חופש הדת | Freedom of Religion |
| 304 | שוויון | Equality |
| 305 | אזרחות, תושבות וכניסה | Citizenship/Residency/Entry |
| 306 | יהודיות ולאומיות | Jewishness & Nationalism |

### Direction Coding
| Code | Hebrew | English | Description |
|------|--------|---------|-------------|
| +1 | הרחבה/חיזוק | Strengthening | Bill expands/strengthens the institution or right |
| -1 | צמצום/פגיעה | Weakening | Bill restricts/weakens the institution or right |
| 0 | אחר | Other/Neutral | Bill doesn't clearly affect the institution or right |

## Setup Instructions

### 1. Enable the Feature

Add the following to your `.streamlit/secrets.toml` file:

```toml
[cap_annotation]
enabled = true
password = "your-secure-password"
researcher_name = "Dr. Your Name"
```

### 2. Configure Multiple Researchers (Optional)

For multiple researchers, each needs their own secrets configuration. On Streamlit Cloud, you can update the secrets through the dashboard. For local development, each researcher can have their own `secrets.toml`.

### 3. Cloud Sync

Annotations are stored in the DuckDB database and automatically sync to Google Cloud Storage (if configured). This means:
- Annotations persist between sessions
- Data is backed up to the cloud
- Multiple researchers can share annotations (with appropriate coordination)

## Usage

### Logging In

1. Navigate to the "קידוד הצעות חוק" section in the app
2. Enter the password configured in `secrets.toml`
3. You'll see a confirmation with your researcher name

### Coding Bills

1. **Select a Bill**: Browse the uncoded bills queue
2. **Review the Bill**: Click the link to view the full bill on the Knesset website
3. **Choose Category**: Select the major and minor category
4. **Set Direction**: Choose +1, -1, or 0
5. **Add Submission Date**: Required for bills coded +1 or -1
6. **Save**: Click save to record the annotation

### Viewing Coded Bills

The "Coded Bills" tab shows all previously coded bills with:
- Filter by Knesset number
- Filter by CAP code
- Export to CSV functionality

### Statistics Dashboard

The statistics tab provides:
- Total bills coded vs. total available
- Coding percentage
- Breakdown by category
- Breakdown by direction
- Progress by Knesset

## Database Schema

### UserCAPTaxonomy
Stores the codebook taxonomy:
- `MajorCode`: Major category (1, 2, or 3)
- `MajorTopic_HE/EN`: Major category names
- `MinorCode`: Minor category code (101-306)
- `MinorTopic_HE/EN`: Minor category names
- `Description_HE`: Detailed description
- `Examples_HE`: Example classifications

### UserBillCAP
Stores bill annotations:
- `BillID`: Reference to KNS_Bill
- `CAPMinorCode`: Selected minor category
- `Direction`: +1, -1, or 0
- `AssignedBy`: Researcher name
- `AssignedDate`: Timestamp
- `Confidence`: High/Medium/Low
- `Notes`: Optional researcher notes
- `SubmissionDate`: Bill submission date

## Files

```
src/
├── ui/
│   ├── services/
│   │   └── cap_service.py           # Backend service for CAP operations
│   └── renderers/
│       └── cap_annotation_page.py   # Streamlit UI for annotation
data/
└── taxonomies/
    └── democratic_erosion_codebook.csv  # Editable codebook CSV
```

## Coding Guidelines (from original codebook)

Key rules for coding:
1. Each bill is coded to ONE category only
2. If a bill involves both weakening and strengthening, code by the weakening effect
3. If a bill affects both an institution and a right, code by the institution
4. Bills combining "Jewish state" elements with other aspects → code under "Jewishness"
5. Bills combining equality issues with other rights → code under "Equality"

## Troubleshooting

### "Annotation System Not Enabled"
- Check that `[cap_annotation]` section exists in `secrets.toml`
- Ensure `enabled = true` is set

### "Incorrect Password"
- Verify the password in `secrets.toml`
- Check for trailing spaces in the password

### "Error loading taxonomy"
- The taxonomy CSV may not be loaded
- Check that `data/taxonomies/democratic_erosion_codebook.csv` exists
- Restart the app to reinitialize

### Annotations Not Saving
- Check database write permissions
- Verify the database path is correct
- Check the logs for specific errors

## Export Format

The CSV export includes:
- Bill information (ID, name, Knesset number, type)
- CAP classification (major code, minor code, category names)
- Direction (numeric and Hebrew text)
- Metadata (assigned by, date, confidence, notes)
- Submission date

This format is compatible with statistical analysis tools and can be imported into Excel, R, Python, etc.
