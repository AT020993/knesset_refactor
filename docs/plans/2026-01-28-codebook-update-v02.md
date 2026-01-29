# Democratic Erosion Codebook v02 Update Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Update the CAP annotation codebook taxonomy from v01 to v02, adding new codes and restructuring Rights categories.

**Architecture:** This is a data-only update affecting the taxonomy CSV and documentation. The taxonomy service (`taxonomy.py`) dynamically loads from CSV, so no code changes are required. Tests use dynamic lookups, not hardcoded values.

**Tech Stack:** CSV, Markdown documentation, DuckDB taxonomy table, Python/pytest for verification

---

## Summary of Changes

| Type | Code | Old | New |
|------|------|-----|-----|
| ADD | 109 | - | נשיא המדינה (President) |
| CHANGE | 305 | אזרחות תושבות וכניסה | זכות לחירות ופרטיות (Liberty & Privacy) |
| CHANGE | 306 | יהודיות ולאומיות | אזרחות, תושבות וכניסה לארץ (Citizenship) |
| ADD | 307 | - | זכות לקניין (Right to Property) |
| ADD | 308 | - | זכות לחיים (Right to Life) |
| REMOVE | - | יהודיות ולאומיות | Now handled via secondary coding rules |

---

### Task 1: Backup Current Taxonomy

**Files:**
- Read: `data/taxonomies/democratic_erosion_codebook.csv`
- Create: `data/taxonomies/democratic_erosion_codebook_v01_backup.csv`

**Step 1: Create backup copy**

```bash
cp data/taxonomies/democratic_erosion_codebook.csv data/taxonomies/democratic_erosion_codebook_v01_backup.csv
```

**Step 2: Verify backup exists**

```bash
ls -la data/taxonomies/democratic_erosion_codebook_v01_backup.csv
```
Expected: File exists with same size as original

**Step 3: Commit backup**

```bash
git add data/taxonomies/democratic_erosion_codebook_v01_backup.csv
git commit -m "chore: backup codebook v01 before v02 update"
```

---

### Task 2: Update Taxonomy CSV with New Codes

**Files:**
- Modify: `data/taxonomies/democratic_erosion_codebook.csv`

**Step 1: Replace CSV content with updated taxonomy**

The complete updated CSV content (replacing the entire file):

```csv
MajorCode,MajorTopic_HE,MajorTopic_EN,MinorCode,MinorTopic_HE,MinorTopic_EN,Description_HE,Examples_HE
1,מוסדות שלטון,Government Institutions,100,כללי,General,קטגוריית סל למוסדות שלטון,
1,מוסדות שלטון,Government Institutions,101,כנסת,Knesset,"חקיקה המשנה את סמכויות הכנסת, אופן עבודתה, כללי הפיקוח שלה על הממשלה, או מעמדה מול רשויות אחרות","העלאת אחוז החסימה → צמצום ייצוג; חקיקה המגבילה דיון או זמן הסתייגויות → צמצום פיקוח"
1,מוסדות שלטון,Government Institutions,102,ממשלה,Government/Executive,"חקיקה המשפיעה על סמכויות הממשלה והשרים, מבנה הרשות המבצעת, חלוקת סמכויות פנימית, או יחסי הממשלה עם רשויות אחרות","צמצום סמכויות ממשלת מעבר → צמצום כוח הרשות המבצעת"
1,מוסדות שלטון,Government Institutions,103,מערכת המשפט,Judicial System,"חקיקה העוסקת במעמד בתי המשפט, סמכויות שיפוטיות, ביקורת שיפוטית, מינוי שופטים, או עצמאות הרשות השופטת","ביטול עילת הסבירות → צמצום; שינוי הרכב הוועדה לבחירת שופטים → פגיעה בעצמאות; הטלת עונשי מינימום → צמצום שיקול דעת"
1,מוסדות שלטון,Government Institutions,104,יועמ״ש / פרקליטות,Attorney General / Prosecution,"חקיקה המשנה את סמכויות היועץ המשפטי לממשלה והפרקליטות, עצמאותם, מעמדם ביחס לדרג הפוליטי. כוללת מחלקה לחקירות שוטרים, יועמשים משרדיים, פרקליטות צבאית","חוות דעת לא מחייבת → צמצום; פיצול תפקיד היועמש → שינוי מבני; מינוי פוליטי → פגיעה באי-תלות"
1,מוסדות שלטון,Government Institutions,105,שירות המדינה,Civil Service,"חקיקה המשפיעה על שירות המדינה, מינויים מקצועיים, כללי מנהל ציבורי. כולל מבקר המדינה, רשויות המס, ועדות תכנון, חברות ממשלתיות","ביטול דרישת כישורים מקצועיים → החלשה; מינוי מנכל ללא ועדת איתור → צמצום עצמאות"
1,מוסדות שלטון,Government Institutions,106,כוחות הביטחון,Security Forces,"חקיקה המשפיעה על סמכויות צה״ל, משטרה, שב״ס, שב״כ. יחסי דרג פוליטי-פיקודי, סמכויות חקירה ומעצר, אחריותיות וביקורת","הכפפת מינויים בכירים לשיקול פוליטי ישיר → פגיעה בעצמאות מקצועית"
1,מוסדות שלטון,Government Institutions,107,רשויות מקומיות,Local Authorities,"חקיקה המשפיעה על סמכויות הרשויות המקומיות, עצמאות, חלוקת סמכויות מול השלטון המרכזי, ארנונה, פיקוח ממשלתי","הרחבת פיקוח ממשלתי → צמצום אוטונומיה; מינוי ועדה קרואה → פגיעה בשלטון נבחר"
1,מוסדות שלטון,Government Institutions,108,מפלגות,Political Parties,"חקיקה הנוגעת לפעילות מפלגות פוליטיות, מימון, תנאי התמודדות, רישום, הגבלות על פעילותן","הגבלת מימון זר → צמצום חופש פעולה; שינוי נוסחת מימון לטובת מפלגות גדולות → פגיעה בשוויון"
1,מוסדות שלטון,Government Institutions,109,נשיא המדינה,President,חקיקה הנוגעת לסמכויות הנשיא,
2,מוסדות אזרחיים,Civil Institutions,200,כללי,General,קטגוריית סל למוסדות אזרחיים,
2,מוסדות אזרחיים,Civil Institutions,201,תקשורת,Media,"חקיקה המשפיעה על חופש העיתונות, רגולציה של כלי תקשורת, בעלות, פיקוח, צנזורה","רגולטור נשלט ע״י הממשלה → פגיעה; הגבלת ביקורת בזמן חירום → צמצום"
2,מוסדות אזרחיים,Civil Institutions,202,עמותות,NGOs/Civil Society,"חקיקה הנוגעת לארגוני חברה אזרחית, חופש ההתאגדות, מימון (בפרט זר), רגולציה","חובת דיווח מיוחדת לעמותות במימון זר → צמצום; איסור פעילות ציבורית → פגיעה"
2,מוסדות אזרחיים,Civil Institutions,203,חינוך ואקדמיה,Education & Academia,"חקיקה המשפיעה על מערכת החינוך וההשכלה הגבוהה, חופש אקדמי, תכנים לימודיים, עצמאות מוסדות","התערבות פוליטית בתכני לימוד → פגיעה; שלילת תקצוב בשל עמדות פוליטיות → פגיעה בחופש אקדמי"
2,מוסדות אזרחיים,Civil Institutions,204,מוסדות תרבות,Cultural Institutions,"חקיקה הנוגעת לפעילות תרבותית ואמנותית, מימון ציבורי, חופש יצירה, התניות אידאולוגיות","התניית תקצוב בנאמנות למדינה → פגיעה; קביעת קריטריונים לתמיכה → החלשת עצמאות"
3,זכויות,Rights,300,כללי,General,קטגוריית סל לזכויות,
3,זכויות,Rights,301,זכות לבחור ולהיבחר,Right to Vote/Be Elected,"חקיקה המשפיעה על זכויות ההצבעה וההתמודדות בבחירות, תנאי סף, פסילות מועמדים","העלאת גיל הצבעה → צמצום; צמצום קלפיות לאנשים עם מוגבלות → החלשה"
3,זכויות,Rights,302,חופש הביטוי והמחאה,Freedom of Expression/Protest,"חקיקה המשפיעה על חופש הביטוי הפוליטי, האזרחי או התרבותי, הגבלות, פליליזציה","הפללת קריאה לחרם → צמצום; צמצום חסינות ביטוי בהפגנות → צמצום"
3,זכויות,Rights,303,חופש הדת,Freedom of Religion,"חקיקה הנוגעת לחופש פולחן, חופש מדת, כפייה דתית, יחסי דת–מדינה","כפיית נורמות דתיות במרחב הציבורי → פגיעה בחופש מדת; הגבלת חופש פולחן → צמצום"
3,זכויות,Rights,304,שוויון,Equality,"חקיקה העוסקת בעקרון השוויון בפני החוק, איסור אפליה, מתן העדפה לקבוצות על בסיס זהות. קידוד משנה: נשים (1); ערבים (2); להטב (3); חרדים (4)","אפליה מטעמי אמונה → פגיעה; ועדות קבלה ביישובים קטנים → פגיעה"
3,זכויות,Rights,305,זכות לחירות ופרטיות,Right to Liberty & Privacy,"חקיקה הנוגעת למעצר ומאסר וכן לחיפוש, מעקב או פגיעה אחרת בפרטיות","החמרת תנאי שחרור בערבות → פגיעה בחירות; הרחבת סמכויות מעקב שבכ → פגיעה בפרטיות; שלילת זכויות אזרחיות מאסירים → פגיעה בחירות"
3,זכויות,Rights,306,אזרחות תושבות וכניסה,Citizenship/Residency/Entry,"חקיקה הנוגעת למתן או שלילת אזרחות ותושבות, מדיניות הגירה, איחוד משפחות, וכניסה לישראל","הגבלת איחוד משפחות → פגיעה; הגבלת מתן אזרחות למהגרים לא יהודים → צמצום"
3,זכויות,Rights,307,זכות לקניין,Right to Property,"חקיקה הפוגעת ברכושו של אדם או ביכולת של אדם להשתמש ברכושו","הפקעת קרקעות; הריסת בתים; חילוט כלי נשק; קביעת מחירים מקסימליים"
3,זכויות,Rights,308,זכות לחיים,Right to Life,חקיקה הנוגעת לעונש מוות,
```

**Step 2: Verify CSV structure**

```bash
cd /Users/amir/Library/CloudStorage/OneDrive-Personal/Work/Research\ Assistant/knesset_refactor && head -5 data/taxonomies/democratic_erosion_codebook.csv && echo "..." && tail -5 data/taxonomies/democratic_erosion_codebook.csv
```
Expected: Header row + Government codes at start, Rights codes 306-308 at end

**Step 3: Count rows (should be 22 including header)**

```bash
wc -l data/taxonomies/democratic_erosion_codebook.csv
```
Expected: 22 lines (header + 21 codes)

---

### Task 3: Update CAP Annotation Guide Documentation

**Files:**
- Modify: `docs/CAP_ANNOTATION_GUIDE.md:19-51` (Codebook Structure section)

**Step 1: Update Government Institutions table**

Change line 19 from:
```markdown
### 1. Government Institutions (מוסדות שלטון) - Codes 100-108
```
To:
```markdown
### 1. Government Institutions (מוסדות שלטון) - Codes 100-109
```

Add after line 30 (after 108 entry):
```markdown
| 109 | נשיא המדינה | President |
```

**Step 2: Update Rights section**

Change line 41 from:
```markdown
### 3. Rights (זכויות) - Codes 300-306
```
To:
```markdown
### 3. Rights (זכויות) - Codes 300-308
```

Replace lines 49-50 (codes 305-306) with:
```markdown
| 305 | זכות לחירות ופרטיות | Right to Liberty & Privacy |
| 306 | אזרחות, תושבות וכניסה | Citizenship/Residency/Entry |
| 307 | זכות לקניין | Right to Property |
| 308 | זכות לחיים | Right to Life |
```

**Step 3: Update coding guidelines (lines 152-159)**

Replace lines 154-159 with updated rules reflecting v02 codebook:
```markdown
Key rules for coding:
1. Each bill is coded to ONE category only
2. Code by the means (institution/right mentioned) not ultimate target
3. If a bill affects both an institution and a right, code by the institution
4. Increasing oversight of an authority = reducing its powers
5. Bills combining equality issues with other rights → code under "Equality" with sub-code: Women (1), Arabs (2), LGBTQ (3), Ultra-Orthodox (4)
6. Bills with Jewish/national identity elements → add secondary "Jewish-National Identity" coding
```

---

### Task 4: Reload Taxonomy and Verify in Database

**Files:**
- Test: Database table `UserCAPTaxonomy`

**Step 1: Create verification script**

```python
# verify_taxonomy.py (run with: python verify_taxonomy.py)
import sys
sys.path.insert(0, 'src')
from pathlib import Path
from ui.services.cap.taxonomy import CAPTaxonomyService

db_path = Path('data/warehouse.duckdb')
service = CAPTaxonomyService(db_path)
service.ensure_tables_exist()
service.load_taxonomy_from_csv()

taxonomy = service.get_taxonomy()
print(f"Total categories: {len(taxonomy)}")
print("\nGovernment Institutions (100s):")
gov = taxonomy[taxonomy['MinorCode'].between(100, 199)]
for _, row in gov.iterrows():
    print(f"  {row['MinorCode']}: {row['MinorTopic_HE']} ({row['MinorTopic_EN']})")

print("\nRights (300s):")
rights = taxonomy[taxonomy['MinorCode'].between(300, 399)]
for _, row in rights.iterrows():
    print(f"  {row['MinorCode']}: {row['MinorTopic_HE']} ({row['MinorTopic_EN']})")
```

**Step 2: Run verification**

```bash
cd /Users/amir/Library/CloudStorage/OneDrive-Personal/Work/Research\ Assistant/knesset_refactor && source .venv/bin/activate && PYTHONPATH=src python verify_taxonomy.py
```
Expected output:
- Total categories: 21
- Code 109 appears under Government Institutions
- Codes 305-308 appear under Rights with new names

**Step 3: Clean up verification script**

```bash
rm verify_taxonomy.py
```

---

### Task 5: Run Tests to Verify No Regressions

**Files:**
- Test: `tests/test_cap_*.py`

**Step 1: Run CAP-related tests**

```bash
cd /Users/amir/Library/CloudStorage/OneDrive-Personal/Work/Research\ Assistant/knesset_refactor && source .venv/bin/activate && pytest tests/test_cap_services.py tests/test_cap_integration.py -v --tb=short
```
Expected: All tests pass (tests use dynamic category lookups, not hardcoded values)

**Step 2: Run full test suite**

```bash
pytest tests/ --ignore=tests/test_api_integration.py --ignore=tests/test_e2e.py --ignore=tests/test_data_pipeline_integration.py --ignore=tests/test_connection_leaks.py --tb=short -q
```
Expected: All tests pass

---

### Task 6: Commit All Changes

**Files:**
- Modified: `data/taxonomies/democratic_erosion_codebook.csv`
- Modified: `docs/CAP_ANNOTATION_GUIDE.md`
- Created: `data/taxonomies/democratic_erosion_codebook_v01_backup.csv`

**Step 1: Stage changes**

```bash
git add data/taxonomies/democratic_erosion_codebook.csv docs/CAP_ANNOTATION_GUIDE.md
```

**Step 2: Create commit**

```bash
git commit -m "feat: update Democratic Erosion codebook to v02

Changes:
- Add code 109 (President/נשיא המדינה)
- Add code 305 (Liberty & Privacy/זכות לחירות ופרטיות)
- Move Citizenship to code 306 (was 305)
- Add code 307 (Property/זכות לקניין)
- Add code 308 (Right to Life/זכות לחיים)
- Remove separate Jewish/National Identity category (now secondary coding)
- Update coding guidelines per v02 codebook

Co-Authored-By: Claude <noreply@anthropic.com>"
```

---

### Task 7: Manual UI Verification (Optional)

**Step 1: Launch the application**

```bash
cd /Users/amir/Library/CloudStorage/OneDrive-Personal/Work/Research\ Assistant/knesset_refactor && source .venv/bin/activate && streamlit run src/ui/data_refresh.py --server.port 8501
```

**Step 2: Navigate to CAP Annotation section**

1. Open browser to http://localhost:8501
2. Navigate to "קידוד הצעות חוק" section
3. Login with researcher credentials

**Step 3: Verify new categories appear**

1. Select a bill
2. In Major Category dropdown, select "מוסדות שלטון"
3. Verify code 109 (נשיא המדינה) appears in Minor Category
4. Select "זכויות"
5. Verify codes 305-308 appear with new names

**Step 4: Stop application**

Press Ctrl+C in terminal

---

## Post-Implementation Notes

1. **No migration needed**: Database verified to have no annotations using codes 305-308
2. **Tests are robust**: All CAP tests use dynamic category lookups from taxonomy, not hardcoded values
3. **Backup preserved**: v01 backup saved for rollback if needed
4. **Secondary coding**: יהודיות ולאומיות is now documented as secondary coding, not a separate category. Consider implementing secondary code selection in UI (future enhancement)
