# CAP Annotation Simplification

**Date:** 2026-01-29
**Status:** Approved
**Author:** Claude (with Amir)

## Background

Feedback from researcher meeting identified two improvements to the CAP annotation system:
1. The Direction field (+1/-1/0) is not needed for their research
2. Finding specific bills to edit is difficult with many annotations

## Changes

### Change 1: Remove Direction Field Completely

Remove the Direction field from both UI and database - clean removal, not just hiding.

**Affected Components:**

| File | Changes |
|------|---------|
| `coded_bills_renderer.py` | Remove Direction from `_render_bills_table()` display columns, remove Direction display in `_render_edit_section()`, remove Direction radio buttons from `_render_edit_form()` |
| `form_renderer.py` | Remove Direction radio buttons from new annotation form |
| `cap_service.py` | Remove `direction` parameter from `save_annotation()` method signature |
| `repository.py` | Update INSERT/UPDATE SQL to exclude Direction column |
| `taxonomy.py` | Add migration function to recreate table without Direction |

**Database Migration (safe table swap):**

```sql
-- 1. Create new table without Direction column
CREATE TABLE UserBillCAP_new (
    AnnotationID INTEGER PRIMARY KEY DEFAULT nextval('seq_annotation_id'),
    BillID INTEGER NOT NULL,
    ResearcherID INTEGER NOT NULL,
    CAPMinorCode INTEGER NOT NULL,
    AssignedDate TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    Confidence VARCHAR DEFAULT 'Medium',
    Notes VARCHAR,
    Source VARCHAR DEFAULT 'Database',
    SubmissionDate VARCHAR,
    UNIQUE(BillID, ResearcherID)
);

-- 2. Copy data (excluding Direction)
INSERT INTO UserBillCAP_new
SELECT AnnotationID, BillID, ResearcherID, CAPMinorCode, AssignedDate,
       Confidence, Notes, Source, SubmissionDate
FROM UserBillCAP;

-- 3. Verify counts match
-- 4. Swap tables
DROP TABLE UserBillCAP;
ALTER TABLE UserBillCAP_new RENAME TO UserBillCAP;
```

### Change 2: Add Bill ID Search Bar

Add a search input to quickly find annotations by Bill ID in the Edit Annotations section.

**UI Design:**

```
┌─────────────────────────────────────────────────┐
│ ✏️ Edit Annotation                              │
├─────────────────────────────────────────────────┤
│ 🔍 Search by Bill ID: [________]                │  ← NEW
│                                                 │
│ Select annotation to edit: [dropdown ▼]         │  ← Filtered
└─────────────────────────────────────────────────┘
```

**Behavior:**

| Search Input | Result |
|--------------|--------|
| Empty | Show all annotations (current behavior) |
| `12345` | Filter to bills where ID contains "12345" |
| No match | Show "No annotations found for Bill ID: X" |

**Implementation:** Add `st.text_input()` in `_render_edit_section()` before the selectbox, filter the `coded_bills` DataFrame based on input.

## Files to Modify

1. `src/ui/renderers/cap/coded_bills_renderer.py` - Remove Direction display, add search
2. `src/ui/renderers/cap/form_renderer.py` - Remove Direction from new annotation form
3. `src/ui/services/cap_service.py` - Remove direction parameter
4. `src/ui/services/cap/repository.py` - Update SQL queries
5. `src/ui/services/cap/taxonomy.py` - Add migration function

## Testing

- Verify existing annotations are preserved after migration
- Verify new annotations save without Direction
- Verify search filters correctly by Bill ID
- Verify empty search shows all annotations
