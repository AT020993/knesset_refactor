# Streamlit Cloud QA Checklist

Manual testing checklist for verifying the CAP annotation platform before researcher access.

## Pre-Deployment Verification

- [ ] All secrets configured in Streamlit Cloud dashboard
- [ ] GCS credentials using `credentials_base64` format (not multi-line)
- [ ] Bootstrap admin password is secure (not default `knesset2026`)
- [ ] Database uploaded to GCS bucket via `python upload_to_gcs.py --bucket YOUR_BUCKET`
- [ ] Taxonomy CSV files present (`data/taxonomies/democratic_erosion_codebook.csv`)

## Authentication Flow

- [ ] Login page loads without errors
- [ ] Can select researcher from dropdown
- [ ] Correct password allows login
- [ ] Wrong password shows error message (not crash)
- [ ] Session persists across page refreshes
- [ ] Session expires after 2 hours (test by modifying `cap_login_time` in session state)
- [ ] Logout button clears session completely
- [ ] Deactivated user cannot access after admin deactivation (shows "account deactivated" message)

## Annotation Workflow (New Annotation Tab)

- [ ] Bill queue loads with uncoded bills
- [ ] Search filter works (by Bill ID or Name)
- [ ] Knesset filter works
- [ ] "Show my annotated bills" toggle works
- [ ] PDF documents load and display embedded
- [ ] PDF load failure shows specific error message (not blank)
- [ ] PDF fallback link opens document in new tab
- [ ] Category selector shows all 3 major categories
- [ ] Minor categories filter based on major selection
- [ ] Direction radio buttons work (+1, -1, 0)
- [ ] Confidence dropdown works (High/Medium/Low)
- [ ] Notes textarea accepts input
- [ ] Save button saves annotation
- [ ] Success message "✅ Annotation saved successfully!" appears
- [ ] Cloud sync status shown (success or warning if sync failed)
- [ ] Bill removed from queue after annotation

## API Fetch Tab

- [ ] Search by bill name returns results
- [ ] Empty search shows "No bills found matching your search" (info message)
- [ ] API timeout shows specific error message (not empty results)
- [ ] API network error shows specific error message
- [ ] Can annotate API-fetched bill
- [ ] Annotated bill removed from API results list

## Coded Bills Tab (View/Edit)

- [ ] Shows bills previously annotated by current researcher
- [ ] Knesset filter works
- [ ] CAP category filter works
- [ ] "Show all annotations" toggle works (admin only sees all researchers)
- [ ] Can edit existing annotation
- [ ] Edit shows current values pre-filled
- [ ] Can delete annotation (with confirmation dialog)
- [ ] Multi-annotator: can see other researchers' annotations in expandable section
- [ ] Annotation count badges show correctly

## Statistics Tab

- [ ] Summary metrics display correctly (Bills Coded / Total / Coverage %)
- [ ] Category breakdown chart renders
- [ ] Direction distribution chart renders
- [ ] Coverage breakdown by Knesset displays
- [ ] Zero annotations case handled gracefully (no division by zero errors)

## Admin Panel (Admin Users Only)

- [ ] Only visible to users with role='admin'
- [ ] User list displays all users with correct info
- [ ] Can add new researcher (username, display name, password, role)
- [ ] Duplicate username shows clear error message
- [ ] Short password (<6 chars) shows error
- [ ] Invalid username format shows error
- [ ] Can reset user password
- [ ] Can change user role (researcher ↔ admin)
- [ ] Can update display name
- [ ] Can deactivate user (soft delete)
- [ ] Cannot delete self (prevented with message)
- [ ] Cannot permanently delete user with annotations (must deactivate)
- [ ] Annotation count per user displays correctly

## Cloud Storage & Data Persistence

- [ ] Check logs for "☁️ Cloud storage: ENABLED" message at startup
- [ ] Create test annotation
- [ ] Wait for cloud sync to complete
- [ ] Force app restart (Settings → Reboot app in Streamlit Cloud)
- [ ] Verify annotation persists after restart
- [ ] If sync fails, warning message shown to user

## Performance (Streamlit Cloud Free Tier)

- [ ] Initial load completes in < 30 seconds
- [ ] Page navigation < 5 seconds
- [ ] Annotation save < 3 seconds
- [ ] No memory errors in Streamlit Cloud logs
- [ ] App doesn't crash on repeated use

## Error Recovery

- [ ] App recovers from database connection error
- [ ] App recovers from GCS connection error
- [ ] Browser refresh doesn't lose session (within 2-hour timeout)
- [ ] Multiple browser tabs work correctly (session shared)
- [ ] Network disconnection handled gracefully

## Multi-Researcher Scenario

- [ ] Create two researcher accounts (via admin panel)
- [ ] Login as Researcher A, annotate Bill X
- [ ] Login as Researcher B, Bill X still appears in queue (independent queues)
- [ ] Researcher B annotates same Bill X
- [ ] Both can see each other's annotations in "Other Annotations" section
- [ ] Each researcher's statistics count only their own annotations
- [ ] Deleting Researcher A's annotation doesn't affect Researcher B's

---

## Sign-Off

| Role | Name | Date | Result |
|------|------|------|--------|
| Tester | | | Pass / Fail |
| Admin | | | Approved |

## Notes

_Record any issues discovered during testing:_

1.
2.
3.
