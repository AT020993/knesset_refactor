-- Faction Attribution Validation Query
-- Compares old (KnessetNum-only) vs new (date-based) faction attribution logic
-- Purpose: Measure impact of faction attribution fixes applied 2025-10-05

-- Run this query in DuckDB to see how many bills changed faction attribution
-- after adding date-based JOIN logic to plot_top_bill_initiators and plot_bill_initiators_by_faction

WITH BillFirstSubmission AS (
    -- Get the earliest activity date for each bill (true submission date)
    SELECT
        B.BillID,
        MIN(earliest_date) as FirstSubmissionDate
    FROM KNS_Bill B
    LEFT JOIN (
        -- Initiator assignment dates
        SELECT BI.BillID, MIN(CAST(BI.LastUpdatedDate AS TIMESTAMP)) as earliest_date
        FROM KNS_BillInitiator BI WHERE BI.LastUpdatedDate IS NOT NULL GROUP BY BI.BillID
        UNION ALL
        -- Committee session dates
        SELECT csi.ItemID as BillID, MIN(CAST(cs.StartDate AS TIMESTAMP)) as earliest_date
        FROM KNS_CmtSessionItem csi JOIN KNS_CommitteeSession cs ON csi.CommitteeSessionID = cs.CommitteeSessionID
        WHERE csi.ItemID IS NOT NULL AND cs.StartDate IS NOT NULL GROUP BY csi.ItemID
        UNION ALL
        -- Plenum session dates
        SELECT psi.ItemID as BillID, MIN(CAST(ps.StartDate AS TIMESTAMP)) as earliest_date
        FROM KNS_PlmSessionItem psi JOIN KNS_PlenumSession ps ON psi.PlenumSessionID = ps.PlenumSessionID
        WHERE psi.ItemID IS NOT NULL AND ps.StartDate IS NOT NULL GROUP BY psi.ItemID
        UNION ALL
        -- Publication dates
        SELECT B.BillID, CAST(B.PublicationDate AS TIMESTAMP) as earliest_date
        FROM KNS_Bill B WHERE B.PublicationDate IS NOT NULL
    ) all_dates ON B.BillID = all_dates.BillID
    WHERE all_dates.earliest_date IS NOT NULL
    GROUP BY B.BillID
),
OldLogic AS (
    -- OLD: Faction attribution based on KnessetNum only (no date checking)
    SELECT
        b.BillID,
        bi.PersonID,
        p.FirstName || ' ' || p.LastName as MKName,
        b.KnessetNum,
        COALESCE(f.Name, 'Unknown Faction') as FactionName
    FROM KNS_Bill b
    JOIN KNS_BillInitiator bi ON b.BillID = bi.BillID
    JOIN KNS_Person p ON bi.PersonID = p.PersonID
    LEFT JOIN KNS_PersonToPosition ptp ON bi.PersonID = ptp.PersonID
        AND b.KnessetNum = ptp.KnessetNum  -- ❌ Only Knesset match, no date check
        AND ptp.FactionID IS NOT NULL
    LEFT JOIN KNS_Faction f ON ptp.FactionID = f.FactionID
    WHERE bi.Ordinal = 1  -- Main initiators only
        AND bi.PersonID IS NOT NULL
        AND b.KnessetNum = 25  -- Change this to test other Knessets
),
NewLogic AS (
    -- NEW: Faction attribution with date-based matching (CORRECT)
    SELECT
        b.BillID,
        bi.PersonID,
        p.FirstName || ' ' || p.LastName as MKName,
        b.KnessetNum,
        COALESCE(f.Name, 'Unknown Faction') as FactionName
    FROM KNS_Bill b
    LEFT JOIN BillFirstSubmission bfs ON b.BillID = bfs.BillID
    JOIN KNS_BillInitiator bi ON b.BillID = bi.BillID
    JOIN KNS_Person p ON bi.PersonID = p.PersonID
    LEFT JOIN KNS_PersonToPosition ptp ON bi.PersonID = ptp.PersonID
        AND b.KnessetNum = ptp.KnessetNum
        AND COALESCE(bfs.FirstSubmissionDate, CAST(b.LastUpdatedDate AS TIMESTAMP))
            BETWEEN CAST(ptp.StartDate AS TIMESTAMP)
            AND CAST(COALESCE(ptp.FinishDate, '9999-12-31') AS TIMESTAMP)  -- ✅ Date-based match
        AND ptp.FactionID IS NOT NULL
    LEFT JOIN KNS_Faction f ON ptp.FactionID = f.FactionID
    WHERE bi.Ordinal = 1
        AND bi.PersonID IS NOT NULL
        AND b.KnessetNum = 25  -- Change this to test other Knessets
),
Comparison AS (
    SELECT
        o.BillID,
        o.PersonID,
        o.MKName,
        o.KnessetNum,
        o.FactionName as OldFaction,
        n.FactionName as NewFaction,
        CASE
            WHEN o.FactionName != n.FactionName THEN 1
            ELSE 0
        END as AttributionChanged
    FROM OldLogic o
    JOIN NewLogic n ON o.BillID = n.BillID AND o.PersonID = n.PersonID
)

-- Summary Statistics
SELECT
    '=== FACTION ATTRIBUTION FIX IMPACT (Knesset 25) ===' as Metric,
    NULL as Value,
    NULL as Percentage
UNION ALL
SELECT
    'Total Bills Analyzed',
    CAST(COUNT(*) as VARCHAR),
    '100%'
FROM Comparison
UNION ALL
SELECT
    'Bills with Changed Attribution',
    CAST(SUM(AttributionChanged) as VARCHAR),
    CAST(ROUND(100.0 * SUM(AttributionChanged) / COUNT(*), 2) as VARCHAR) || '%'
FROM Comparison
UNION ALL
SELECT
    'Bills with Correct Attribution',
    CAST(SUM(CASE WHEN AttributionChanged = 0 THEN 1 ELSE 0 END) as VARCHAR),
    CAST(ROUND(100.0 * SUM(CASE WHEN AttributionChanged = 0 THEN 1 ELSE 0 END) / COUNT(*), 2) as VARCHAR) || '%'
FROM Comparison
UNION ALL
SELECT
    '',
    NULL,
    NULL
UNION ALL
SELECT
    '=== TOP 10 CHANGED ATTRIBUTIONS ===' as Metric,
    NULL as Metric,
    NULL as Percentage
UNION ALL
SELECT
    'MK: ' || MKName,
    'Old: ' || OldFaction || ' → New: ' || NewFaction,
    NULL
FROM Comparison
WHERE AttributionChanged = 1
ORDER BY BillID
LIMIT 10;

-- Detailed breakdown by faction changes
SELECT
    '=== FACTION CHANGE BREAKDOWN ===' as Summary,
    NULL as OldFaction,
    NULL as NewFaction,
    NULL as BillCount
UNION ALL
SELECT
    'Faction transitions:',
    OldFaction,
    NewFaction,
    CAST(COUNT(*) as VARCHAR) as BillCount
FROM Comparison
WHERE AttributionChanged = 1
GROUP BY OldFaction, NewFaction
ORDER BY COUNT(*) DESC;
