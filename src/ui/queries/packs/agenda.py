"""Agenda query definitions."""

from typing import Any

from ui.queries.sql_templates import SQLTemplates

AGENDA_QUERIES: dict[str, dict[str, Any]] = {
    "Agenda Motions (Full Details)": {
        "sql": f"""
WITH {SQLTemplates.STANDARD_FACTION_LOOKUP},
-- Agenda Documents CTE (aggregates documents per agenda)
{SQLTemplates.AGENDA_DOCUMENTS}
SELECT
    A.AgendaID,
    A.Number,
    A.KnessetNum,
    A.Name AS AgendaName,
    A.SubTypeID AS AgendaSubTypeID,
    A.SubTypeDesc AS AgendaSubTypeDesc,
    A.StatusID AS AgendaStatusID,
    S."Desc" AS AgendaStatusDesc,
    A.ClassificationID AS AgendaClassificationID,
    A.ClassificationDesc AS AgendaClassificationDesc,
    COALESCE(P.FirstName, 'Institutional') AS InitiatorMKFirstName,
    COALESCE(P.LastName, 'Initiative') AS InitiatorMKLastName,
    COALESCE(P.GenderDesc, 'N/A') AS InitiatorMKGender,
    COALESCE(P.IsCurrent, false) AS InitiatorMKIsCurrent,

    -- Simplified faction lookup
    COALESCE(ufs.NewFactionName, f.Name, 'Unknown') AS InitiatorMKFactionName,
    COALESCE(ufs.CoalitionStatus, 'Unknown') AS InitiatorMKFactionCoalitionStatus,

    -- Main Initiator Display (prominent field showing who proposed the agenda)
    CASE
        WHEN A.ClassificationDesc = 'עצמאית' AND P.PersonID IS NOT NULL THEN
            P.FirstName || ' ' || P.LastName || ' (' || COALESCE(ufs.NewFactionName, f.Name, 'Unknown Faction') || ')'
        WHEN A.ClassificationDesc = 'כוללת' THEN
            'Inclusive Proposal (הצעה כוללת - Multiple MKs)'
        ELSE
            COALESCE(P.FirstName || ' ' || P.LastName, 'No Initiator Recorded')
    END AS MainInitiatorDisplay,

    -- Proposal Type in English
    CASE
        WHEN A.ClassificationDesc = 'עצמאית' THEN 'Independent'
        WHEN A.ClassificationDesc = 'כוללת' THEN 'Inclusive'
        ELSE A.ClassificationDesc
    END AS ProposalTypeEN,

    -- Document fields
    COALESCE(ad.DocumentCount, 0) AS AgendaDocumentCount,
    COALESCE(ad.PrimaryDocPDF, ad.PrimaryDocOther) AS AgendaPrimaryDocumentURL,
    ad.PrimaryDocType AS AgendaPrimaryDocumentType,
    ad.PDFDocCount AS AgendaPDFDocCount,
    ad.WordDocCount AS AgendaWordDocCount,
    ad.DocumentTypes AS AgendaDocumentTypes,
    -- Knesset website link for agenda
    'https://main.knesset.gov.il/Activity/Legislation/Laws/Pages/LawBill.aspx?t=lawsuggestionssearch&lawitemid=' || CAST(A.AgendaID AS VARCHAR) AS AgendaKnessetWebsiteURL,

    strftime(CAST(A.LastUpdatedDate AS TIMESTAMP), '%Y-%m-%d') AS LastUpdatedDateFormatted,
    strftime(CAST(A.PresidentDecisionDate AS TIMESTAMP), '%Y-%m-%d') AS PresidentDecisionDateFormatted,

    -- Leading Agenda (parent umbrella for consolidated proposals)
    -- NOTE: LeadingAgendaID only has data for Knessets 12-19. Starting from Knesset 20,
    -- the Knesset stopped using מוכללת (Consolidated) classification and no longer
    -- records individual submissions linked to umbrella proposals.
    A.LeadingAgendaID,
    LA.Name AS LeadingAgendaName,

    -- Research coding columns (policy classification)
    uacoding.MajorIL AS CodingMajorIL,
    uacoding.MinorIL AS CodingMinorIL,
    uacoding.Religion AS CodingReligion,
    uacoding.Territories AS CodingTerritories,
    uacoding.MatchMethod AS CodingMatchMethod

FROM KNS_Agenda A
LEFT JOIN KNS_Person P ON A.InitiatorPersonID = P.PersonID
LEFT JOIN KNS_Status S ON A.StatusID = S.StatusID
LEFT JOIN StandardFactionLookup sfl ON A.InitiatorPersonID = sfl.PersonID
    AND A.KnessetNum = sfl.KnessetNum
    AND sfl.rn = 1
LEFT JOIN KNS_Faction f ON sfl.FactionID = f.FactionID
LEFT JOIN UserFactionCoalitionStatus ufs ON f.FactionID = ufs.FactionID
    AND A.KnessetNum = ufs.KnessetNum
LEFT JOIN AgendaDocuments ad ON A.AgendaID = ad.AgendaID
LEFT JOIN KNS_Agenda LA ON A.LeadingAgendaID = LA.AgendaID
LEFT JOIN UserAgendaCoding uacoding ON A.AgendaID = uacoding.AgendaID

ORDER BY A.KnessetNum DESC, A.AgendaID DESC
LIMIT 1000;
        """,
        "knesset_filter_column": "A.KnessetNum",
        "faction_filter_column": "f.FactionID",
        "description": (
            "Comprehensive agenda items data with faction details, "
            "status information, and document links"
        ),
    },
}
