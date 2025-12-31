"""
Table configuration and display name mappings.

Provides user-friendly names for database tables and conversion utilities.
"""

# User-friendly display names for database tables
TABLE_DISPLAY_NAMES = {
    "KNS_Query": "Parliamentary Queries",
    "KNS_Bill": "Knesset Bills",
    "KNS_Agenda": "Motions for the Agenda",
    "KNS_Person": "Knesset Members",
    "KNS_Faction": "Political Factions",
    "KNS_Committee": "Knesset Committees",
    "KNS_CommitteeSession": "Committee Sessions",
    "KNS_PlenumSession": "Plenary Sessions",
    "KNS_GovMinistry": "Government Ministries",
    "KNS_Status": "Status Codes",
    "KNS_PersonToPosition": "Member Positions",
    "KNS_BillInitiator": "Bill Sponsors",
    "KNS_KnessetDates": "Knesset Dates",
    "KNS_DocumentBill": "Bill Documents",
    "KNS_DocumentAgenda": "Agenda Documents",
    "KNS_BillHistoryInitiator": "Bill History - Initiators",
    "KNS_BillName": "Bill Names",
    "KNS_BillSplit": "Bill Splits",
    "KNS_BillUnion": "Bill Unions",
    "KNS_CmtSiteCode": "Committee Site Codes",
    "KNS_ItemType": "Item Types",
    "KNS_JointCommittee": "Joint Committees",
    "UserFactionCoalitionStatus": "Faction Coalition Status (User Data)",
}

# Reverse mapping: display name -> table name
TABLE_NAME_FROM_DISPLAY = {v: k for k, v in TABLE_DISPLAY_NAMES.items()}


def get_table_display_name(table_name: str) -> str:
    """Get user-friendly display name for a database table.

    Args:
        table_name: Technical table name (e.g., "KNS_Query")

    Returns:
        User-friendly display name (e.g., "Parliamentary Queries")
    """
    return TABLE_DISPLAY_NAMES.get(table_name, table_name)


def get_table_name_from_display(display_name: str) -> str:
    """Get actual table name from user-friendly display name.

    Args:
        display_name: User-friendly name (e.g., "Parliamentary Queries")

    Returns:
        Technical table name (e.g., "KNS_Query")
    """
    return TABLE_NAME_FROM_DISPLAY.get(display_name, display_name)
