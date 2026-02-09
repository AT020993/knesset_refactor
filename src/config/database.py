"""Database configuration and connection settings."""

from typing import Dict, List, Tuple


class DatabaseConfig:
    """Database-specific configuration."""

    # User-defined tables (created and managed by application, not from OData)
    USER_TABLES = [
        "UserFactionCoalitionStatus",
        "UserTopicTaxonomy",
        "UserAgendaTopics",
        "UserQueryTopics",
        "UserBillTopics",
        "UserBillCoding",
        "UserQueryCoding",
        "UserAgendaCoding",
    ]

    # Table definitions (fetched from Knesset OData API)
    TABLES = [
        "KNS_Person",
        "KNS_Faction",
        "KNS_GovMinistry",
        "KNS_Status",
        "KNS_PersonToPosition",
        "KNS_Query",
        "KNS_Agenda",
        "KNS_Committee",
        "KNS_CmtSiteCode",
        "KNS_CommitteeSession",
        "KNS_PlenumSession",
        "KNS_KnessetDates",
        "KNS_Bill",
        "KNS_BillInitiator",
        "KNS_BillName",
        "KNS_BillHistoryInitiator",
        "KNS_BillUnion",
        "KNS_BillSplit",
        "KNS_DocumentBill",
        "KNS_Law",
        "KNS_IsraelLaw",
        "KNS_CmtSessionItem",
        "KNS_PlmSessionItem",
        "KNS_Vote",
        "KNS_DocumentCommitteeSession",
        "KNS_DocumentPlenumSession",
        "KNS_DocumentAgenda"
    ]
    
    # Tables requiring cursor-based paging: (table_name, (primary_key, chunk_size))
    CURSOR_TABLES: Dict[str, Tuple[str, int]] = {
        "KNS_Person": ("PersonID", 100),
        "KNS_Committee": ("CommitteeID", 100),
        "KNS_CommitteeSession": ("CommitteeSessionID", 100),
        "KNS_PlenumSession": ("PlenumSessionID", 100),
        "KNS_Bill": ("BillID", 100),
        "KNS_Query": ("QueryID", 100),
        "KNS_CmtSessionItem": ("CmtSessionItemID", 100),
        "KNS_PlmSessionItem": ("plmPlenumSessionID", 100),
    }
    
    # Connection settings
    CONNECTION_TIMEOUT = 60
    READ_ONLY_DEFAULT = True
    
    @classmethod
    def get_all_tables(cls) -> List[str]:
        """Get all table names including cursor tables."""
        return list(set(cls.TABLES + list(cls.CURSOR_TABLES.keys())))
    
    @classmethod
    def is_cursor_table(cls, table_name: str) -> bool:
        """Check if a table uses cursor-based paging."""
        return table_name in cls.CURSOR_TABLES
    
    @classmethod
    def get_cursor_config(cls, table_name: str) -> Tuple[str, int]:
        """Get cursor configuration for a table."""
        return cls.CURSOR_TABLES.get(table_name, ("id", 100))