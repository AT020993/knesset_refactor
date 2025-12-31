"""Table definitions and metadata for the Knesset database."""

from typing import Dict, List, Tuple, Optional
from dataclasses import dataclass


@dataclass
class TableMetadata:
    """Metadata for a database table."""
    name: str
    description: str
    primary_key: str
    is_cursor_paged: bool = False
    chunk_size: int = 100
    dependencies: List[str] = None


class KnessetTables:
    """Centralized table definitions and metadata."""
    
    # Core person and faction tables
    PERSON = TableMetadata(
        name="KNS_Person",
        description="Members of Knesset personal information",
        primary_key="PersonID",
        is_cursor_paged=True,
        chunk_size=100
    )
    
    FACTION = TableMetadata(
        name="KNS_Faction",
        description="Political factions in the Knesset",
        primary_key="FactionID"
    )
    
    # Position and ministry tables
    PERSON_TO_POSITION = TableMetadata(
        name="KNS_PersonToPosition",
        description="Assignment of persons to positions",
        primary_key="PersonToPositionID",
        dependencies=["KNS_Person", "KNS_Faction"]
    )
    
    GOV_MINISTRY = TableMetadata(
        name="KNS_GovMinistry",
        description="Government ministries",
        primary_key="GovMinistryID"
    )
    
    STATUS = TableMetadata(
        name="KNS_Status",
        description="Status codes and descriptions",
        primary_key="StatusID"
    )
    
    # Parliamentary activity tables
    QUERY = TableMetadata(
        name="KNS_Query",
        description="Parliamentary queries",
        primary_key="QueryID",
        is_cursor_paged=True,
        chunk_size=100,
        dependencies=["KNS_Person", "KNS_GovMinistry", "KNS_Status"]
    )
    
    AGENDA = TableMetadata(
        name="KNS_Agenda",
        description="Parliamentary agenda items",
        primary_key="AgendaID",
        dependencies=["KNS_Person", "KNS_Committee", "KNS_Status"]
    )
    
    # Committee and session tables
    COMMITTEE = TableMetadata(
        name="KNS_Committee",
        description="Knesset committees",
        primary_key="CommitteeID"
    )
    
    COMMITTEE_SESSION = TableMetadata(
        name="KNS_CommitteeSession",
        description="Committee session records",
        primary_key="CommitteeSessionID",
        is_cursor_paged=True,
        chunk_size=100,
        dependencies=["KNS_Committee"]
    )
    
    PLENUM_SESSION = TableMetadata(
        name="KNS_PlenumSession",
        description="Plenum session records",
        primary_key="PlenumSessionID",
        is_cursor_paged=True,
        chunk_size=100
    )
    
    # Administrative tables
    KNESSET_DATES = TableMetadata(
        name="KNS_KnessetDates",
        description="Knesset term dates and information",
        primary_key="KnessetNum"
    )
    
    # Legislation tables
    BILL = TableMetadata(
        name="KNS_Bill",
        description="Legislative bills",
        primary_key="BillID",
        is_cursor_paged=True,
        chunk_size=100,
        dependencies=["KNS_Person", "KNS_Committee"]
    )
    
    BILL_INITIATOR = TableMetadata(
        name="KNS_BillInitiator",
        description="Bill initiators",
        primary_key="BillInitiatorID",
        dependencies=["KNS_Bill", "KNS_Person"]
    )
    
    LAW = TableMetadata(
        name="KNS_Law",
        description="Enacted laws",
        primary_key="LawID",
        dependencies=["KNS_Bill"]
    )
    
    ISRAEL_LAW = TableMetadata(
        name="KNS_IsraelLaw",
        description="General Israeli legislation",
        primary_key="LawID"
    )

    # Document tables
    DOCUMENT_AGENDA = TableMetadata(
        name="KNS_DocumentAgenda",
        description="Documents attached to agenda items",
        primary_key="DocumentAgendaID",
        dependencies=["KNS_Agenda"]
    )

    # User-defined tables
    USER_FACTION_COALITION_STATUS = TableMetadata(
        name="UserFactionCoalitionStatus",
        description="User-maintained faction coalition status",
        primary_key="FactionID"
    )

    # Topic classification tables (user-managed, for external topic data)
    USER_TOPIC_TAXONOMY = TableMetadata(
        name="UserTopicTaxonomy",
        description="Topic/subject taxonomy for parliamentary items",
        primary_key="TopicID"
    )

    USER_AGENDA_TOPICS = TableMetadata(
        name="UserAgendaTopics",
        description="Topic mappings for agenda items",
        primary_key="AgendaID",
        dependencies=["KNS_Agenda", "UserTopicTaxonomy"]
    )

    USER_QUERY_TOPICS = TableMetadata(
        name="UserQueryTopics",
        description="Topic mappings for parliamentary queries",
        primary_key="QueryID",
        dependencies=["KNS_Query", "UserTopicTaxonomy"]
    )

    USER_BILL_TOPICS = TableMetadata(
        name="UserBillTopics",
        description="Topic mappings for bills",
        primary_key="BillID",
        dependencies=["KNS_Bill", "UserTopicTaxonomy"]
    )

    # CAP (Democratic Erosion) annotation tables
    USER_CAP_TAXONOMY = TableMetadata(
        name="UserCAPTaxonomy",
        description="Democratic Erosion codebook taxonomy for bill classification",
        primary_key="MinorCode"
    )

    USER_BILL_CAP = TableMetadata(
        name="UserBillCAP",
        description="Bill annotations using Democratic Erosion codebook",
        primary_key="BillID",
        dependencies=["KNS_Bill", "UserCAPTaxonomy"]
    )

    @classmethod
    def get_all_tables(cls) -> List[TableMetadata]:
        """Get all table metadata."""
        return [
            cls.PERSON, cls.FACTION, cls.PERSON_TO_POSITION, cls.GOV_MINISTRY,
            cls.STATUS, cls.QUERY, cls.AGENDA, cls.COMMITTEE, cls.COMMITTEE_SESSION,
            cls.PLENUM_SESSION, cls.KNESSET_DATES, cls.BILL, cls.BILL_INITIATOR, cls.LAW, cls.ISRAEL_LAW,
            cls.DOCUMENT_AGENDA
        ]
    
    @classmethod
    def get_table_by_name(cls, name: str) -> Optional[TableMetadata]:
        """Get table metadata by name."""
        for table in cls.get_all_tables():
            if table.name == name:
                return table
        return None
    
    @classmethod
    def get_cursor_tables(cls) -> List[TableMetadata]:
        """Get tables that require cursor-based paging."""
        return [table for table in cls.get_all_tables() if table.is_cursor_paged]
    
    @classmethod
    def get_table_names(cls) -> List[str]:
        """Get all table names."""
        return [table.name for table in cls.get_all_tables()]
    
    @classmethod
    def get_dependencies(cls, table_name: str) -> List[str]:
        """Get dependencies for a table."""
        table = cls.get_table_by_name(table_name)
        return table.dependencies or [] if table else []
    
    @classmethod
    def get_load_order(cls) -> List[str]:
        """Get tables in dependency order for loading."""
        # Simple topological sort for table dependencies
        loaded = set()
        result = []
        
        def can_load(table: TableMetadata) -> bool:
            return not table.dependencies or all(dep in loaded for dep in table.dependencies)
        
        tables = cls.get_all_tables()
        while len(result) < len(tables):
            for table in tables:
                if table.name not in loaded and can_load(table):
                    result.append(table.name)
                    loaded.add(table.name)
                    break
            else:
                # Handle circular dependencies by loading remaining tables
                remaining = [t for t in tables if t.name not in loaded]
                if remaining:
                    result.append(remaining[0].name)
                    loaded.add(remaining[0].name)
        
        return result