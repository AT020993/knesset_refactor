"""Resume state management for cursor-based table downloads."""

import json
import time
from pathlib import Path
from typing import Dict, Any, Optional
import logging

from config.settings import Settings


class ResumeStateService:
    """Service for managing resume state for cursor-paged tables."""
    
    def __init__(self, resume_file: Optional[Path] = None, logger_obj: Optional[logging.Logger] = None):
        self.resume_file = resume_file or Settings.RESUME_STATE_FILE
        self.logger = logger_obj or logging.getLogger(__name__)
        self._state: Dict[str, Dict[str, Any]] = self._load_state()
    
    def _load_state(self) -> Dict[str, Dict[str, Any]]:
        """Load resume state from JSON file."""
        if not self.resume_file.exists():
            return {}
        
        try:
            data = json.loads(self.resume_file.read_text())
            
            # Migrate old format (just int values) to new format
            if data and isinstance(list(data.values())[0], int):
                self.logger.info("Migrating resume state to new format")
                return {
                    table: {
                        "last_pk": pk,
                        "total_rows": 0,
                        "last_update": time.time()
                    }
                    for table, pk in data.items()
                }
            
            return data
            
        except json.JSONDecodeError:
            self.logger.warning(f"Could not decode resume file {self.resume_file}. Starting fresh.")
            return {}
        except Exception as e:
            self.logger.warning(f"Error loading resume file: {e}. Starting fresh.")
            return {}
    
    def _save_state(self) -> None:
        """Save current state to JSON file."""
        try:
            self.resume_file.parent.mkdir(parents=True, exist_ok=True)
            
            # Add timestamps to all entries
            timestamped_state = {}
            for table, data in self._state.items():
                if isinstance(data, dict):
                    timestamped_state[table] = {**data, "last_update": time.time()}
                else:
                    # Handle legacy format during transition
                    timestamped_state[table] = {
                        "last_pk": data,
                        "total_rows": 0,
                        "last_update": time.time()
                    }
            
            self.resume_file.write_text(json.dumps(timestamped_state, indent=4))
            self.logger.debug(f"Resume state saved for {len(timestamped_state)} tables")
            
        except Exception as e:
            self.logger.warning(f"Could not save resume state: {e}")
    
    def get_table_state(self, table_name: str) -> Dict[str, Any]:
        """Get resume state for a specific table."""
        default_state = {"last_pk": -1, "total_rows": 0}
        return self._state.get(table_name, default_state)
    
    def update_table_state(
        self,
        table_name: str,
        last_pk: int,
        total_rows: int,
        chunk_size: Optional[int] = None
    ) -> None:
        """Update resume state for a table."""
        self._state[table_name] = {
            "last_pk": last_pk,
            "total_rows": total_rows,
            "last_update": time.time()
        }
        
        if chunk_size is not None:
            self._state[table_name]["chunk_size"] = chunk_size
        
        self._save_state()
    
    def clear_table_state(self, table_name: str) -> None:
        """Clear resume state for a table (called when download completes)."""
        if table_name in self._state:
            del self._state[table_name]
            self._save_state()
            self.logger.debug(f"Cleared resume state for {table_name}")
    
    def get_all_states(self) -> Dict[str, Dict[str, Any]]:
        """Get all resume states."""
        return dict(self._state)
    
    def clear_all_states(self) -> None:
        """Clear all resume states."""
        self._state.clear()
        self._save_state()
        self.logger.info("Cleared all resume states")