"""Minimal resume state service placeholder."""

from pathlib import Path
from typing import Any, Dict

class ResumeStateService:
    """Stub service for managing resume state during downloads."""

    def __init__(self, state_path: Path | None = None) -> None:
        self.state_path = state_path or Path(".resume_state.json")

    def load_state(self) -> Dict[str, Any]:
        """Load resume state from disk if available."""
        if self.state_path.exists():
            import json
            with self.state_path.open("r", encoding="utf-8") as f:
                return json.load(f)
        return {}

    def save_state(self, state: Dict[str, Any]) -> None:
        """Persist resume state to disk."""
        import json
        self.state_path.write_text(json.dumps(state), encoding="utf-8")
