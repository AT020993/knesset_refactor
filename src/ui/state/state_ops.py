"""Shared session state initialization/reset helpers."""

from __future__ import annotations

from typing import Any

import streamlit as st


def initialize_state_keys(key_defaults: dict[str, Any]) -> None:
    """Initialize missing session-state keys with configured defaults."""
    for key, default_value in key_defaults.items():
        if key not in st.session_state:
            st.session_state[key] = default_value() if callable(default_value) else default_value


def reset_state_group(key_defaults: dict[str, Any]) -> None:
    """Reset session-state group to configured defaults."""
    for key, default_value in key_defaults.items():
        st.session_state[key] = default_value() if callable(default_value) else default_value

