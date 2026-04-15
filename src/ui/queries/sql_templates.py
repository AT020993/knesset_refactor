"""Backward-compat stub. Canonical location: ``data.queries.sql_templates``.

New code should import ``SQLTemplates`` from ``data.queries.sql_templates``
(or ``data.queries``) directly. This stub exists to keep the large set of
existing ``from ui.queries.sql_templates import SQLTemplates`` callers
working during the reorg.
"""

from data.queries.sql_templates import *  # noqa: F401,F403
from data.queries.sql_templates import SQLTemplates  # noqa: F401

__all__ = ["SQLTemplates"]
