"""Plenum-vote ingestion from the live Knesset WebSiteApi.

The official OData ``Votes.svc`` is frozen at 2021 and has no current-Knesset
data, so current per-MK votes are sourced from the live site API instead. See
``web_votes_client`` for the endpoints and ``ingest`` for the orchestration.
"""
