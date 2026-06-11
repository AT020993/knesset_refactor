"""MK biographical + committee-membership ingestion from the Knesset WebSiteApi.

The ParliamentInfo OData feed exposes neither MK CV fields (birth, education,
military service, languages) nor committee membership. Both are served by the
undocumented site backend that powers the mklobby member pages
(``WebSiteApi/knessetapi/MKs``), keyed by a per-MK ``SiteId`` (mapped from
``PersonID`` via OData ``KNS_MkSiteCode``).

This package fetches them and writes two warehouse tables — ``WebMkCv`` and
``WebMkCommittee`` — which the snapshot exporter shapes into ``mk_cv.parquet``
and ``committee_members_by_faction.parquet``.
"""
