from data.queries.packs.bills import BILLS_QUERIES


def test_bills_full_details_group_concats_are_ordered() -> None:
    sql = BILLS_QUERIES["Bills & Legislation (Full Details)"]["sql"]
    group_concat_count = sql.upper().count("GROUP_CONCAT(")
    ordered_group_concat_count = sql.upper().count("ORDER BY CASE") + sql.upper().count(
        "ORDER BY CAST"
    ) + sql.upper().count("ORDER BY DB.GROUPTYPEDESC")

    assert group_concat_count == 6
    assert ordered_group_concat_count == group_concat_count
