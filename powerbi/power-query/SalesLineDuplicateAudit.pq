let
    Source = fnGetBiView("bi", "fact_sales"),
    KeyCounts = Table.Group(
        Source,
        {"order_id", "line_number"},
        {{"row_count", each Table.RowCount(_), Int64.Type}}
    ),
    DuplicateKeys = Table.SelectRows(KeyCounts, each [row_count] > 1)
in
    DuplicateKeys
