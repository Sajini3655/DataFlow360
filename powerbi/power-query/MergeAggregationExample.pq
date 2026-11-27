let
    SalesByCustomer = Table.Group(
        SalesTransactions,
        {"customer_key"},
        {
            {"revenue", each List.Sum([revenue]), Currency.Type},
            {"order_lines", each Table.RowCount(_), Int64.Type}
        }
    ),
    MergedCustomer = Table.NestedJoin(
        SalesByCustomer,
        {"customer_key"},
        CustomerDimension,
        {"customer_key"},
        "Customer",
        JoinKind.LeftOuter
    ),
    Expanded = Table.ExpandTableColumn(
        MergedCustomer,
        "Customer",
        {"customer_id", "customer_name", "region"},
        {"customer_id", "customer_name", "region"}
    )
in
    Expanded
