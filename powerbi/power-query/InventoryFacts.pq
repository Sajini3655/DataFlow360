let
    Source = fnGetBiView("bi", "fact_inventory"),
    Typed = Table.TransformColumnTypes(
        Source,
        {
            {"inventory_key", Int64.Type},
            {"product_key", Int64.Type},
            {"supplier_key", Int64.Type},
            {"region_key", Int64.Type},
            {"date_key", Int64.Type},
            {"full_date", type date},
            {"opening_stock", Int64.Type},
            {"quantity_sold", Int64.Type},
            {"closing_stock", Int64.Type}
        }
    ),
    ValidSnapshots = Table.SelectRows(Typed, each [product_key] <> null and [full_date] <> null)
in
    ValidSnapshots
