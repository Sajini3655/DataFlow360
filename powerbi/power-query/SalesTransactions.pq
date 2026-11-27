let
    Source = fnGetBiView("bi", "fact_sales"),
    Typed = Table.TransformColumnTypes(
        Source,
        {
            {"order_id", type text},
            {"line_number", Int64.Type},
            {"customer_key", Int64.Type},
            {"product_key", Int64.Type},
            {"date_key", Int64.Type},
            {"region_key", Int64.Type},
            {"supplier_key", Int64.Type},
            {"full_date", type date},
            {"quantity", Int64.Type},
            {"unit_price", Currency.Type},
            {"revenue", Currency.Type}
        }
    ),
    InRange = Table.SelectRows(
        Typed,
        each [full_date] >= Date.From(RangeStart) and [full_date] < Date.From(RangeEnd)
    ),
    ValidKeys = Table.SelectRows(
        InRange,
        each [order_id] <> null and [line_number] <> null and [full_date] <> null
    ),
    UniqueOrderLines = Table.Distinct(ValidKeys, {"order_id", "line_number"})
in
    UniqueOrderLines
