let
    Source = fnGetBiView("bi", "dim_supplier"),
    Typed = Table.TransformColumnTypes(
        Source,
        {
            {"supplier_key", Int64.Type},
            {"supplier_id", type text},
            {"supplier_name", type text},
            {"region", type text}
        }
    ),
    Trimmed = Table.TransformColumns(
        Typed,
        {{"supplier_name", each if _ = null then null else Text.Trim(_), type text}}
    )
in
    Trimmed
