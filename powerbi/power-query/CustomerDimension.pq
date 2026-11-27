let
    Source = fnGetBiView("bi", "dim_customer"),
    Typed = Table.TransformColumnTypes(
        Source,
        {{"customer_key", Int64.Type}, {"customer_id", type text}, {"customer_name", type text}, {"region", type text}}
    ),
    Trimmed = Table.TransformColumns(
        Typed,
        {{"customer_name", each if _ = null then null else Text.Trim(_), type text}}
    ),
    DisplayRegion = Table.ReplaceValue(Trimmed, null, "Unknown", Replacer.ReplaceValue, {"region"})
in
    DisplayRegion
