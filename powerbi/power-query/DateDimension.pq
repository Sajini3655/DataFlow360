let
    Source = fnGetBiView("bi", "dim_date"),
    Typed = Table.TransformColumnTypes(
        Source,
        {
            {"date_key", Int64.Type},
            {"full_date", type date},
            {"year", Int64.Type},
            {"quarter", Int64.Type},
            {"month", Int64.Type},
            {"month_name", type text},
            {"month_start", type date}
        }
    )
in
    Typed
