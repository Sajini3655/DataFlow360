let
    Source = fnGetBiView("bi", "fact_quality_run"),
    Typed = Table.TransformColumnTypes(
        Source,
        {
            {"pipeline_run_id", Int64.Type},
            {"source_name", type text},
            {"rows_checked", Int64.Type},
            {"rows_accepted", Int64.Type},
            {"rows_rejected", Int64.Type},
            {"acceptance_rate", type number},
            {"rejection_rate", type number},
            {"overall_score", type number},
            {"checked_at", type datetimezone},
            {"checked_date", type date}
        }
    )
in
    Typed
