let
    Source = fnGetBiView("bi", "fact_pipeline_execution"),
    Typed = Table.TransformColumnTypes(
        Source,
        {
            {"pipeline_run_id", Int64.Type},
            {"source_name", type text},
            {"state", type text},
            {"engine", type text},
            {"started_at", type datetimezone},
            {"started_date", type date},
            {"finished_at", type datetimezone},
            {"duration_seconds", type number},
            {"rows_read", Int64.Type},
            {"accepted_rows", Int64.Type},
            {"rejected_rows", Int64.Type}
        }
    ),
    ValidRuns = Table.SelectRows(Typed, each [pipeline_run_id] <> null)
in
    ValidRuns
