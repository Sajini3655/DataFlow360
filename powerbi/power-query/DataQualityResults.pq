let
    Source = fnGetBiView("bi", "fact_data_quality"),
    Typed = Table.TransformColumnTypes(
        Source,
        {
            {"result_id", Int64.Type},
            {"pipeline_run_id", Int64.Type},
            {"rule_id", type text},
            {"rule_name", type text},
            {"dimension", type text},
            {"severity", type text},
            {"state", type text},
            {"checked_count", Int64.Type},
            {"failure_count", Int64.Type},
            {"failure_rate", type number},
            {"threshold_percent", type number},
            {"checked_at", type datetimezone},
            {"checked_date", type date}
        }
    ),
    FailureFlag = Table.AddColumn(Typed, "is_failure", each [state] = "failed", type logical)
in
    FailureFlag
