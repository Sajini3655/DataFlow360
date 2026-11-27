let
    Source = fnGetBiView("analytics", "monthly_sales"),
    Typed = Table.TransformColumnTypes(
        Source,
        {
            {"sales_month", type date},
            {"region_name", type text},
            {"revenue", Currency.Type},
            {"units_sold", Int64.Type},
            {"running_revenue", Currency.Type},
            {"rolling_three_month_revenue", Currency.Type},
            {"previous_month_revenue", Currency.Type},
            {"revenue_growth_rate", type number}
        }
    )
in
    Typed
