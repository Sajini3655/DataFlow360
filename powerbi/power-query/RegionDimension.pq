let
    Source = fnGetBiView("bi", "dim_region"),
    Typed = Table.TransformColumnTypes(
        Source,
        {{"region_key", Int64.Type}, {"region_name", type text}}
    ),
    UniqueRegions = Table.Distinct(Typed, {"region_key"})
in
    UniqueRegions
