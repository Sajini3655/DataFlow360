let
    Source = fnGetBiView("bi", "dim_product"),
    Typed = Table.TransformColumnTypes(
        Source,
        {
            {"product_key", Int64.Type},
            {"product_name", type text},
            {"category_key", Int64.Type},
            {"category_name", type text}
        }
    ),
    NonBlankProducts = Table.SelectRows(Typed, each [product_name] <> null and Text.Trim([product_name]) <> "")
in
    NonBlankProducts
