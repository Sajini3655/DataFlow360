let
    WarehouseLines = SalesTransactions,
    AdditionalPartition = fnReadCsvExport(pCsvSalesFile),
    Combined = Table.Combine({WarehouseLines, AdditionalPartition}),
    TypedKeys = Table.TransformColumnTypes(
        Combined,
        {{"order_id", type text}, {"line_number", Int64.Type}, {"full_date", type date}}
    ),
    UniqueLines = Table.Distinct(TypedKeys, {"order_id", "line_number"})
in
    UniqueLines
