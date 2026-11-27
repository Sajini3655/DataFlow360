(schemaName as text, viewName as text) as table =>
let
    Source = PostgreSQL.Database(pServer, pDatabase, [CreateNavigationProperties = false]),
    Result = Source{[Schema = schemaName, Item = viewName]}[Data]
in
    Result
