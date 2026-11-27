(fileName as text) as table =>
let
    FilePath = pExportFolder & "\" & fileName,
    Source = Csv.Document(
        File.Contents(FilePath),
        [Delimiter = ",", Encoding = 65001, QuoteStyle = QuoteStyle.Csv]
    ),
    Headers = Table.PromoteHeaders(Source, [PromoteAllScalars = true])
in
    Headers
