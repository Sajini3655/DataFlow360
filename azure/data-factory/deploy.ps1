param(
    [Parameter(Mandatory = $true)][string]$SubscriptionId,
    [Parameter(Mandatory = $true)][string]$ResourceGroup,
    [Parameter(Mandatory = $true)][string]$FactoryName
)

$ErrorActionPreference = 'Stop'
$apiVersion = '2018-06-01'
$baseUri = "https://management.azure.com/subscriptions/$SubscriptionId/resourceGroups/$ResourceGroup/providers/Microsoft.DataFactory/factories/$FactoryName"

az account show --subscription $SubscriptionId --output none
if ($LASTEXITCODE -ne 0) {
    throw 'Azure CLI is not authenticated to the selected subscription.'
}

$resourceFolders = @(
    @{ Path = 'linkedServices'; Segment = 'linkedservices' },
    @{ Path = 'datasets'; Segment = 'datasets' },
    @{ Path = 'pipelines'; Segment = 'pipelines' },
    @{ Path = 'triggers'; Segment = 'triggers' }
)

foreach ($resourceFolder in $resourceFolders) {
    $folder = Join-Path $PSScriptRoot $resourceFolder.Path
    Get-ChildItem -LiteralPath $folder -Filter '*.json' -File | Sort-Object Name | ForEach-Object {
        $resource = Get-Content -LiteralPath $_.FullName -Raw | ConvertFrom-Json
        $uri = "$baseUri/$($resourceFolder.Segment)/$($resource.name)?api-version=$apiVersion"
        az rest --method put --uri $uri --body "@$($_.FullName)" --output none
        if ($LASTEXITCODE -ne 0) {
            throw "Failed to deploy Data Factory resource $($resource.name)."
        }
        Write-Host "Deployed $($resourceFolder.Segment)/$($resource.name)"
    }
}

Write-Host 'Templates are deployed. Review the stopped trigger and start it only after validating all linked services and parameters.'
