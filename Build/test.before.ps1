Write-Host "Executing test.before.ps1"
if($null -eq (Get-OdbcDriver -Name "ODBC Driver 17 for SQL Server" -ErrorAction SilentlyContinue))
{
Write-Host "Installing ODBC Driver 17 for SQL Server"
$url = "https://go.microsoft.com/fwlink/?linkid=2168524"
$outpath = "D:\a\_temp\odbc.msi"
Invoke-WebRequest -Uri $url -OutFile $outpath
Start-Process -Filepath $outpath -ArgumentList "/qr IACCEPTMSODBCSQLLICENSETERMS=YES"
Write-Host "ODBC Driver 17 for SQL Server successfully installed"
}
else {
Write-Host "ODBC Driver 17 for SQL Server already installed"
}
$env:AzureWebJobsDataLake = "nonprd"
$env:Subscription = "nonprd"
$env:Environment = "dev"

Write-Host “Creating variable AZURE_CLIENT_ID based on AzureIntegrationTestUserName”
$env:AZURE_CLIENT_ID = $env:AzureIntegrationTestUserName

Write-Host “Creating variable AZURE_CLIENT_SECRET based on AzureIntegrationTestPassword”
$env:AZURE_CLIENT_SECRET = $env:AzureIntegrationTestPassword

Write-Host “Creating variable AZURE_TENANT_ID based on AzureAuthTenant”
$env:AZURE_TENANT_ID = $env:AzureAuthTenant

Write-Host "Finished test.before.ps1"