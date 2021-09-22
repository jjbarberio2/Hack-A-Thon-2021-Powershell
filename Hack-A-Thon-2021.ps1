# ==================================================================================================================
# Created By:  Jeff Barberio
# Created On:  20210921
# Updates:     
# Description: pull all file paths in the repo from github along with the hash of their contents to compare against
#              what is stored in the database. new/updated scripts will be run against all databases           
# ==================================================================================================================

# ==================================================================================================================
# DECLARE FUNCTION
# ==================================================================================================================

Function Get-GitHubContent {
[cmdletbinding()]
    param (
    [string]$paramDirectoryPath
    ,[string]$gitHubCreds
    )

    Process {

        # write-host("Get-GitHubContent Called with paramDirectoryPath = {0}" -f $paramDirectoryPath)
        $directoryMetadata = invoke-webrequest -Uri $paramDirectoryPath -Headers @{"Authorization"="Basic $gitHubCreds"}
        $contents = $directoryMetadata.Content | ConvertFrom-Json
        $files = $contents | Where-Object {$_.type -eq "file"}

        foreach ($file in $files) {
    
            #Create a row
            $row = $ScriptHashes.NewRow()

            #Enter data in the row
            $row.filePath = $file.path
            $row.SHA = $file.sha

            #Add the row to the table
            $ScriptHashes.Rows.Add($row)
        }

        $directories = $contents | Where-Object {$_.type -eq "dir"}
        foreach($directory in $directories){
            $newDirectoryPath = $paramDirectoryPath + "/" + $directory.path
            Get-GitHubContent -paramDirectoryPath $newDirectoryPath
        }

    }

}

# ==================================================================================================================
# Gather paths and Hashes
# ==================================================================================================================

$batchId = New-Guid

$AuthBytes  = [System.Text.Encoding]::Ascii.GetBytes("jjbarberio2:5513Int3l")
$BasicCreds = [Convert]::ToBase64String($AuthBytes)

$contentDirectoryPath = "https://api.github.com/repos/jjbarberio2/Hack-a-thon-2021/contents"

$ScriptHashes = New-Object system.Data.DataTable

#Define Columns
$col1 = New-Object system.Data.DataColumn filePath,([string])
$col2 = New-Object system.Data.DataColumn SHA,([string])

#Add the Columns
$ScriptHashes.columns.add($col1)
$ScriptHashes.columns.add($col2)

Get-GitHubContent -paramDirectoryPath $contentDirectoryPath -gitHubCreds $BasicCreds

# write-output("script hashes to send`r`n`r`n")
# $ScriptHashes

# ==================================================================================================================
# call stored proc to find new/updated scripts
# ==================================================================================================================

$scriptPathBase = "https://raw.githubusercontent.com/jjbarberio2/Hack-a-thon-2021/main/"

$connectionString = 'server=ssb-dev-databases.database.windows.net;user id=svcETL;password= ql^$RwSPyCwAK6s;initial catalog=SSBRPDevelopment'
			
$connection = New-Object System.Data.SqlClient.SqlConnection($connectionString)
$connection.Open()

$command = New-Object System.Data.SqlClient.SqlCommand
$command.CommandType = [System.Data.CommandType]::StoredProcedure
$command.CommandText = "dbo.sp_HackAThon2021_jbarberio_UpdateHashes"
$command.Connection = $connection

$ScriptHashValuesParam = New-Object('system.data.sqlclient.sqlparameter')
$ScriptHashValuesParam.ParameterName = "ScriptHashValues"
$ScriptHashValuesParam.SqlDBtype = [System.Data.SqlDbType]::Structured
$ScriptHashValuesParam.Direction = [System.Data.ParameterDirection]::Input
$ScriptHashValuesParam.value = $ScriptHashes

$command.parameters.add($ScriptHashValuesParam) | Out-Null

#SQL Adapter - get the results using the SQL Command
$sqlAdapter = new-object System.Data.SqlClient.SqlDataAdapter 
$sqlAdapter.SelectCommand = $command
$dataSet = new-object System.Data.Dataset
$recordCount = $sqlAdapter.Fill($dataSet) 

$data = $dataSet.Tables[0]

$scriptsToRun = @()

foreach($row in $data)
{ 
    $scriptsToRun += $row.ScriptPath
}

# $scriptsToRun

# ==================================================================================================================
# Gather Db's
# ==================================================================================================================

$configConnString = 'Data Source=ssbmetadata.database.windows.net;Initial Catalog=SSBRPProduction;User ID=svcETL;Password=ql^$RwSPyCwAK6s;'
$databases = @()
$queryGetDBs = "

SELECT DISTINCT TOP 3 TenantDataSource.TenantDataSourceID
	  ,server.FQDN	ServerName
	  ,TenantDataSource.DBName
	  ,TenantDataSource.Username
	  ,TenantDataSource.EncryptedPassword
	  ,TenantDataSource.IsActive
FROM dbo.TenantDataSource TenantDataSource
	JOIN dbo.DBType DbType ON DbType.DBTypeID = TenantDataSource.DBTypeID
	JOIN dbo.Server server ON server.ServerID = TenantDataSource.ServerID
WHERE server.FQDN = 'VM-DB-DEV-01.ssbinfo.com' 
	  AND DBType = 'SSB CI DW - Core'
	  AND TenantDataSource.IsActive = 1
ORDER BY serverName, DBName

"

$databases += Invoke-Sqlcmd -ConnectionString $configConnString -Query $queryGetDBs -OutputAs DataRows

# foreach($database in $databases){
#     write-output("`r`n{0} - {1}" -f $database.ServerName, $database.DBName)
# }

# ==================================================================================================================
# run code
# ==================================================================================================================

$auditData = New-Object system.Data.DataTable "auditRecord"

#Define Columns
$col1 = New-Object system.Data.DataColumn BatchId,([string])
$col2 = New-Object system.Data.DataColumn TenantDataSourceID,([string])
$col3 = New-Object system.Data.DataColumn ServerName,([string])
$col4 = New-Object system.Data.DataColumn Database,([string])
$col5 = New-Object system.Data.DataColumn ScriptPath,([string])
$col6 = New-Object system.Data.DataColumn Deployed,([boolean])


#Add the Columns
$auditData.columns.add($col1)
$auditData.columns.add($col2)
$auditData.columns.add($col3)
$auditData.columns.add($col4)
$auditData.columns.add($col5)
$auditData.columns.add($col6)


foreach($scriptToRun in $scriptsToRun){

    write-output("deploying script {0}`r`n`r`n" -f $scriptToRun)
    $pathToCode = $scriptPathBase + $scriptToRun
    $codeToRun = invoke-webrequest -Uri $pathToCode -Headers @{"Authorization"="Basic $gitHubCreds"}   

    foreach($database in $databases){

        [boolean]$deployed = $false

        if (![string]::IsNullOrEmpty($EncryptedPassword)){
            $conn = 'Data Source={0};Initial Catalog={1};User ID={2};Password={3};' -f $database.ServerName, $database.DBName, $database.Username, $database.EncryptedPassword
        }
        else {
            $conn = 'Data Source={0};Initial Catalog={1};Integrated Security=true;' -f $database.ServerName, $database.DBName
        }

        #  $conn = 'Data Source={0};Initial Catalog={1};User ID=svcETL;Password=ql^$RwSPyCwAK6s;' -f $database.ServerName, $database.DBName

        try {
            write-output("`tdeploying to {0}/{1}`r`n" -f $database.ServerName, $database.DBName)
            Invoke-Sqlcmd -ConnectionString $conn -Query $codeToRun
            $deployed = $true
        }
        catch{
            $deployed = $false
        }

        #Create a row
        $row = $auditData.NewRow()

        #Enter data in the row
        $row.BatchId = $batchId
        $row.TenantDataSourceID = $database.TenantDataSourceID
        $row.ServerName = $database.ServerName
        $row.Database = $database.DBName
        $row.ScriptPath = $scriptToRun
        $row.Deployed = $deployed

        #Add the row to the table
        $auditData.Rows.Add($row)
        
    }

}

# $auditData

[string]$userName = 'svcETL'
[string]$userPassword = 'ql^$RwSPyCwAK6s'

[securestring]$secStringPassword = ConvertTo-SecureString $userPassword -AsPlainText -Force

[pscredential]$credObject = New-Object System.Management.Automation.PSCredential ($userName, $secStringPassword)

Write-SqlTableData -Credential $credObject -ServerInstance "SSBMetadata.database.windows.net" -DatabaseName "SSBRPProduction" -SchemaName "audit" -TableName "HackAThon2021_jbarberio_DeploymentLog" -InputData $auditData -Timeout 300

# write-output("testing 1 2")
