
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using System.Data;
using Microsoft.VisualBasic.FileIO;
using azap.util;
using System.Globalization;
using Microsoft.Azure.Functions.Worker;
using Azure.Monitor.Query;
using Azure.Identity;
using Azure.Core;
using Azure.Monitor.Query.Models;
using Azure;
using Azure.Security.KeyVault.Secrets;

namespace azap
{
    public  class azap_log_analytics {

      private readonly ILogger<azap_log_analytics>? _logger;

        public azap_log_analytics(ILogger<azap_log_analytics>? logger)
        {
            _logger = logger;
        }

        [Function("log-analytics")]
        public static async Task<IActionResult> Run([HttpTrigger(AuthorizationLevel.Function, "get", "post")] HttpRequest req, FunctionContext executionContext)
        {
            ILogger logger = executionContext.GetLogger("log-analytics");
            //-----------------------  Parameter -----------------------------------
            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            dynamic data = JsonConvert.DeserializeObject(requestBody);             
            string storageAccount =data?.storageAccount; 
            string days_lastmodified =data?.days_lastmodified;
            string environment =data?.environment;
            string source= data?.source;

            //-----------------------  Variables -----------------------------------     
            string keyVaultUrl = $"https://kv-azap-common-{environment}.vault.azure.net/";
            string secretName = "id-log-analytics";
            var keyvaultclient = new SecretClient(new Uri(keyVaultUrl), new DefaultAzureCredential());
            KeyVaultSecret secret = await keyvaultclient.GetSecretAsync(secretName);
            string workspaceId = secret.Value;      

            int days=Int16.Parse(days_lastmodified); 
            var adls_sink = new DatalakeClient( storageAccount, "log-analytics"); 
            var client = new LogsQueryClient(new DefaultAzureCredential());
            

            for (int d = 0; d < days; d++)
            {
            string query = "";           
            string sinkFile = $"{source}/{DateTime.UtcNow.AddDays(-d):yyyy}/{DateTime.UtcNow.AddDays(-d):MM}/{source}_{DateTime.UtcNow.AddDays(-d).ToString("yyyy-MM-dd")}.parquet";
           
            switch (source)
            {   
                case "StorageBlobLogs":
                query = $@"StorageBlobLogs
                | where  bin(TimeGenerated ,1d) == datetime({DateTime.UtcNow.AddDays(-d).ToString("yyyy-MM-dd")})
                | extend CallerIpAddress = iff(UserAgentHeader endswith 'Analytics/Spark/' and CallerIpAddress startswith '10.0', '10.0.xx.xx', iff(UserAgentHeader == 'SQLBLOBACCESS' and CallerIpAddress startswith '10.0.0', '10.0.0.xx', split(CallerIpAddress, ':')[0]))
                | extend ObjectKeySplit = split(ObjectKey, '/')
                | extend Container = tostring(ObjectKeySplit[2])
                | extend Folder = strcat(ObjectKeySplit[3], '/', ObjectKeySplit[4])
                | summarize EventCount = count(), 
                            ReadCount = countif(Category == 'StorageRead'), 
                            WriteCount = countif(Category == 'StorageWrite'), 
                            DeleteCount = countif(Category == 'StorageDelete'), 
                            MaxTimeGenerated = max(TimeGenerated)
                            by Datum=bin(TimeGenerated, 1d), AccountName, CallerIpAddress, AuthenticationType, Container, Folder, UserAgentHeader
                | project Datum, AccountName, CallerIpAddress, AuthenticationType, Container, Folder, UserAgentHeader, EventCount, ReadCount, WriteCount, DeleteCount, MaxTimeGenerated
                | order by EventCount desc";
                    break;
                
                default:
                query =source;
                    break;
            }
            
                        
            Response<LogsQueryResult> response = await client.QueryWorkspaceAsync(
                workspaceId,
                query,
                new QueryTimeRange(TimeSpan.FromDays(days)));

            LogsTable table = response.Value.Table;

            // Create a DataTable from the query result
            DataTable dt = new DataTable();
            foreach (var column in table.Columns)
            {
                dt.Columns.Add(column.Name, typeof(string));
            }
            // Add the export_date column
            dt.Columns.Add("datum", typeof(string));

            foreach (var row in table.Rows)
            {
                DataRow newRow = dt.NewRow();
                for (int i = 0; i < table.Columns.Count; i++)
                {
                    newRow[i] = row[i];
                }
                // Set the export_date value to the current UTC time
                newRow["datum"] = DateTime.UtcNow.ToString("o");
                dt.Rows.Add(newRow);
            }

            // Write the DataTable to Parquet
            BlobClient sinkBlobClient = adls_sink._containerClient.GetBlobClient(sinkFile);
            var parquetClient = new ParquetClient(logger);
            logger.LogInformation(await parquetClient.WriteDataTableToParquet(dt, sinkFile, sinkBlobClient));
        }


          
         // ---------------------- Reading from Source --------------------
         
            return new OkObjectResult(new {Result = "success"});            
        }  //public static async
    }  //public static class azap_extract_column
}
