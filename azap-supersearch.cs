using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Azure.Identity;
using Microsoft.Data.SqlClient;
using Azure.Security.KeyVault.Secrets;
using System.Data;
using azap.util;


namespace azap
{
    public static class azap_supersearch
    {
        [FunctionName("azap-supersearch")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "post", Route = null)] HttpRequest req,
            ILogger mylog)
        {           
            //-----------------------  Parameter -----------------------------------
            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            dynamic data = JsonConvert.DeserializeObject(requestBody);             
            // string database = data?.database;
            // string servername = data?.servername; 
            // string user = data?.user;  
            // string authentication=data?.authentication;
            // string environment=data?.environment;
            string query=data?.query;

            // string connectionString;
            SqlConnection connection=new AzureSqlConnection(mylog, data)._connection;
            // string url_token;
      

            // Create a new SecretClient using the DefaultAzureCredential
            // if (user == "msi") {
            //     var credential = new DefaultAzureCredential(); 
            //     mylog.LogInformation("MSI-Authentification: " + authentication);
            //     if (authentication=="token") {
            //         connectionString=$"Server=tcp:{servername},1433;Initial Catalog={database};";  
            //         if (servername.Contains("azuresynapse")) {
            //             url_token="https://sql.azuresynapse.net/.default";
            //            // mylog.LogInformation("url_token from: " + authentication);
            //         }
            //          else {
            //             url_token="https://database.windows.net/.default";             
            //         }                  
            //         var token = credential.GetToken(new Azure.Core.TokenRequestContext(new[] { url_token }));
            //       //  mylog.LogInformation("token: " + token);
            //         connection = new SqlConnection(connectionString);
            //         connection.AccessToken = token.Token;                     
            //     }
            //         else
            //     { 
            //         connectionString=$"Server=tcp:{servername},1433;Initial Catalog={database};Authentication=Active Directory Managed Identity;";    
            //         connection = new SqlConnection(connectionString);                   
                   
            //     }
                 
            // }
            // else
            // {
            //     var kvUri = $"https://kv-azap-common-{environment}.vault.azure.net";
            //     var secretClient = new SecretClient(new Uri(kvUri), new DefaultAzureCredential());
            //     KeyVaultSecret secret = await secretClient.GetSecretAsync("pw-supersearch");
            //     connectionString = $"Server=tcp:{servername},1433;Initial Catalog={database};User ID={user};Password={secret.Value};";
            //  //   mylog.LogInformation("SQL-User-Authentification: " + authentication);
            //     connection = new SqlConnection(connectionString);    
            // }

            await connection.OpenAsync();       
            using (SqlCommand command = new SqlCommand(query, connection))
            {
                using (SqlDataReader reader = await command.ExecuteReaderAsync())
                {
                    var dataTable = new DataTable();
                    dataTable.Load(reader);
                    string jsonResult = JsonConvert.SerializeObject(dataTable);
                    return new OkObjectResult(jsonResult);
                }
            }
        }
    } 

}