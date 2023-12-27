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
            string database = data?.database;
            string servername = data?.servername; 
            string user = data?.user;  
            string authentication=data?.authentication;
            string environment=data?.environment;

            string connectionString;
            SqlConnection connection;
            string url_token;
      

            // Create a new SecretClient using the DefaultAzureCredential
            if (user == "msi") {
                var credential = new DefaultAzureCredential(); 
                mylog.LogInformation("MSI-Authentification: " + authentication);
                if (authentication=="token") {
                    connectionString=$"Server=tcp:{servername},1433;Initial Catalog={database};";  
                    if (servername.Contains("azuresynapse")) {
                        url_token="https://sql.azuresynapse.net/.default";
                    }
                     else {
                        url_token="https://database.windows.net/.default";             
                    }                  
                    var token = credential.GetToken(new Azure.Core.TokenRequestContext(new[] { url_token }));
                    connection = new SqlConnection(connectionString);
                    connection.AccessToken = token.Token;                     
                }
                    else
                { 
                    connectionString=$"Server=tcp:{servername},1433;Initial Catalog={database};Authentication=Active Directory Managed Identity;";    
                    connection = new SqlConnection(connectionString);                   
                   
                }
                 
            }
            else
            {
                var kvUri = $"https://kv-azap-common-{environment}.vault.azure.net";
                var secretClient = new SecretClient(new Uri(kvUri), new DefaultAzureCredential());
                KeyVaultSecret secret = await secretClient.GetSecretAsync("pw-supersearch");
                connectionString = $"Server=tcp:{servername},1433;Initial Catalog={database};User ID={user};Password={secret.Value};";
                mylog.LogInformation("SQL-User-Authentification: " + authentication);
                connection = new SqlConnection(connectionString);    
            }

            int rowCount=0;        

            await connection.OpenAsync();
            // SQL Query
            string query = "SELECT COUNT(*) FROM search.cubis_product";
            using (SqlCommand command = new SqlCommand(query, connection))
            {
                rowCount = (int)await command.ExecuteScalarAsync();
                // Return the row count as the function's return value                    
            }
            // If no rows were found, return a default value
            return new OkObjectResult(rowCount.ToString());
        }
    }

        // logging disabled
    // public static class azap_audit // logging disabled
    // {
    // public static string Auditlogger(String logname, String functionname, String load_id, String pipeline, String infostr, String runtime)    {

    //         string connectionString = Environment.GetEnvironmentVariable("auditlogConnection");  
    //         String result="";
    //         var query=$"";
    //         // Using the connection string to open a connection
    //         try{
    //             using(SqlConnection connection = new SqlConnection(connectionString)){
    //                 // Opening a connection
    //                 connection.Open(); 
    //                 switch (logname)     
    //                     {
    //                         case "log_azurefunction":
    //                              query = $"EXEC [audit].[usp_log_azurefunction_add] @pipeline = N'{pipeline}',@load_id ={load_id},@azurefunction = N'{functionname}',@infostr = N'{infostr}',@runtime ='{runtime}'";
    //                             break;    
    //                         case "log_blobcopy":
    //                              query = $"EXEC [audit].[usp_log_blobcopy_add] @pipeline = N'{pipeline}',@load_id ={load_id},@azurefunction = N'{functionname}',@blobname = N'{infostr}',@runtime ='{runtime}'";
    //                             break;             
    //                         default:
    //                           query = $"Select 0";
    //                         break;
    //                     }
                   
    //                 SqlCommand command = new SqlCommand(query,connection); 
    //                 // Open the connection, execute and close connection
    //                 if(command.Connection.State == System.Data.ConnectionState.Open){
    //                     command.Connection.Close();
    //                 }
    //                 command.Connection.Open();
    //                 command.ExecuteNonQuery(); 
    //                 result="Success";
    //             }
    //         }
    //         catch(Exception e){               
    //             result = e.ToString();
    //         }
    //     return result;
    //     }
    // }

}