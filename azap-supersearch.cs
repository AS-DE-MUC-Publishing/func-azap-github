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
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using System.Collections.Generic;
using System.Data;
using Microsoft.VisualBasic.FileIO;
using azap.util;
using System.Globalization;
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
            string workspace = data?.workspace; 
            string environment=data?.environment;  
            // string user = data?.user;  
            // Create a new SecretClient using the DefaultAzureCredential
            string username ="supersearch";

            var kvUri = $"https://kv-azap-common-{environment}.vault.azure.net";
            var secretClient = new SecretClient(new Uri(kvUri), new DefaultAzureCredential());

// Retrieve the secret
KeyVaultSecret secret = await secretClient.GetSecretAsync("pw-supersearch");
 mylog.LogInformation("secret: " + secret.Value);
// Use the secret in your connection string
// string connectionString = $"Server=tcp:syn-azap-{workspace}-{environment}.sql.azuresynapse.net,1433;Initial Catalog={database};User ID={username};Password={secret.Value};";
//  mylog.LogInformation("connectionString: " + connectionString);
// Rest of your code...
            
            
            
            // SQL Connection



string connectionString = $"Server=tcp:syn-azap-{workspace}-{environment}.sql.azuresynapse.net,1433;Initial Catalog={database};Authentication=Active Directory Managed Identity;";//TrustServerCertificate=True";
//             string connectionString = $"Server=tcp:syn-azap-{workspace}-{environment}.sql.azuresynapse.net,1433;Initial Catalog={database};User ID={username};Password={password};";
//             mylog.LogInformation("connectionString: " + connectionString);
//             SqlConnection connection = new SqlConnection(connectionString); 
//            //SqlConnection connection = new SqlConnection("Server=tcp:<servername>.database.windows.net;Database=<DBNAME>;Authentication=Active Directory Default;User Id=adf391a2-652a-4f84-8d96-e5efce57d19b;TrustServerCertificate=True");  // user-assigned identity
//             var credential = new DefaultAzureCredential(); 
//  // var credential = new Azure.Identity.DefaultAzureCredential(new DefaultAzureCredentialOptions { ManagedIdentityClientId = '<client-id-of-user-assigned-identity>' }); // user-assigned identity
//             var token = credential.GetToken(new Azure.Core.TokenRequestContext(new[] { "https://database.windows.net/.default" }));
//             connection.AccessToken = token.Token;
//             mylog.LogInformation("token.Token: " + token.Token);


            //using Microsoft.Data.SqlClient.SqlConnection connection = new(connectionString);

             SqlConnection connection = new SqlConnection(connectionString); 

            await connection.OpenAsync();
                // SQL Query
                string query = "SELECT COUNT(*) FROM global.cubis_product";
                using (SqlCommand command = new SqlCommand(query, connection))
                {
                    int rowCount = (int)await command.ExecuteScalarAsync();
                    // Return the row count as the function's return value                    
                }
  
            // If no rows were found, return a default value
            return new OkObjectResult(0);
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