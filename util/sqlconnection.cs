using Azure.Storage.Blobs;
using Microsoft.Extensions.Logging;
using System;
using Azure.Identity;
using Microsoft.Data.SqlClient;
using Azure.Security.KeyVault.Secrets;


namespace azap.util
{
    public class AzureSqlConnection
    {
        public readonly SqlConnection _connection;
        private readonly ILogger _log = null;

        public AzureSqlConnection(ILogger log, dynamic data)
        {
              
            _log = log;       
            string database = data?.database;
            string servername = data?.servername; 
            string user = data?.user;  
            string authentication=data?.authentication;
            string environment=data?.environment;

            string connectionString;
            string url_token;
      

            // Create a new SecretClient using the DefaultAzureCredential
            if (user == "msi") {
                var credential = new DefaultAzureCredential(); 
                _log.LogInformation("MSI-Authentification: " + authentication);
                if (authentication=="token") {
                    connectionString=$"Server=tcp:{servername},1433;Initial Catalog={database};";  
                    if (servername.Contains("azuresynapse")) {
                        url_token="https://sql.azuresynapse.net/.default";
                       // mylog.LogInformation("url_token from: " + authentication);
                    }
                     else {
                        url_token="https://database.windows.net/.default";             
                    }                  
                    var token = credential.GetToken(new Azure.Core.TokenRequestContext(new[] { url_token }));
                  //  _log.LogInformation("token: " + token);
                    _connection = new SqlConnection(connectionString);
                    _connection.AccessToken = token.Token;                     
                }
                    else
                { 
                    connectionString=$"Server=tcp:{servername},1433;Initial Catalog={database};Authentication=Active Directory Managed Identity;";    
                    _connection = new SqlConnection(connectionString);                   
                   
                }
                 
            }
            else
            {
                var kvUri = $"https://kv-azap-common-{environment}.vault.azure.net";
                var secretClient = new SecretClient(new Uri(kvUri), new DefaultAzureCredential());
                KeyVaultSecret secret = secretClient.GetSecret("pw-supersearch");
                connectionString = $"Server=tcp:{servername},1433;Initial Catalog={database};User ID={user};Password={secret.Value};";
             //   _log.LogInformation("SQL-User-Authentification: " + authentication);
                _connection = new SqlConnection(connectionString);    
            }
            // var credential = new DefaultAzureCredential();
            // _log = log;            
            // _containerClient = new BlobContainerClient(new Uri($"https://{storageAccount}.blob.core.windows.net/{Container}"), credential);  
            // _log.LogInformation($"DatalakeClient: {_containerClient.Uri}");         
        }

    }

}