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
    public static class azap_supersearch_semantic
    {
        [FunctionName("azap-supersearch-semantic")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "post", Route = null)] HttpRequest req,
            ILogger mylog)
        {           
            //-----------------------  Parameter -----------------------------------
            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            dynamic data = JsonConvert.DeserializeObject(requestBody);   
            string input=data?.input;
            string searchtype=data?.searchtype;
            string top=data?.top;
            string environment=data?.environment;

            string vector = await new vectorizer().vectorize(input, environment);

            string query="EXECUTE [vector_function].[usp_linked_editions_similarity]  '" + vector + "' ," + top + "  , '" + searchtype + "' ;";

            SqlConnection connection=new AzureSqlConnection(mylog, data)._connection;

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