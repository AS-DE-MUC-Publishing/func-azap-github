using System.Text;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using azap.util;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.DurableTask;
using Microsoft.DurableTask.Client;
using Azure.Storage.Sas;
using Azure.Storage.Blobs.Specialized;
using Azure;


namespace azap
{
    public class copy_between_storageaccounts
    {   
        private readonly ILogger<copy_between_storageaccounts> _logger;        

        public copy_between_storageaccounts(ILogger<copy_between_storageaccounts> logger)
        {
            _logger = logger;
        }
//--------------------------------------------------------------------------------------------------------------------------------------
//-------------------H T T P - T R I G G E R  ------------------------------------------------------------------------------------------
//--------------------------------------------------------------------------------------------------------------------------------------
        
        [Function("copy-between-storageaccounts")]
        public static async Task<HttpResponseData> HttpStart(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post")] Microsoft.Azure.Functions.Worker.Http.HttpRequestData req,
            [DurableClient] DurableTaskClient client,
            FunctionContext executionContext)
        {
            ILogger logger = executionContext.GetLogger("copy-between-storageaccounts_HttpStart");

            // Function input comes from the request content.
            string instanceId = await client.ScheduleNewOrchestrationInstanceAsync(nameof(copy_between_storageaccounts), await req.ReadAsStringAsync(),null);

            logger.LogInformation("Started orchestration with ID = '{instanceId}'.", instanceId);

            return await client.CreateCheckStatusResponseAsync(req, instanceId);
        }

//--------------------------------------------------------------------------------------------------------------------------------------
//-------------------O R C H E S T R A T O R - D U R A B L E   T A S K  ----------------------------------------------------------------
//--------------------------------------------------------------------------------------------------------------------------------------
        // expects a json string with the following properties:
        // {"function":"value" ... other key-value pairs ...} ... value can be currently "opus" or "fias"
        
        [Function(nameof(copy_between_storageaccounts))]
        public static async Task<List<string>> RunOrchestrator(
            [OrchestrationTrigger] TaskOrchestrationContext context)
        {
            ILogger logger = context.CreateReplaySafeLogger(nameof(copy_between_storageaccounts));
            var outputs = new List<string>();
            // Deserialize the input data

            string input = context.GetInput<string>();
            byte[] inputBytes = Encoding.UTF8.GetBytes(input);
            dynamic data = JsonConvert.DeserializeObject(input);
            // Check the value of the "function" property
            string function = data?.function;
            using (var stream = new MemoryStream(inputBytes))
            using (var reader = new StreamReader(stream))
            {
                string request = await reader.ReadToEndAsync();
                outputs.Add(await context.CallActivityAsync<string>("copy-activity", request));
                                      
            }            
            return outputs;
        }
        
//--------------------------------------------------------------------------------------------------------------------------------------
//-------------------COPY FUNCTION  ---------------------------------------------------------------------------------------------------
//--------------------------------------------------------------------------------------------------------------------------------------
 
       
        [Function("copy-activity")]
         public static async Task<string>CopyActivity([ActivityTrigger]  string request, FunctionContext executionContext)
        {
            ILogger logger = executionContext.GetLogger("copy-activity");
            logger.LogInformation("copy-between-subscriptions called");
           
            dynamic data = JsonConvert.DeserializeObject(request);     
            string storageAccount_sink =data?.storageAccount_sink; 
            string container_sink = data?.container_sink;  
            string storageAccount_source =data?.storageAccount_source; 
            string container_source = data?.container_source;  
            string filepath = data?.filepath + "/"; 
            string source_filename_option=data?.source_filename_option;     
            string days_lastmodified =data?.days_lastmodified; 
            bool delete_sinkfolder = data?.delete_sinkfolder ?? false;

            //-----------------------  Variables -----------------------------------
           
            int copyCount=0; 
            short number;
            bool isParsable=Int16.TryParse(days_lastmodified, out number);
            int days= isParsable ? number: 3;
            string resultstring="";
        try {
            // ---------------------- Storage Account --------------------         
            var adls_source = new DatalakeClient(storageAccount_source, container_source);   
            var adls_sink = new DatalakeClient(storageAccount_sink, container_sink);

                    
        await foreach (BlobItem blobItem in adls_source._containerClient.GetBlobsAsync(BlobTraits.None, BlobStates.None, filepath ))   
           {  

            if ((blobItem.Name.Contains(source_filename_option) || source_filename_option == "") && !blobItem.Name.EndsWith("_SUCCESS") )
            {
                if ( DateTime.Now.AddDays(-1*days)<blobItem.Properties.LastModified)
                {  
                       
                    string folderPath = Path.GetDirectoryName(blobItem.Name);
                    //logger.LogInformation("folderPath: " + folderPath);
                    if (folderPath != null  &&  !storageAccount_sink.EndsWith("prod") && delete_sinkfolder)
                    {
                        BlobContainerClient sinkContainer = adls_sink._containerClient;
                        folderPath=folderPath.Replace("\\","/");
                     await foreach (BlobItem blob in sinkContainer.GetBlobsAsync(prefix: folderPath))
                    {
                        if (blob.Name.Contains(source_filename_option))
                        {
                            try
                            {
                                await sinkContainer.GetBlobClient(blob.Name).DeleteIfExistsAsync();
                                logger.LogInformation("Deleted existing blob in sink: " + blob.Name);
                            }
                            catch (RequestFailedException ex) when (ex.ErrorCode == BlobErrorCode.ConditionNotMet)
                            {
                                logger.LogWarning($"Failed to delete blob {blob.Name} due to condition not met: {ex.Message}");
                            }
                            catch (RequestFailedException ex)
                            {
                                logger.LogError($"Failed to delete blob {blob.Name}: {ex.Message}");
                                throw;
                            }
                        }
                    }
                    }
                    
                    
                    // mylog.LogInformation("Found blob:  " + blobItem.Name   );   
                    BlobClient sinkBlob = adls_sink._containerClient.GetBlobClient(blobItem.Name);
                    BlobClient sourceBlob=adls_source._containerClient.GetBlobClient(blobItem.Name);    

                    Uri sourceUri = sourceBlob.Uri;

                    if (storageAccount_sink!=storageAccount_source)
                    {
                    var blobServiceClient = sourceBlob
                                            .GetParentBlobContainerClient().GetParentBlobServiceClient();
                    var userDelegationKey = await blobServiceClient
                                            .GetUserDelegationKeyAsync(DateTimeOffset.UtcNow, DateTimeOffset.UtcNow.AddHours(1));

                    var sasBuilder = new BlobSasBuilder()
                    {
                        BlobContainerName = sourceBlob.BlobContainerName,
                        BlobName = sourceBlob.Name,
                        Resource = "b",
                        StartsOn = DateTimeOffset.UtcNow,
                        ExpiresOn = DateTimeOffset.UtcNow.AddHours(1)
                    };

                    sasBuilder.SetPermissions(BlobSasPermissions.Read);

                    var blobUriBuilder = new BlobUriBuilder(sourceBlob.Uri)
                    {
                        Sas = sasBuilder.ToSasQueryParameters(
                                userDelegationKey, 
                                blobServiceClient.AccountName)
                    };

                    var uri = blobUriBuilder.ToUri();
                    sourceUri = uri;
                   
                    CopyFromUriOperation response = await sinkBlob.StartCopyFromUriAsync(sourceUri).ConfigureAwait(false);
                    logger.LogInformation($"Started copy of blob {blobItem.Name} from source to sink.");
                    await response.WaitForCompletionAsync().ConfigureAwait(false);

                    // await sinkBlob.StartCopyFromUriAsync (sourceBlob.GenerateSasUri);
                    logger.LogInformation("Copied " + blobItem.Name   );   
                    copyCount=copyCount+1; 
                    }                                                   
                }  
            }          
            }  
            logger.LogInformation($"Total blobs copied: {copyCount}");
            resultstring="success"; 
            return resultstring;              
            //} // try
            // catch  (Exception ex)
            // {
            
            //         resultstring="error: " + ex.Message;
            //         return resultstring;
            // }  // catch   
             }
            catch (RequestFailedException ex)
            {
                logger.LogError($"Request failed: {ex.Message}");
                throw;
            }
            catch (Exception ex)
            {
                logger.LogError($"An unexpected error occurred: {ex.Message}");
                throw;
            }      
        }       
     }
}
