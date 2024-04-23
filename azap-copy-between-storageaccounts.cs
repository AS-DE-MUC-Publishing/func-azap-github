using System;
using System.IO;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using Azure.Storage.Sas;
using azap.util;
using Azure.Storage.Blobs.Specialized;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using System.Net.Http;
using System.Collections.Generic;
using System.Text;


namespace azap
{
    public static class azap_copy_between_storageaccounts
    {   
        
        
        [FunctionName("copy-between-storageaccounts")]
        public static async Task<HttpResponseMessage> HttpStart(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post")] HttpRequestMessage req,
            [DurableClient] IDurableOrchestrationClient starter,
            ILogger log)
        {
            // Function input comes from the request content.
            string instanceId = await starter.StartNewAsync("copy-between-storageaccounts-orchestrator",  null,  await req.Content.ReadAsStringAsync());

            log.LogInformation("Started orchestration with ID = '{instanceId}'.", instanceId);

            return starter.CreateCheckStatusResponse(req, instanceId);
        }

          [FunctionName("copy-between-storageaccounts-orchestrator")]
        public static async Task<List<string>> RunOrchestrator(
            [OrchestrationTrigger] IDurableOrchestrationContext context)
        {
            var outputs = new List<string>();

            string input = context.GetInput<string>();
            byte[] inputBytes = Encoding.UTF8.GetBytes(input);
            using (var stream = new MemoryStream(inputBytes))
            using (var reader = new StreamReader(stream))
            {
                string request = await reader.ReadToEndAsync();
                outputs.Add(await context.CallActivityAsync<string>("copy-between-storageaccounts-activity", request));
            }
            return outputs;
        }

        
        [FunctionName("copy-between-storageaccounts-activity")]
        public static async Task<string> Activity([ActivityTrigger]  string request, ILogger mylog)
        {
           mylog.LogInformation("Activity function copy-between-storageaccounts startet");     
            //-----------------------  Parameter -----------------------------------

             //-----------------------  Parameter -----------------------------------
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

            // ---------------------- Storage Account --------------------         
            var adls_source = new DatalakeClient(mylog, storageAccount_source, container_source);   
            var adls_sink = new DatalakeClient(mylog, storageAccount_sink, container_sink);

                    
        await foreach (BlobItem blobItem in adls_source._containerClient.GetBlobsAsync(BlobTraits.None, BlobStates.None, filepath ))   
           {  

            if ((blobItem.Name.Contains(source_filename_option) || source_filename_option == "") && !blobItem.Name.EndsWith("_SUCCESS") )
            {
                if ( DateTime.Now.AddDays(-1*days)<blobItem.Properties.LastModified)
                {  
                       
                    string folderPath = Path.GetDirectoryName(blobItem.Name);
                    mylog.LogInformation("folderPath: " + folderPath);
                    if (folderPath != null  &&  !storageAccount_sink.EndsWith("prod") && delete_sinkfolder)
                    {
                        BlobContainerClient sinkContainer = adls_sink._containerClient;
                        folderPath=folderPath.Replace("\\","/");
                        await foreach (BlobItem blob in sinkContainer.GetBlobsAsync( prefix: folderPath))
                                    {
                                        if (DateTime.Now.AddMinutes(-30) > blob.Properties.LastModified && blob.Name.Contains(source_filename_option))
                                        {
                                        await sinkContainer.GetBlobClient(blob.Name).DeleteIfExistsAsync();
                                        mylog.LogInformation("Deleted exiting blob in sink : " + blob.Name);
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
                    }
                    CopyFromUriOperation response = await sinkBlob.StartCopyFromUriAsync(sourceUri).ConfigureAwait(false);
                    await response.WaitForCompletionAsync().ConfigureAwait(false);

                    // await sinkBlob.StartCopyFromUriAsync (sourceBlob.GenerateSasUri);
                    mylog.LogInformation("Copied " + blobItem.Name   );   
                    copyCount=copyCount+1;                                                    
                }  
            }          
            }                          
         
            return "Completed";            
        }       
     }
}
