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

namespace azap
{
    public static class azap_copy_between_storageaccounts
    {   
        [FunctionName("copy-between-storageaccounts")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function,  "post", Route = null)] HttpRequest req,
            ILogger mylog)
        {
            //-----------------------  Parameter -----------------------------------

            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            dynamic data = JsonConvert.DeserializeObject(requestBody);
             string storageAccount_sink =data?.storageAccount_sink; 
            string container_sink = data?.container_sink;  
            string storageAccount_source =data?.storageAccount_source; 
            string container_source = data?.container_source;  
            string filepath = data?.filepath + "/"; 
            string source_filename_option=data?.source_filename_option;     
            string days_lastmodified =data?.days_lastmodified; 

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
            if (blobItem.Name.Contains(source_filename_option) || source_filename_option=="" )         
            {
                if ( DateTime.Now.AddDays(-1*days)<blobItem.Properties.LastModified)
                {  
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
         
            return new OkObjectResult(new {Result = "Success"});            
        }       
     }
}
