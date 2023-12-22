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
using azap.util;
using System.Data;
using Microsoft.VisualBasic.FileIO;

namespace azap
{
    public static class azap_blobcopy
    {   
        [FunctionName("blobcopy")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function,  "post", Route = null)] HttpRequest req,
            ILogger mylog)
        {
            //-----------------------  Parameter -----------------------------------

            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            dynamic data = JsonConvert.DeserializeObject(requestBody);
            string storageAccount =data?.storageAccount; 
            string sourceContainer = data?.sourceContainer;  
            string sinkContainer=data?.sinkContainer;  
            string filepath = data?.filepath + "/"; 
            string source_filename=data?.source_filename;            
            string source_suffix=data?.source_suffix;
            string sink_filename=data?.sink_filename;
            string days_lastmodified =data?.days_lastmodified; 

            //-----------------------  Variables -----------------------------------
           
            int copyCount=0; 
            short number;
            bool isParsable=Int16.TryParse(days_lastmodified, out number);
            int days= isParsable ? number: 3;

            // ---------------------- Storage Account --------------------         
            var adls_source = new DatalakeClient(mylog, storageAccount, sourceContainer);   
            var adls_sink = new DatalakeClient(mylog, storageAccount, sinkContainer);
           // var adls_log = new DatalakeClient(mylog, storageAccount, "importlogs");
            
        await foreach (BlobItem blobItem in adls_source._containerClient.GetBlobsAsync(BlobTraits.None, BlobStates.None, filepath ))   
        {   
            // mylog.LogInformation("blobItem.Name: " + blobItem.Name);

            if (blobItem.Name.Contains(source_filename) && blobItem.Name.EndsWith(source_suffix))         
            {
                // ...

                using (StreamReader reader = new StreamReader(await adls_source._containerClient.GetBlobClient(blobItem.Name).OpenReadAsync()))
                using (TextFieldParser parser = new TextFieldParser(reader.BaseStream, System.Text.Encoding.UTF8))
                {
                    // Rest of the code
                }
            }          
        }                           
         
            string infostr=sink_filename  + ": " + copyCount.ToString() + " Files copied from " + sourceContainer + " to " + sinkContainer;
            return new OkObjectResult(new {Result = "Success"});            
        }       
     }
}
