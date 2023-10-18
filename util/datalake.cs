using Azure.Storage.Blobs;
using Microsoft.Extensions.Logging;
using System;
using Azure.Identity;


namespace azap.util
{
    public class DatalakeClient
    {
        public readonly BlobContainerClient _containerClient;
        private readonly ILogger _log = null;

        public DatalakeClient(ILogger log, string storageAccount, string Container)
        {
            var credential = new DefaultAzureCredential();
            _log = log;            
            _containerClient = new BlobContainerClient(new Uri($"https://{storageAccount}.blob.core.windows.net/{Container}"), credential);           
        }

    }

}