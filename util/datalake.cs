using Azure.Storage.Blobs;
using Azure.Identity;


namespace azap.util
{
    public class DatalakeClient
    {
        public readonly BlobContainerClient _containerClient;

        public DatalakeClient( string storageAccount, string Container)
        {              
            var credential = new DefaultAzureCredential();
            _containerClient = new BlobContainerClient(new Uri($"https://{storageAccount}.blob.core.windows.net/{Container}"), credential);  
        }

    }

}