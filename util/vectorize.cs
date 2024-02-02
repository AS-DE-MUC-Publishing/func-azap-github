using System;
using System.Threading.Tasks;
using Azure.Identity;
using Azure.Security.KeyVault.Secrets;
using System.Net.Http;
using System.Net.Http.Headers;
using System.Text;
using System.Net;
using Azure;
using Azure.AI.OpenAI;
using System.Linq;


namespace azap.util
{
    public class vectorizer
    {
        public async Task<string> vectorize(string input, string environment)
        {
           
            var keyVaultUrl = $"https://kv-azap-common-{environment}.vault.azure.net";

            var client = new SecretClient(new Uri(keyVaultUrl), new DefaultAzureCredential());
            KeyVaultSecret key_embedding = await client.GetSecretAsync("key-embedding-ada-002");
            KeyVaultSecret url_embedding = await client.GetSecretAsync("url-embedding-ada-002");

            string response;

            Uri oaiEndpoint = new (url_embedding.Value);
            AzureKeyCredential credentials = new (key_embedding.Value);
            OpenAIClient openAIClient = new (oaiEndpoint, credentials);
            EmbeddingsOptions embeddingOptions = new()
            {
                DeploymentName = "text-embedding-ada-002",
                Input = { input },
            };

            var returnValue = openAIClient.GetEmbeddings(embeddingOptions);
            // foreach (float item in returnValue.Value.Data[0].Embedding.ToArray())
            // {
            //     Console.WriteLine(item);
            // }

            response=string.Join("\n", returnValue.Value.Data[0].Embedding.ToArray());

            response = string.Join(",\n", returnValue.Value.Data[0].Embedding.ToArray().Select(x => x.ToString(System.Globalization.CultureInfo.InvariantCulture)));
            response="[\n"+response+"\n]";
            return response;
        }

    }
}



