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
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;



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

        public async Task<string> vectorize_vision_text(string input, string environment)
        {
           
            var keyVaultUrl = $"https://kv-azap-common-{environment}.vault.azure.net";

            var client = new SecretClient(new Uri(keyVaultUrl), new DefaultAzureCredential());
            KeyVaultSecret key_embedding = await client.GetSecretAsync("key-ai-vision-api");
            KeyVaultSecret url_embedding = await client.GetSecretAsync("url-ai-vision-api-text");


            Uri visionEndpoint = new (url_embedding.Value);
            AzureKeyCredential credentials = new (key_embedding.Value);

            using (var httpClient = new HttpClient())
            {
                // httpClient.DefaultRequestHeaders.Authorization = new AuthenticationHeaderValue("Ocp-Apim-Subscription-Key", key_embedding.Value);
                 httpClient.DefaultRequestHeaders.Add("Ocp-Apim-Subscription-Key", key_embedding.Value);
                var request = new HttpRequestMessage(HttpMethod.Post, url_embedding.Value);                
                var requestBody = new { text = input };
                var jsonRequest = JsonConvert.SerializeObject(requestBody);
                request.Content = new StringContent(jsonRequest, Encoding.UTF8, "application/json");

                var httpResponse = await httpClient.SendAsync(request);
                var jsonResponse = await httpResponse.Content.ReadAsStringAsync();

                var vectorObject = JsonConvert.DeserializeObject<JObject>(jsonResponse);
                var vectorArray = vectorObject["vector"].ToObject<float[]>();

                var response = string.Join(",\n", vectorArray.Select(x => x.ToString(System.Globalization.CultureInfo.InvariantCulture)));
                response = "[" + response + "]";
                return response;
            }

        }

    }
}



