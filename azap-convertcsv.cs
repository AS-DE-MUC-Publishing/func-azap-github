using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Logging;
using Microsoft.VisualBasic.FileIO;
using System.Data;
using System.IO;
using System.Linq;
using System.Threading.Tasks;

public static class ReplaceCsvFunction
{
    [FunctionName("ReplaceCsv")]
    public static async Task<IActionResult> Run(
        [HttpTrigger(AuthorizationLevel.Function, "post", Route = null)] HttpRequest req,
        [Blob("{containerName}/{fileName}", FileAccess.Write)] Stream outputBlob,
        string fileName,
        string delimiter,
        string columns,
        string header_row,
        ILogger log)
    {
        log.LogInformation("C# HTTP trigger function processed a request.");

        // Read the input CSV file from the request body
        using (StreamReader reader = new StreamReader(req.Body))
        {
            // ------------- neue sinkTable ----------------
            DataTable sinkTable = new DataTable();
            int rownum = 1;
            DateTime blobstart = DateTime.Now;

            using (TextFieldParser parser = new TextFieldParser(reader.BaseStream, System.Text.Encoding.UTF8))
            {
                parser.TextFieldType = FieldType.Delimited;
                parser.SetDelimiters(delimiter);
                parser.HasFieldsEnclosedInQuotes = true;
                parser.TrimWhiteSpace = true;

                // Write the output CSV file to the blob storage
                using (StreamWriter writer = new StreamWriter(outputBlob))
                {
                    while (!parser.EndOfData)
                    {
                        string[] fields = parser.ReadFields();
                        writer.WriteLine(string.Join(",", fields.Select(field => $"\"{field}\"")));
                    }
                }
            }
        }

        return new OkObjectResult($"File {fileName} has been replaced.");
    }
}