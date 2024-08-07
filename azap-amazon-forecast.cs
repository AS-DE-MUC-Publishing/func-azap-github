
using Microsoft.AspNetCore.Mvc;
using Microsoft.AspNetCore.Http;
using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using System.Data;
using Microsoft.VisualBasic.FileIO;
using azap.util;
using System.Globalization;
using Microsoft.Azure.Functions.Worker;

namespace azap
{
    public  class azap_amazon_forecast {

      private readonly ILogger<azap_amazon_forecast>? _logger;

        public azap_amazon_forecast(ILogger<azap_amazon_forecast>? logger)
        {
            _logger = logger;
        }

        [Function("amazon-forecast")]
        public static async Task<IActionResult> Run([HttpTrigger(AuthorizationLevel.Function, "get", "post")] HttpRequest req, FunctionContext executionContext)
        {
            ILogger logger = executionContext.GetLogger("amazon-forecast");
            //-----------------------  Parameter -----------------------------------
            string requestBody = await new StreamReader(req.Body).ReadToEndAsync();
            dynamic data = JsonConvert.DeserializeObject(requestBody);             
            string storageAccount =data?.storageAccount; 
            string header_row=data?.header_row;   
            string sourceContainer = data?.sourceContainer;  
            string sinkContainer=data?.sinkContainer;  
            string filepath = data?.filepath + "/"; 
            string source_filename=data?.source_filename;            
            string source_suffix=data?.source_suffix;
            string sink_filename=data?.sink_filename;
            string sink_filename_withDate=data?.sink_filename_withDate;               
            string days_lastmodified =data?.days_lastmodified;

            //-----------------------  Variables -----------------------------------

            int days=Int16.Parse(days_lastmodified); 
            CultureInfo provider = CultureInfo.InvariantCulture;  
            int copyCount=0;
            string[] header_source=new string[100];
            List<SelectedColumn> ColumnPosition = new List<SelectedColumn>();   
            int int_header_row=Int16.Parse(header_row);

            // ---------------------- Storage Account --------------------
            var adls_source = new DatalakeClient( storageAccount, sourceContainer);
            var adls_sink = new DatalakeClient( storageAccount, sinkContainer);
            var adls_log = new DatalakeClient( storageAccount, "importlogs");     
          
         // ---------------------- Reading from Source --------------------

            // truncate  temp
        await foreach (BlobItem blobItem in adls_source._containerClient.GetBlobsAsync(BlobTraits.None, BlobStates.None, filepath ))   
           {   
                 logger.LogInformation("blobItem.Name: " + blobItem.Name);

        if (blobItem.Name.Contains(source_filename) && blobItem.Name.EndsWith(source_suffix))         
        {
            if ( DateTime.Now.AddDays(-1*days)<blobItem.Properties.LastModified)
            {  

            BlobClient sourceBlob=adls_source._containerClient.GetBlobClient(blobItem.Name);
            var stream = await sourceBlob.OpenReadAsync();

           // ------------- neue sinkTable ----------------
            DataTable sinkTable = new amazon_forecastTable(); 
            DataTable logTable = new importLogTable(); 
            int rownum=1;   
            String Datum_id=ConvertDate.Date2ConvertWithPattern(blobItem.Name.Replace(filepath + source_filename,""), "[0-9]", sink_filename_withDate.ToLower());
            String Datum=Datum_id.ToString();
            logger.LogInformation("Datum: " + Datum);
            DateTime blobstart=DateTime.Now;               

        using (TextFieldParser parser = new TextFieldParser(stream, System.Text.Encoding.UTF8))
             {
            parser.TextFieldType = FieldType.Delimited;
            parser.SetDelimiters(",");
            parser.HasFieldsEnclosedInQuotes = true;
            parser.TrimWhiteSpace = true;
           

            while (rownum < Int16.Parse(header_row))
                 {
                    parser.ReadFields();                    
                    logger.LogInformation("rownum: " + rownum);
                    rownum=rownum+1;                       
                } 

            if (rownum==Int16.Parse(header_row))
                {
                    logger.LogInformation("Header aus der Datenquelle wird gelesen");   
                    header_source = parser.ReadFields();  
                    rownum=rownum+1;                 
                }       
                             

                while (!parser.EndOfData)
                        {                            
                            string[] fields = parser.ReadFields();
                            for (int i = 0; i < fields.Length; i++) 
                            {
                               
                                if (header_source[i].Contains("Woche"))
                                {
                                DataRow newDataRow = sinkTable.NewRow();
                                newDataRow[0]=fields[0];                                
                                newDataRow[1]=fields[2];
                                newDataRow[2]=Datum_id;
                                //newDataRow[3]=calendarWeek;                                
                                newDataRow[3]=header_source[i];
                                newDataRow[4]=(fields[i].Length==0) ? DBNull.Value: fields[i].Replace(".","");
                                sinkTable.Rows.Add(newDataRow);
                                }
                            //  log.LogInformation("parsed field: " + fields[col.Position]);
                            }
                            rownum=rownum+1;
                        }  

                     DataRow logDataRow = logTable.NewRow();
                     logDataRow[0]=Datum_id;       
                     //logDataRow[1]=calendarWeek;                                       
                     logDataRow[1]=rownum;
                     logDataRow[2]=stream.Length;                      
                     logTable.Rows.Add(logDataRow);  

                     parser.Close();                       
                }    

                 // --------------- write sink file ------------------------------------------------------   

                string sinkFile=filepath  + Datum_id.Substring(0,4) +"/" + Datum_id.Substring(4,2) +"/" + sink_filename + Datum_id + ".parquet";
                logger.LogInformation("filename: " + sinkFile);  
                BlobClient sinkBlobClient= adls_sink._containerClient.GetBlobClient(sinkFile);   
                var parquetClient = new ParquetClient(logger);    
                await parquetClient.WriteDataTableToParquet(sinkTable, sinkFile, sinkBlobClient);    

                // --------------- write log file ------------------------------------------------------   
                string logFile=sinkFile; 
                BlobClient logBlobClient= adls_log._containerClient.GetBlobClient(sinkFile);   
                var logParquetClient = new ParquetClient(logger);    
                await logParquetClient.WriteDataTableToParquet(logTable, logFile, logBlobClient, false);  

                ColumnPosition.Clear();
                sinkTable.Clear();
                copyCount=copyCount+1;
                String blobtime=(DateTime.Now-blobstart).TotalSeconds.ToString();
              
            }
        }   // if (blobItem.Name.Contains("_wait_for_cleaning")  )                                                 
        } // await foreach (BlobItem blobItem 
            string infostr=filepath  + ": " + copyCount.ToString() + " Files filtered in " + sourceContainer ;          
            return new OkObjectResult(new {Result = "success"});            
        }  //public static async
    }  //public static class azap_extract_column
}
