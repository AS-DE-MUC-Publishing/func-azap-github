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
using System.Collections.Generic;
using System.Data;
using Microsoft.VisualBasic.FileIO;
using azap.util;
using System.Globalization;

namespace azap
{
    public static class azap_amazon_forecast
    {
        [FunctionName("amazon-forecast")]
        public static async Task<IActionResult> Run(
            [HttpTrigger(AuthorizationLevel.Function, "post", Route = null)] HttpRequest req,
            ILogger mylog)
        {                    

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
            var adls_source = new DatalakeClient(mylog, storageAccount, sourceContainer);
            var adls_sink = new DatalakeClient(mylog, storageAccount, sinkContainer);
            var adls_log = new DatalakeClient(mylog, storageAccount, "importlogs");     
          
         // ---------------------- Reading from Source --------------------

            // truncate  temp
        await foreach (BlobItem blobItem in adls_source._containerClient.GetBlobsAsync(BlobTraits.None, BlobStates.None, filepath ))   
           {   
                 mylog.LogInformation("blobItem.Name: " + blobItem.Name);

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
            mylog.LogInformation("Datum: " + Datum);
            DateTime blobstart=DateTime.Now;  

        // // Aktuelle Kultur ermitteln
        //     CultureInfo currentCulture = CultureInfo.CurrentCulture;

        // // Aktuellen Kalender ermitteln
        //     Calendar calendar = currentCulture.Calendar;
        //     DateTime kwDatum = DateTime.Parse(Datum_id.Substring(6,2) + "." + Datum_id.Substring(4,2) + "." + Datum_id.Substring(0,4));

        //  // Kalenderwoche über das Calendar-Objekt ermitteln
        //     int calendarWeek = calendar.GetWeekOfYear(kwDatum, currentCulture.DateTimeFormat.CalendarWeekRule, currentCulture.DateTimeFormat.FirstDayOfWeek);
                      

        using (TextFieldParser parser = new TextFieldParser(stream, System.Text.Encoding.UTF8))
             {
            parser.TextFieldType = FieldType.Delimited;
            parser.SetDelimiters(",");
            parser.HasFieldsEnclosedInQuotes = true;
            parser.TrimWhiteSpace = true;
           

            while (rownum < Int16.Parse(header_row))
                 {
                    parser.ReadFields();                    
                    mylog.LogInformation("rownum: " + rownum);
                    rownum=rownum+1;                       
                } 

            if (rownum==Int16.Parse(header_row))
                {
                    mylog.LogInformation("Header aus der Datenquelle wird gelesen");   
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
                mylog.LogInformation("filename: " + sinkFile);  
                BlobClient sinkBlobClient= adls_sink._containerClient.GetBlobClient(sinkFile);   
                var parquetClient = new ParquetClient(mylog);    
                parquetClient.writeToParquet(sinkTable, sinkFile, sinkBlobClient);    

                // --------------- write log file ------------------------------------------------------   
                string logFile=sinkFile; 
                BlobClient logBlobClient= adls_log._containerClient.GetBlobClient(sinkFile);   
                var logParquetClient = new ParquetClient(mylog);    
                logParquetClient.writeToParquet(logTable, logFile, logBlobClient);  

                ColumnPosition.Clear();
                sinkTable.Clear();
                copyCount=copyCount+1;
                String blobtime=(DateTime.Now-blobstart).TotalSeconds.ToString();
              
            }
        }   // if (blobItem.Name.Contains("_wait_for_cleaning")  )                                                 
        } // await foreach (BlobItem blobItem 
            string infostr=filepath  + ": " + copyCount.ToString() + " Files filtered in " + sourceContainer ;          
            return new OkObjectResult(new {Result = "Success"});            
        }  //public static async
    }  //public static class azap_extract_column
}
