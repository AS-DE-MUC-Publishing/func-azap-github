
using System;
using System.Threading;
using System.Threading.Tasks;
using Microsoft.AspNetCore.Mvc;
using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Extensions.Http;
using Microsoft.Extensions.Logging;
using System.Collections.Generic;
using System.Net.Http;
using Azure;
using Microsoft.Azure.WebJobs.Extensions.DurableTask;
using Newtonsoft.Json;

using System.IO;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using System.Text;

using System.Data;
using Microsoft.VisualBasic.FileIO;
using System.Linq;
using Azure.Identity;
using azap.util;
using System.Text.RegularExpressions;
using System.Globalization;

namespace azap
{
    public static class durable_ingest_orchestrator
    {
        [FunctionName("durable_ingest_orchestrator")]
        public static async Task<List<string>> RunOrchestrator(
            [OrchestrationTrigger] IDurableOrchestrationContext context)
        {
            var outputs = new List<string>();
            // Deserialize the input data

            string input = context.GetInput<string>();
            byte[] inputBytes = Encoding.UTF8.GetBytes(input);
            using (var stream = new MemoryStream(inputBytes))
            using (var reader = new StreamReader(stream))
            {
                string request = await reader.ReadToEndAsync();
                //  outputs.Add(await context.CallActivityAsync<string>("ingest-selectedcolumns", request));
                outputs.Add(await context.CallActivityAsync<string>(nameof(IngestColumns), request));
            }
            // returns ["Hello Tokyo!", "Hello Seattle!", "Hello London!"]
            return outputs;
        }

        // [FunctionName(nameof(SayHello))]
        // public static async Task<string> SayHello([ActivityTrigger] string request, ILogger mylog)
        // {
           
        //      mylog.LogInformation("Activity function ingest_selectedcolumns startet");        
        //     //-----------------------  Parameter -----------------------------------
        //     dynamic data = JsonConvert.DeserializeObject(request);           

        //     string storageAccount =data?.storageAccount; 
        //     string header_row=data?.header_row;   
        //     string sourceContainer = data?.sourceContainer;  
        //     string sinkContainer=data?.sinkContainer;  
        //     string filepath = data?.filepath + "/"; 
        //     string columns =data?.columns;  
        //     string source_filename=data?.source_filename;
        //     string source_suffix=data?.source_suffix;
        //     string sink_filename=data?.sink_filename;
        //     string sink_filename_withDate=data?.sink_filename_withDate;            
        //     string days_lastmodified =data?.days_lastmodified;  

        //     //-----------------------  Variables -----------------------------------         
        //     int days=Int16.Parse(days_lastmodified); 
        //     List<SelectedColumn> ColumnPosition = new List<SelectedColumn>();   
        //     int int_header_row=Int16.Parse(header_row);

        //     mylog.LogInformation("Parameter from requestbody read");      
        //     return $"Test fertig";
        // }      

        [FunctionName(nameof(IngestColumns))]
        public static async Task<string> IngestColumns([ActivityTrigger]  string request, ILogger mylog)
        {
           mylog.LogInformation("Activity function ingest_selectedcolumns startet");        
            //-----------------------  Parameter -----------------------------------
            dynamic data = JsonConvert.DeserializeObject(request);           

            string storageAccount =data?.storageAccount; 
            string header_row=data?.header_row;   
            string sourceContainer = data?.sourceContainer;  
            string sinkContainer=data?.sinkContainer;  
            string filepath = data?.filepath + "/"; 
            string columns =data?.columns;  
            string source_filename=data?.source_filename;
            string source_suffix=data?.source_suffix;
            string sink_filename=data?.sink_filename;
            string sink_filename_withDate=data?.sink_filename_withDate;            
            string days_lastmodified =data?.days_lastmodified;   
            string source_suffix_switch="";

            //-----------------------  Variables -----------------------------------         
            int days=Int16.Parse(days_lastmodified); 
            int copyCount=0;
            List<SelectedColumn> ColumnPosition = new List<SelectedColumn>();   
            int int_header_row=Int16.Parse(header_row);

            mylog.LogInformation("Parameter from requestbody read");      
           
           // ---------------------- Storage Account --------------------            
            var adls_source = new DatalakeClient(mylog, storageAccount, sourceContainer);
            var adls_sink = new DatalakeClient(mylog, storageAccount, sinkContainer);
            var adls_log = new DatalakeClient(mylog, storageAccount, "importlogs");         

            mylog.LogInformation("Datalakeclients created");       

        // // ---------------------- Reading from Source --------------------
        await foreach (BlobItem blobItem in adls_source._containerClient.GetBlobsAsync(BlobTraits.None, BlobStates.None, filepath))   
           {   
                 mylog.LogInformation("Blob found: " + blobItem.Name);

                 if(filepath.Contains("amazon/search")) 
                 {
                    if(!blobItem.Name.Contains(source_suffix))
                    {
                        source_suffix_switch = "#DE#Original.csv";
                    } else {
                        source_suffix_switch = source_suffix;
                    }
                 } else {
                        source_suffix_switch = source_suffix;                   
                 }

        if (blobItem.Name.Contains(source_filename) && blobItem.Name.EndsWith(source_suffix_switch))         
        {
            //if ( DateTime.Now.AddDays(-1*days)<blobItem.Properties.LastModified)
            if ( DateTime.Now.Subtract(TimeSpan.FromDays(Double.Parse(days_lastmodified)))<blobItem.Properties.LastModified)
            {
            //String Datum_id=blobItem.Name.Replace(filepath + source_filename,"").Substring(0,8);
            //String Datum=Datum_id.Substring(6,2) + "." + Datum_id.Substring(4,2) + "." +  Datum_id.Substring(0,4);
            //Console.WriteLine(DateTime.Now.Subtract(TimeSpan.FromDays(Double.Parse(days_lastmodified))));
            //Console.WriteLine(DateTime.Now.AddDays(-1*days)); 
            //Console.WriteLine(blobItem.Properties.LastModified);                        
            String Datum_id=ConvertDate.Date2ConvertWithPattern(blobItem.Name.Replace(filepath + source_filename,""), "[0-9]", sink_filename_withDate.ToLower());
            String Datum=Datum_id;


            mylog.LogInformation("Filename und Datum_id (Date2ConvertWithPattern): " + filepath + source_filename + " datum_id " + Datum_id);    

            // --------- actual culture -------------------
           CultureInfo myCI = new CultureInfo("en-US");
           Calendar myCal = myCI.Calendar;
           CalendarWeekRule cwr = myCI.DateTimeFormat.CalendarWeekRule;
           DayOfWeek firstDow = myCI.DateTimeFormat.FirstDayOfWeek;
          

            // --------- actual calendar -------------------
            //CultureInfo currentCulture = CultureInfo.CurrentCulture;
            //Calendar calendar = currentCulture.Calendar;
            string[] validformats = new[] { "MM/dd/yyyy", "yyyy/MM/dd", "dd.MM.yyyy", "yyyyMMdd" };

            //string test = Datum_id.Substring(4,2) + "/" + Datum_id.Substring(6,2) + "/" + Datum_id.Substring(0,4);
            DateTime kwDatum = DateTime.ParseExact(Datum, validformats, myCI);

            //-----AbfrageDatum auf Sonntag, dann Montag ---------
            DateTime dateManipulated = DateTime.ParseExact(Datum, validformats, myCI);
                // Display the DayOfWeek string representation
            Console.WriteLine(dateManipulated.DayOfWeek.ToString());
            if (dateManipulated.DayOfWeek.ToString() == "Sunday" && filepath == "amazon/search/week/")
            {
                string dateManipulatedDayOffset = kwDatum.AddDays(1).ToString("yyyyMMdd");
                Datum_id = dateManipulatedDayOffset;       
                Datum = dateManipulatedDayOffset;                      
                //Console.WriteLine(Datum_id);
            }

            // Kalenderwoche über das Calendar-Objekt ermitteln
           //int calendarWeek = calendar.GetWeekOfYear(kwDatum, currentCulture.DateTimeFormat.CalendarWeekRule, currentCulture.DateTimeFormat.FirstDayOfWeek);            
           // int calendarWeek = calendar.GetWeekOfYear(kwDatum, cwr, firstDow);               
            int calendarWeek = myCal.GetWeekOfYear(kwDatum, cwr, firstDow); 
            string kw = "";

            if(calendarWeek.ToString().Length == 1)           
            {
                kw = "0" + calendarWeek.ToString();
            } else {
                kw = calendarWeek.ToString();
            }

            mylog.LogInformation("blob to be processed: " + blobItem.Name);
            BlobClient sourceBlob=adls_source._containerClient.GetBlobClient(blobItem.Name);
            var stream = await sourceBlob.OpenReadAsync();

           // ------------- neue sinkTable ----------------
            DataTable sinkTable = new DataTable(); 
            DataTable logTable = new importLogTable(); 
            int rownum=1;   
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
                    mylog.LogInformation("rownum: " + rownum);
                    rownum=rownum+1;                       
                } 

            if (rownum==Int16.Parse(header_row))
                {
                    //mylog.LogInformation("Header aus der Datenquelle wird gelesen");                 
                     
                    
                    string[] header_source = parser.ReadFields();
                    string[] header_target = columns.Split('|'); 
                    //mylog.LogInformation("rownum: " + rownum);
                    rownum=rownum+1;

                     //default column date_id and kw:
                     int colnum=0;
                     ColumnPosition.Add(new SelectedColumn("date_id", -1, colnum));
                     colnum=colnum+1;
                     
                     //ColumnPosition.Add(new SelectedColumn("kw", -1, colnum));
                     //colnum=colnum+1;                     
                   
                for (int i = 0; i < header_target.Length; i++)    
                    {        
                        bool check_source=false;
                                                
                            for (int j = 0; j < header_source.Length; j++)
                            {  
                            if (header_source[j].ToLower()==header_target[i].ToLower())
                                {                               
                                ColumnPosition.Add(new SelectedColumn(header_target[i], j, colnum));
                                colnum=colnum+1;
                                check_source=true;
                                break;
                                }
                            }
                        if (check_source==false)  
                        {
                            ColumnPosition.Add(new SelectedColumn(header_target[i], -1, colnum));
                            colnum=colnum+1;
                        }                         
                    }    
                }       
               
                foreach (SelectedColumn col in ColumnPosition)
                {
                    //sinkTable.Columns.Add(col.Name.Replace(" ","_") );
                    if (customer.convertCustomerString(filepath) == "asd" || customer.convertCustomerString(filepath) == "asm" || customer.convertCustomerString(filepath) == "asw")
                    {
                        if(col.Name != "date_id")
                        {
                            col.Name = ConvertHeader.Header2Convert(col.Name);
                        }
                    }
                    sinkTable.Columns.Add(Regex.Replace(col.Name.ToLower(), @"[^va-zA-Z0-9äöü_\s]+-","").Replace(" ","_").Replace(" ","_").Trim().Replace("ä", "ae").Replace("ü", "ue").Replace("ö", "oe"));                    
                }
     
                while (!parser.EndOfData)
                        {
                            DataRow newDataRow = sinkTable.NewRow();
                            string[] fields = parser.ReadFields();
                            foreach (SelectedColumn col in ColumnPosition)
                            {
                                if (col.Name=="date_id")
                                {
                                    newDataRow[col.Colnum]=Datum_id;        
                                } //else if (col.Name=="kw")
                                //{
                                //    newDataRow[col.Colnum]=calendarWeek;        
                                //}                                
                                else
                                //geändert auf position>=0, da sonst Probleme auftreten, wo alle Spalten genommen werden, die bei Position 0 beginnen; scheint keine Auswirkungen auf amazon zu haben - 30.06.2023, tho18
                                //date_id hat immer die Position -1
                                if (col.Position>=0)
                                { 
                                    newDataRow[col.Colnum]=(fields[col.Position].Length==0) ? DBNull.Value: convertCurrency.convertEuroString(fields[col.Position]);
                                }                                
                                else {                             
                                {
                                    newDataRow[col.Colnum]="";
                                }
                                }
                          //  log.LogInformation("parsed field: " + fields[col.Position]);
                            }
                            sinkTable.Rows.Add(newDataRow);
                            rownum=rownum+1;
                        }   
                     DataRow logDataRow = logTable.NewRow();
                     //logDataRow[0]=blobItem.Name.Replace(filepath + source_filename,"").Substring(0,8) ; 
                     logDataRow[0]=Datum_id ;                                              
                     //logDataRow[1]=calendarWeek;
                     logDataRow[1]=rownum;   
                     logDataRow[2]=stream.Length;                                         
                     logTable.Rows.Add(logDataRow);  
                        
                     parser.Close();                      
                }  

                 // --------------- write sink file ------------------------------------------------------               
                string sinkFile = "";
                if(sink_filename_withDate.ToLower() == "j")
                    if (customer.convertCustomerString(filepath) == "asd")
                    {
                        sinkFile=filepath  + Datum_id.Substring(0,4) +"/" + Datum_id.Substring(4,2) +"/"  + sink_filename  + Datum + "_" + "kw_" + kw + ".parquet";                      
                    } else if (customer.convertCustomerString(filepath) == "asw")  {
                        sinkFile=filepath  + Datum_id.Substring(0,4) +"/" + Datum_id.Substring(4,2) +"/"  + sink_filename +  "kw_" + Datum.Substring(0,4) + kw  + ".parquet";                 
                    }  else if (customer.convertCustomerString(filepath) == "asm")  {
                        sinkFile=filepath  + Datum_id.Substring(0,4) +"/" + Datum_id.Substring(4,2) +"/"  + sink_filename + Datum.Substring(0,6)  + ".parquet";                 
                    } else if (customer.convertCustomerString(filepath) == "veb" || customer.convertCustomerString(filepath) == "vke" || customer.convertCustomerString(filepath) == "vpr")  {
                        string[] filePathVearsa = blobItem.Name.Split('-');
                        int filePathVearsaCount = filePathVearsa.Count();
                        if (filePathVearsaCount == 2)
                        {
                            sinkFile=filepath  + Datum_id.Substring(0,4) +"/" + Datum_id.Substring(4,2) +"/"  + Datum.Substring(0,8) + "-" +  filePathVearsa[1].Remove(filePathVearsa[1].Length - 4) + ".parquet";                                         
                        } else {
                            sinkFile=filepath  + Datum_id.Substring(0,4) +"/" + Datum_id.Substring(4,2) +"/"  + Datum.Substring(0,8) + "-" +  filePathVearsa[2].Remove(filePathVearsa[2].Length - 4) + ".parquet";                 
                        }
                    }  else {
                        sinkFile=filepath  + Datum_id.Substring(0,4) +"/" + Datum_id.Substring(4,2) +"/"  + sink_filename + Datum_id  + ".parquet";                 
                    }  else {
                        string[] filePathWithoutDate = blobItem.Name.Split('.');
                            sinkFile=filePathWithoutDate[0]+ "_" + Datum_id + ".parquet";                 
                    }

                BlobClient sinkBlobClient= adls_sink._containerClient.GetBlobClient(sinkFile);   
                var parquetClient = new ParquetClient(mylog);    
                parquetClient.writeToParquet(sinkTable, sinkFile, sinkBlobClient);                       
                mylog.LogInformation("Blob transformed to parquet from source file  " + filepath + source_filename + " from " + sourceContainer +  " to container " + sinkContainer + " as " + sinkFile); 

                 // --------------- write log file ------------------------------------------------------   
                string logFile=sinkFile; 
                BlobClient logBlobClient= adls_log._containerClient.GetBlobClient(sinkFile);   
                var logParquetClient = new ParquetClient(mylog);    
                logParquetClient.writeToParquet(logTable, logFile, logBlobClient);             

                ColumnPosition.Clear();
                sinkTable.Reset();
                copyCount=copyCount+1;
                String blobtime=(DateTime.Now-blobstart).TotalSeconds.ToString();
                
              
            }
        }   // if (blobItem.Name.Contains("_wait_for_cleaning")  )                                                 
        } // await foreach (BlobItem blobItem 
            string infostr="filesWritten:" + copyCount.ToString()  ;          
            return  "blab"; //infostr;        
        }  //public static async

        

        [FunctionName("durable_ingest_orchestrator_HttpStart")]
        public static async Task<HttpResponseMessage> HttpStart(
            [HttpTrigger(AuthorizationLevel.Anonymous, "get", "post")] HttpRequestMessage req,
            [DurableClient] IDurableOrchestrationClient starter,
            ILogger log)
        {
            // Function input comes from the request content.
            string instanceId = await starter.StartNewAsync("durable_ingest_orchestrator",  null,  await req.Content.ReadAsStringAsync());

            log.LogInformation("Started orchestration with ID = '{instanceId}'.", instanceId);

            return starter.CreateCheckStatusResponse(req, instanceId);
        }
    }
}