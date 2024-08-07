using Microsoft.Extensions.Logging;
using Newtonsoft.Json;
using Azure.Storage.Blobs;
using Azure.Storage.Blobs.Models;
using System.Text;
using System.Data;
using Microsoft.VisualBasic.FileIO;
using System.Linq;
using azap.util;
using System.Text.RegularExpressions;
using System.Globalization;
using Microsoft.Azure.Functions.Worker;
using Microsoft.Azure.Functions.Worker.Http;
using Microsoft.DurableTask;
using Microsoft.DurableTask.Client;


namespace azap
{
    public class ingest_columns
    {   
        private readonly ILogger<ingest_columns> _logger;        

        public ingest_columns(ILogger<ingest_columns> logger)
        {
            _logger = logger;
        }
//--------------------------------------------------------------------------------------------------------------------------------------
//-------------------H T T P - T R I G G E R  ------------------------------------------------------------------------------------------
//--------------------------------------------------------------------------------------------------------------------------------------
        
        [Function("ingestcolumns")]
        public static async Task<HttpResponseData> HttpStart(
            [HttpTrigger(AuthorizationLevel.Function, "get", "post")] Microsoft.Azure.Functions.Worker.Http.HttpRequestData req,
            [DurableClient] DurableTaskClient client,
            FunctionContext executionContext)
        {
            ILogger logger = executionContext.GetLogger("ingestcolumns_HttpStart");

            // Function input comes from the request content.
            string instanceId = await client.ScheduleNewOrchestrationInstanceAsync(nameof(ingest_columns), await req.ReadAsStringAsync(),null);

            logger.LogInformation("Started orchestration with ID = '{instanceId}'.", instanceId);

            return await client.CreateCheckStatusResponseAsync(req, instanceId);
        }

//--------------------------------------------------------------------------------------------------------------------------------------
//-------------------O R C H E S T R A T O R - D U R A B L E   T A S K  ----------------------------------------------------------------
//--------------------------------------------------------------------------------------------------------------------------------------
        // expects a json string with the following properties:
        // {"function":"value" ... other key-value pairs ...} ... value can be currently "opus" or "fias"
        
        [Function(nameof(ingest_columns))]
        public static async Task<List<string>> RunOrchestrator(
            [OrchestrationTrigger] TaskOrchestrationContext context)
        {
            ILogger logger = context.CreateReplaySafeLogger(nameof(copy_between_storageaccounts));
            var outputs = new List<string>();
            // Deserialize the input data

            string input = context.GetInput<string>();
            byte[] inputBytes = Encoding.UTF8.GetBytes(input);
            dynamic data = JsonConvert.DeserializeObject(input);
            // Check the value of the "function" property
            string function = data?.function;
            using (var stream = new MemoryStream(inputBytes))
            using (var reader = new StreamReader(stream))
            {
                string request = await reader.ReadToEndAsync();
                logger.LogInformation("requestbody read");
                outputs.Add(await context.CallActivityAsync<string>("ingestcolumns-activity", request));
                                      
            }            
            return outputs;
        }
        
//--------------------------------------------------------------------------------------------------------------------------------------
//-------------------ingestcolumns FUNCTION  ---------------------------------------------------------------------------------------------------
//--------------------------------------------------------------------------------------------------------------------------------------
 
        [Function("ingestcolumns-activity")]
          public static async Task<string>ingestcolumnsActivity([ActivityTrigger]  string request, FunctionContext executionContext)
        {
            ILogger logger = executionContext.GetLogger("ingestcolumns-activity");
            logger.LogInformation("ingestcolumns called");
           
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
            string sink_filename_withDate=data?.sink_filename_withDate ?? "j";            
            string days_lastmodified =data?.days_lastmodified;   
            string source_suffix_switch="";
            // string test=data?.test ?? "false";
            // string filepath_sink=(test=="true") ? filepath  + "test/": filepath;

            //-----------------------  Variables -----------------------------------         
            int days=Int16.Parse(days_lastmodified); 
            int copyCount=0;
            List<SelectedColumn> ColumnPosition = new List<SelectedColumn>();   
            int int_header_row=Int16.Parse(header_row);
            string resultstring="";
            DateTime lastmodifed=DateTime.Now.Subtract(TimeSpan.FromDays(days));
            logger.LogInformation("Parameter from requestbody read");    

            try {
           
           // ---------------------- Storage Account --------------------            
            var adls_source = new DatalakeClient( storageAccount, sourceContainer);
            var adls_sink = new DatalakeClient( storageAccount, sinkContainer);
            var adls_log = new DatalakeClient( storageAccount, "importlogs");                      

        // ---------------------- Reading from Source --------------------
        await foreach (BlobItem blobItem in adls_source._containerClient.GetBlobsAsync(BlobTraits.None, BlobStates.None, filepath))   
           {   
              
        if (blobItem.Name.Contains(source_filename) && blobItem.Name.EndsWith(source_suffix) && lastmodifed<blobItem.Properties.LastModified)         
        {            
            
            logger.LogInformation("Blob found: " + blobItem.Name);
            String Datum_id=ConvertDate.Date2ConvertWithPattern(blobItem.Name.Replace(filepath + source_filename,""), "[0-9]", sink_filename_withDate.ToLower());
            String Datum=Datum_id;


            logger.LogInformation("Filename und Datum_id (Date2ConvertWithPattern): " + filepath + source_filename + " datum_id " + Datum_id);    

            // --------- actual culture -------------------
           CultureInfo myCI = new CultureInfo("de-DE");
           Calendar myCal = myCI.Calendar;
           CalendarWeekRule cwr = myCI.DateTimeFormat.CalendarWeekRule;
           DayOfWeek firstDow = myCI.DateTimeFormat.FirstDayOfWeek;

            // --------- actual calendar -------------------
            string[] validformats = new[] { "MM/dd/yyyy", "yyyy/MM/dd", "dd.MM.yyyy", "yyyyMMdd" };

            //string test = Datum_id.Substring(4,2) + "/" + Datum_id.Substring(6,2) + "/" + Datum_id.Substring(0,4);
            DateTime kwDatum = DateTime.ParseExact(Datum, validformats, myCI);

            //-----AbfrageDatum auf Sonntag, dann Montag ---------
            DateTime dateManipulated = DateTime.ParseExact(Datum, validformats, myCI);
                // Display the DayOfWeek string representation
            Console.WriteLine(dateManipulated.DayOfWeek.ToString());
            if ((dateManipulated.DayOfWeek.ToString() == "Sonntag" || dateManipulated.DayOfWeek.ToString() == "Sunday") && filepath == "amazon/search/week/")
            {
                kwDatum = kwDatum.AddDays(1); 
                string dateManipulatedDayOffset = kwDatum.ToString("yyyyMMdd");               
                Datum_id = dateManipulatedDayOffset;       
                Datum = dateManipulatedDayOffset;                      
                //Console.WriteLine(Datum_id);
            }
       
            int calendarWeek = myCal.GetWeekOfYear(kwDatum, cwr, firstDow); 
            string kw = "";

            if(calendarWeek.ToString().Length == 1)           
            {
                kw = "0" + calendarWeek.ToString();
            } else {
                kw = calendarWeek.ToString();
            }

            logger.LogInformation("blob to be processed: " + blobItem.Name);
            BlobClient sourceBlob=adls_source._containerClient.GetBlobClient(blobItem.Name);
            var stream = await sourceBlob.OpenReadAsync();

           // ------------- neue sinkTable ----------------
            DataTable sinkTable = new DataTable(); 
            DataTable logTable = new importLogTable(); 
            int rownum=1;   
            DateTime blobstart=DateTime.Now;    
            string sinkFile =getSinkFileName(data, Datum_id, Datum, kw, blobItem.Name);   

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
                    
                    
                    string[] header_source = parser.ReadFields();
                    string[] header_target = columns.Split('|'); 
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
                            string header_source_clean = Regex.Replace(header_source[j], @"\p{C}+", string.Empty);
                            header_source_clean = Regex.Replace(header_source_clean, @"\s+", string.Empty); // Remove white spaces
                            string header_target_clean = Regex.Replace(header_target[i], @"\p{C}+", string.Empty);
                            header_target_clean = Regex.Replace(header_target_clean, @"\s+", string.Empty); // Remove white spaces

                            if (header_target_clean.ToLower()==header_source_clean.ToLower())
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
                    if (convertCustomerString(filepath) == "asd" || convertCustomerString(filepath) == "asm" || convertCustomerString(filepath) == "asw")
                    {
                        if(col.Name != "date_id")
                        {
                            
                            col.Name = Header2Convert(col.Name);
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
                                }                               
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

               

                BlobClient sinkBlobClient= adls_sink._containerClient.GetBlobClient(sinkFile);   
                var parquetClient = new ParquetClient(logger);    
                logger.LogInformation(await parquetClient.WriteDataTableToParquet(sinkTable, sinkFile, sinkBlobClient));
                logger.LogInformation("Blob transformed to parquet from source file  " + filepath + source_filename + " from " + sourceContainer +  " to container " + sinkContainer + " as " + sinkFile); 

                 // --------------- write log file ------------------------------------------------------   
                string logFile=sinkFile; 
                BlobClient logBlobClient= adls_log._containerClient.GetBlobClient(sinkFile);   
                var logParquetClient = new ParquetClient(logger);    
                logger.LogInformation(await logParquetClient.WriteDataTableToParquet(logTable, logFile, logBlobClient, false));             

                ColumnPosition.Clear();
                sinkTable.Reset();
                copyCount=copyCount+1;
                String blobtime=(DateTime.Now-blobstart).TotalSeconds.ToString();

                if (sourceContainer=="temp-stage") {

                    // BlobClient sourceBlobClient= adls_source._containerClient.GetBlobClient(blobItem.Name);   
                    await sourceBlob.DeleteIfExistsAsync(); 
                    logger.LogInformation("Blob deleted from source file  " + filepath + source_filename + " from " + sourceContainer );
                }
                
              
            
            }   // if (blobItem.Name.Contains("_wait_for_cleaning")  )                                                 
        } // await foreach (BlobItem blobItem 
            
        string infostr="filesWritten:" + copyCount.ToString()  ;  
        resultstring="success"; 
        return resultstring;              
        } // try
        catch  (Exception ex)
        {
                    resultstring="error: " + ex.Message;
                    return resultstring;
        }  // catch           
        }  //public static async 

        public static string Header2Convert(string header_source)
      {
         string retVal = string.Empty;
         header_source = Regex.Replace(header_source, @"\p{C}+", string.Empty);
         header_source = Regex.Replace(header_source, @"\s+", string.Empty); // Remove white spaces

         if (header_source == "DatumderBerichterstattung")
         {
            retVal = "report_date";
         }
         else if (header_source == "Suchfrequenz-Rang")
         {
            retVal = "search_rank";
         }
         else if (header_source == "Suchbegriff")
         {
            retVal = "search_term";
         }
         else if (header_source == "MeistgeklickteKategorie#1")
         {
            retVal = "category_1";
         }
         else if (header_source == "MeistgeklickteKategorie#2")
         {
            retVal = "category_2";
         }
         else if (header_source == "MeistgeklickteKategorie#3")
         {
            retVal = "category_3";
         }
         else if (header_source == "AmhäufigstenaufgerufenesProduktNr.1:ASIN")
         {
            retVal = "product_asin_1";
         }
         else if (header_source == "AmhäufigstenaufgerufenesProduktNr.2:ASIN")
         {
            retVal = "product_asin_2";
         }
         else if (header_source == "AmhäufigstenaufgerufenesProduktNr.3:ASIN")
         {
            retVal = "product_asin_3";
         }
         else if (header_source == "AmhäufigstenaufgerufenesProduktNr.1:Produktbezeichnung")
         {
            retVal = "product_name_1";
         }
         else if (header_source == "AmhäufigstenaufgerufenesProduktNr.2:Produktbezeichnung")
         {
            retVal = "product_name_2";
         }
         else if (header_source == "AmhäufigstenaufgerufenesProduktNr.3:Produktbezeichnung")
         {
            retVal = "product_name_3";
         }

         return retVal;
      }  
    
    //--------------------------------------------------------------------------------------------------------------------------------------
    //------------------- set sink file for the ingestcolumn function with specific folder/filenames for amazon/search and vearsa)  --------
    //--------------------------------------------------------------------------------------------------------------------------------------
        public static string getSinkFileName(dynamic data, string Datum_id, string Datum, string kw, string sourceFileName)
        {

             string filepath = data?.filepath + "/";  
             string sink_filename_withDate=data?.sink_filename_withDate ?? "j";   
             string sink_filename=data?.sink_filename;         

             string customerPath = convertCustomerString(filepath);

             string test=data?.test ?? "false";
             filepath=(test=="true") ? filepath  + "test/": filepath;
             string sinkFile="";
            
            if(sink_filename_withDate.ToLower() == "j")
            {
                    // amazon/search/day
                    if (customerPath == "asd")
                    {
                        sinkFile=filepath  + Datum_id.Substring(0,4) +"/" + Datum_id.Substring(4,2) +"/"  + sink_filename  + Datum + "_" + "kw_" + kw + ".parquet";                      
                    } 
                    // amazon/search/week
                    else if (customerPath == "asw")  {
                        sinkFile=filepath  + Datum_id.Substring(0,4) +"/" + Datum_id.Substring(4,2) +"/"  + sink_filename +  "kw_" + Datum.Substring(0,4) + kw  + ".parquet";                 
                    }  
                    // amazon/search/month
                    else if (customerPath == "asm")  {
                        sinkFile=filepath  + Datum_id.Substring(0,4) +"/" + Datum_id.Substring(4,2) +"/"  + sink_filename + Datum.Substring(0,6)  + ".parquet";                 
                    } 
                    // all vearsa formats
                    else if (customerPath == "veb" || customerPath == "vke" || customerPath == "vpr")  {
                        string[] filePathVearsa = sourceFileName.Split('-');
                        int filePathVearsaCount = filePathVearsa.Count();
                        if (filePathVearsaCount == 2)
                        {
                            sinkFile=filepath  + Datum_id.Substring(0,4) +"/" + Datum_id.Substring(4,2) +"/"  + Datum.Substring(0,8) + "-" +  filePathVearsa[1].Remove(filePathVearsa[1].Length - 4) + ".parquet";                                         
                        } else {
                            sinkFile=filepath  + Datum_id.Substring(0,4) +"/" + Datum_id.Substring(4,2) +"/"  + Datum.Substring(0,8) + "-" +  filePathVearsa[2].Remove(filePathVearsa[2].Length - 4) + ".parquet";                 
                        }
                    }  
                    // no specific format
                    else {
                        sinkFile=filepath  + Datum_id.Substring(0,4) +"/" + Datum_id.Substring(4,2) +"/"  + sink_filename + Datum_id  + ".parquet";                 
                    }  
                // no "yyyy/mm" folder
                }    
                else {
                        string[] filePathWithoutDate = sourceFileName.Split('.');
                            sinkFile=filePathWithoutDate[0]+ "_" + Datum_id + ".parquet";                 
                }
            
            
             return sinkFile;
        }

    //--------------------------------------------------------------------------------------------------------------------------------------
    //------------------- get source pattern  from filepath  --------------------------------------------------------------------------------
    //--------------------------------------------------------------------------------------------------------------------------------------
      public static string convertCustomerString(string customerPathvalue)
        {
            string[,] returnvalue = new string[,] { {"amazon/search/day/", "asd"}, {"amazon/search/week/", "asw"} , {"amazon/search/month/", "asm"}, {"vearsa/ebook/", "veb"}, {"vearsa/keywords/", "vke"}, {"vearsa/print/", "vpr"}}; 
                 //      string returnCustomerIndex = "";
            for (int i = 0; i < returnvalue.GetLength(0); i++) 
            {
                if(Regex.IsMatch(customerPathvalue, returnvalue[i,0]))
                {
                    return returnvalue[i,1].ToString();
                //    break;
                }
            }   return customerPathvalue;
        } 
    }
}