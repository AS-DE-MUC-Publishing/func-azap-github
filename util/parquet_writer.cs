using System.Data;
using DataColumn = System.Data.DataColumn;
using Microsoft.Extensions.Logging;
using System.Collections;
using Parquet;
using Azure.Storage.Blobs;
using Microsoft.Extensions.Configuration;
using Parquet.Schema;
using CsvHelper.TypeConversion;

namespace azap.util
{
    public class ParquetClient 
    {
        private readonly ILogger _logger;
    
        public ParquetClient(ILogger logger)
        {
            _logger = logger;
        }
    

    public static List<DataField> GenerateSchemaFromDataTable(DataTable dt, bool stringonly)
    {
        var fields = new List<DataField>();

         foreach (DataColumn column in dt.Columns)
        {
            // Match the type of column to a parquet data type
            DataField field;
            if(stringonly)
            {
                field = new DataField<string>(column.ColumnName);
            }
            else if (column.DataType == typeof(bool))
            {
                field = new DataField<bool>(column.ColumnName);
            }
            else if (column.DataType == typeof(decimal))
            {
                field = new DataField<decimal>(column.ColumnName);
            }
            else if (column.DataType == typeof(int))
            {
                field = new DataField<int>(column.ColumnName);
            }
             else if (column.DataType == typeof(Int32))
            {
                field = new DataField<Int32>(column.ColumnName);
            }
            else if (column.DataType == typeof(long))
            {
                field = new DataField<long>(column.ColumnName);
            }
            else if (column.DataType == typeof(float))
            {
                field = new DataField<float>(column.ColumnName);
            }
            else if (column.DataType == typeof(double))
            {
                field = new DataField<double>(column.ColumnName);
            }
            else if (column.DataType == typeof(DateTime))
            {
                field = new DataField<DateTimeOffset>(column.ColumnName);
            }
            else
            {
                field = new DataField<string>(column.ColumnName);
            }

            fields.Add(field);
        }
        return fields;
    } 
    private static SemaphoreSlim semaphore = new SemaphoreSlim(4);
    public async Task<DataTable> ParquetToDataTable(BlobClient sourceBlobClient)
    {
        DataTable dataTable = new DataTable();
        try
        {
            // Download the parquet file from the blob storage
            using (MemoryStream fileStream = new MemoryStream())
            {
                await sourceBlobClient.DownloadToAsync(fileStream);
                fileStream.Position = 0;

                // Read the parquet file into a DataTable
                using (var parquetReader = await ParquetReader.CreateAsync(fileStream))
                {
                  
                    ParquetSchema schema = parquetReader.Schema;
                    foreach (var field in schema.Fields)
                    {
                         dataTable.Columns.Add(new DataColumn(field.Name, typeof(string)));
                    }

                    for (int i = 0; i < parquetReader.RowGroupCount; i++)
                    {
                        using (ParquetRowGroupReader groupReader = parquetReader.OpenRowGroupReader(i))
                        {
                            // Initialize a list to hold rows of data temporarily
                            List<object[]> rows = new List<object[]>();

                            foreach (DataField field in schema.GetDataFields())
                            {
                                Parquet.Data.DataColumn column = await groupReader.ReadColumnAsync(field);
                                // Resize the rows list to match the number of rows in the column
                                while (rows.Count < column.Data.Length)
                                {
                                    rows.Add(new object[schema.Fields.Count]);
                                }
                                // Populate the data for this column in each row
                                for (int rowIndex = 0; rowIndex < column.Data.Length; rowIndex++)
                                {
                                    rows[rowIndex][dataTable.Columns[field.Name].Ordinal] = column.Data.GetValue(rowIndex);
                                }                           

                                // // Console.WriteLine($"Column Data of '{field.Name}':");
                                // foreach (var value in column.Data)
                                // {
                                //     Console.WriteLine(value);
                                // }
                            }
                            foreach (var row in rows)
                        {
                            dataTable.Rows.Add(row);
                        }

                            
                        }
                    }

                }
            }
                    
        }
        catch (Exception ex)
        {
            _logger.LogError($"Could not read Parquet file: {ex.Message}, {ex.InnerException}");
            return null;
        }
         return dataTable;
    }
    public async Task<string> WriteDataTableToParquet(DataTable sourceTable, string sinkFile, BlobClient sinkBlobclient, bool stringonly = true) {

        try
        {
        // Wait for a slot in the semaphore
        await semaphore.WaitAsync();

        var logfields = GenerateSchemaFromDataTable(sourceTable, stringonly);
        var logschema = new ParquetSchema(logfields);  

        _logger.LogInformation($"Start writing Parquet file to '{sinkFile}'");

            using (MemoryStream fileStream = new MemoryStream())
            {
             try {
                using (var writer = await ParquetWriter.CreateAsync(logschema, fileStream))
                {
                    var startRow = 0;
                 
                        using (var rgw = writer.CreateRowGroup())
                        {
                            // Data is written to the row group column by column
                            for (var i = 0; i < sourceTable.Columns.Count; i++)
                            {
                                var columnIndex = i;
                                var targetType=typeof(string);

                                if (!stringonly)
                                {
                                    // Determine the target data type for the column
                                    targetType = sourceTable.Columns[columnIndex].DataType;
                                    if (targetType == typeof(DateTime)) targetType = typeof(DateTimeOffset);
                                }

                                // Generate the value type, this is to ensure it can handle null values
                                var valueType = targetType;
                                    // var valueType = targetType.IsClass
                                    // ? targetType
                                    // : typeof(Nullable<>).MakeGenericType(targetType);

                                // Create a list to hold values of the required type for the column
                                var list = (IList)typeof(List<>)
                                    .MakeGenericType(valueType)
                                    .GetConstructor(Type.EmptyTypes)
                                    .Invoke(null);

                                // Get the data to be written to the parquet stream
                                foreach (var row in sourceTable.AsEnumerable().Skip(startRow))
                                {
                                    // Check if value is null, if so then add a null value
                                    if (row[columnIndex] == null || row[columnIndex] == DBNull.Value)
                                    {
                                        list.Add(null);
                                    }
                                    else
                                    {
                                        // Add the value to the list, but if it's a DateTime then create it as a DateTimeOffset first
                                        list.Add(sourceTable.Columns[columnIndex].DataType == typeof(DateTime)
                                            ? new DateTimeOffset((DateTime) row[columnIndex])
                                            : row[columnIndex]);
                                    }
                                }

                                // Copy the list values to an array of the same type as the WriteColumn method expects
                                // and Array
                                var valuesArray = Array.CreateInstance(valueType, list.Count);
                                list.CopyTo(valuesArray, 0);

                                // Write the column
                                await rgw.WriteColumnAsync(new Parquet.Data.DataColumn(logfields[i], valuesArray));
                            }
                        }
                        sourceTable.Dispose();
                }
                fileStream.Position = 0;
                await sinkBlobclient.UploadAsync(fileStream, overwrite: true);
                fileStream.Close();
                return $"Finished Parquet file to '{sinkFile}'";
                 } catch(Exception ex) {
                return $"Could not write file {sinkFile} to Parquet: {ex.Message}, {ex.InnerException}";
                }
            }   
             }
    finally
    {
        // Release the slot in the semaphore
        semaphore.Release();
    }
        } 
        
    }
}






// using System.Data;
// using System.Collections.Generic;
// using System;
// using System.Threading.Tasks;
// using DataColumn = System.Data.DataColumn;
// using Microsoft.Extensions.Logging;
// using System.IO;
// using System.Collections;
// using System.Linq;
// using Parquet;
// using Parquet.Data;
// using Azure.Storage.Blobs;
// using Microsoft.Extensions.Configuration;

// namespace azap.util
// {
  
//     public class ParquetClient
//     {
//         private readonly ILogger _log = null;

//         public ParquetClient(ILogger logextract) {
//             this._log = logextract;
//         }    


//         private readonly IConfiguration _configuration;

//         public ParquetClient(IConfiguration configuration)
//         {
//             _configuration = configuration;
//         }
 

//         public static List<DataField> GenerateSchema(DataTable dt)
//         {
//             var fields = new List<DataField>(dt.Columns.Count);

//             foreach (DataColumn column in dt.Columns)
//             {
//                 // Attempt to parse the type of column to a parquet data type
//                 var success = Enum.TryParse<DataType>(column.DataType.Name, true, out var type);

//                 // If the parse was not successful and it's source is a DateTime then use a DateTimeOffset, otherwise default to a string
//                 if (!success && column.DataType == typeof(DateTime))
//                 {
//                     type = DataType.DateTimeOffset;
//                 }
//                 else if (!success)
//                 {
//                     type = DataType.String;
//                 }

//                 fields.Add(new DataField(column.ColumnName, type));
//             }

//             return fields;
//         } 

//         public async void writeToParquet(DataTable sinkTable, string sinkFile, BlobClient sinkBlobClient) {

//         var sinkFields = util.ParquetClient.GenerateSchema(sinkTable);
//         var sinkSchema = new Schema(sinkFields);        

//         sinkBlobClient.DeleteIfExists();

//         _log.LogInformation($"Start writing Parquet file to '{sinkFile}'");

//             using (MemoryStream fileStream = new MemoryStream())
//             {
//              try {
//                 using (var writer = await ParquetWriter.CreateAsync(sinkSchema, fileStream))
//                 {
//                     var startRow = 0;
                 
//                         using (var rgw = writer.CreateRowGroup())
//                         {
//                             // Data is written to the row group column by column
//                             for (var i = 0; i < sinkTable.Columns.Count; i++)
//                             {
//                                 var columnIndex = i;

//                                 // Determine the target data type for the column
//                                 var targetType = sinkTable.Columns[columnIndex].DataType;
//                                 if (targetType == typeof(DateTime)) targetType = typeof(DateTimeOffset);

//                                 // Generate the value type, this is to ensure it can handle null values
//                                 var valueType = targetType.IsClass
//                                     ? targetType
//                                     : typeof(Nullable<>).MakeGenericType(targetType);

//                                 // Create a list to hold values of the required type for the column
//                                 var list = (IList)typeof(List<>)
//                                     .MakeGenericType(valueType)
//                                     .GetConstructor(Type.EmptyTypes)
//                                     .Invoke(null);

//                                 // Get the data to be written to the parquet stream
//                                 foreach (var row in sinkTable.AsEnumerable().Skip(startRow))
//                                 {
//                                     // Check if value is null, if so then add a null value
//                                     if (row[columnIndex] == null || row[columnIndex] == DBNull.Value)
//                                     {
//                                         list.Add(null);
//                                     }
//                                     else
//                                     {
//                                         // Add the value to the list, but if it's a DateTime then create it as a DateTimeOffset first
//                                         list.Add(sinkTable.Columns[columnIndex].DataType == typeof(DateTime)
//                                             ? new DateTimeOffset((DateTime) row[columnIndex])
//                                             : row[columnIndex]);
//                                     }
//                                 }

//                                 // Copy the list values to an array of the same type as the WriteColumn method expects
//                                 // and Array
//                                 var valuesArray = Array.CreateInstance(valueType, list.Count);
//                                 list.CopyTo(valuesArray, 0);

//                                 // Write the column
//                                 await rgw.WriteColumnAsync(new Parquet.Data.DataColumn(sinkFields[i], valuesArray));
//                             }
//                         }
//                 }
//                 fileStream.Position = 0;
//                 sinkBlobClient.Upload(fileStream, overwrite: true);
//                  } catch(Exception ex) {
//                     _log.LogError($"Could not write file {sinkFile} to Parquet: {ex.Message}, {ex.InnerException}");
//                 }

//             }        


//         } 
//     }

// }


