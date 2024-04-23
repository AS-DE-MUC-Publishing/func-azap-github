using System.Data;
using System.Collections.Generic;
using System;
using System.Threading.Tasks;
using DataColumn = System.Data.DataColumn;
using Microsoft.Extensions.Logging;
using System.IO;
using System.Collections;
using System.Linq;
using Parquet;
using Parquet.Data;
using Azure.Storage.Blobs;
using Microsoft.Extensions.Configuration;

namespace azap.util
{
  
    public class ParquetClient
    {
        private readonly ILogger _log = null;

        public ParquetClient(ILogger logextract) {
            this._log = logextract;
        }    


        private readonly IConfiguration _configuration;

        public ParquetClient(IConfiguration configuration)
        {
            _configuration = configuration;
        }
 

        public static List<DataField> GenerateSchema(DataTable dt)
        {
            var fields = new List<DataField>(dt.Columns.Count);

            foreach (DataColumn column in dt.Columns)
            {
                // Attempt to parse the type of column to a parquet data type
                var success = Enum.TryParse<DataType>(column.DataType.Name, true, out var type);

                // If the parse was not successful and it's source is a DateTime then use a DateTimeOffset, otherwise default to a string
                if (!success && column.DataType == typeof(DateTime))
                {
                    type = DataType.DateTimeOffset;
                }
                else if (!success)
                {
                    type = DataType.String;
                }

                fields.Add(new DataField(column.ColumnName, type));
            }

            return fields;
        } 

        public async void writeToParquet(DataTable sinkTable, string sinkFile, BlobClient sinkBlobClient) {

        var sinkFields = util.ParquetClient.GenerateSchema(sinkTable);
        var sinkSchema = new Schema(sinkFields);        

        sinkBlobClient.DeleteIfExists();

        _log.LogInformation($"Start writing Parquet file to '{sinkFile}'");

            using (MemoryStream fileStream = new MemoryStream())
            {
             try {
                using (var writer = await ParquetWriter.CreateAsync(sinkSchema, fileStream))
                {
                    var startRow = 0;
                 
                        using (var rgw = writer.CreateRowGroup())
                        {
                            // Data is written to the row group column by column
                            for (var i = 0; i < sinkTable.Columns.Count; i++)
                            {
                                var columnIndex = i;

                                // Determine the target data type for the column
                                var targetType = sinkTable.Columns[columnIndex].DataType;
                                if (targetType == typeof(DateTime)) targetType = typeof(DateTimeOffset);

                                // Generate the value type, this is to ensure it can handle null values
                                var valueType = targetType.IsClass
                                    ? targetType
                                    : typeof(Nullable<>).MakeGenericType(targetType);

                                // Create a list to hold values of the required type for the column
                                var list = (IList)typeof(List<>)
                                    .MakeGenericType(valueType)
                                    .GetConstructor(Type.EmptyTypes)
                                    .Invoke(null);

                                // Get the data to be written to the parquet stream
                                foreach (var row in sinkTable.AsEnumerable().Skip(startRow))
                                {
                                    // Check if value is null, if so then add a null value
                                    if (row[columnIndex] == null || row[columnIndex] == DBNull.Value)
                                    {
                                        list.Add(null);
                                    }
                                    else
                                    {
                                        // Add the value to the list, but if it's a DateTime then create it as a DateTimeOffset first
                                        list.Add(sinkTable.Columns[columnIndex].DataType == typeof(DateTime)
                                            ? new DateTimeOffset((DateTime) row[columnIndex])
                                            : row[columnIndex]);
                                    }
                                }

                                // Copy the list values to an array of the same type as the WriteColumn method expects
                                // and Array
                                var valuesArray = Array.CreateInstance(valueType, list.Count);
                                list.CopyTo(valuesArray, 0);

                                // Write the column
                                await rgw.WriteColumnAsync(new Parquet.Data.DataColumn(sinkFields[i], valuesArray));
                            }
                        }
                }
                fileStream.Position = 0;
                sinkBlobClient.Upload(fileStream, overwrite: true);
                 } catch(Exception ex) {
                    _log.LogError($"Could not write file {sinkFile} to Parquet: {ex.Message}, {ex.InnerException}");
                }

            }        


        } 
    }

}


