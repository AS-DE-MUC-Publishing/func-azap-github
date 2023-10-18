using System;
using System.IO;
using System.Web;
using Azure.Storage.Blobs;
using Microsoft.WindowsAzure.Storage;
using Microsoft.WindowsAzure.Storage.Blob;


//define your application namespace here
namespace azap.util
{
    public static class exceptionlogging
    {
            public static long GetFileSize(string connectionString, string containerName, string blobName)
            {
                try
                {
                    CloudStorageAccount storageAccount = CloudStorageAccount.Parse(connectionString.ToString());
                    CloudBlobClient blobClient = storageAccount.CreateCloudBlobClient();
                    CloudBlobContainer container = blobClient.GetContainerReference(containerName);
                    CloudBlockBlob blob = container.GetBlockBlobReference(blobName);
                    blob.FetchAttributesAsync();
                    return blob.Properties.Length;
                }
                catch (Exception ex)
                {
                    using (StreamWriter writer = new StreamWriter("log.txt", true))
                    {
                        writer.WriteLine($"Error occurred while getting file size. Container: {containerName}, Blob: {blobName}, Exception: {ex.Message}");
                    }
                    return -1;
                }
            }   

            public static void LogToFile(string logFilePath, string message)
            {
                using (StreamWriter writer = new StreamWriter(logFilePath, true))
                {
                    writer.WriteLine($"{DateTime.Now}: {message}");
                }
            }                
        private static string errorlineno, errormsg, extype, exurl,  errorlocation;

        public static void SendeErrorToText(Exception ex)
        {
            var line = Environment.NewLine + Environment.NewLine;

            errorlineno = ex.StackTrace.ToString();
            errormsg = ex.Message;
            extype = ex.GetType().ToString();
            exurl = exurl.ToString();
            errorlocation = ex.Message.ToString();
            try
            {
                //create a folder named as "exceptiontextfile" inside your application
                //text file path
                string filepath = "Test"; //exurl("~/exceptiontextfile/");  
                if (!Directory.Exists(filepath))
                {
                    Directory.CreateDirectory(filepath);
                }
                //text file name
                filepath = filepath + DateTime.Today.ToString("dd-mm-yy") + ".txt";   
                if (!File.Exists(filepath))
                {
                    File.Create(filepath).Dispose();
                }
                using (StreamWriter sw = File.AppendText(filepath))
                {
                    string error = "log written date:" + " " + DateTime.Now.ToString() 
                    + line + "error line no :" + " " + errorlineno + line 
                    + "error message:" + " " + errormsg + line + "exception type:" + " " 
                    + extype + line + "error location :" + " " + errorlocation + line 
                    + " error page url:" + " " + exurl + line + line;
                    sw.WriteLine("-----------exception details on " + " " + DateTime.Now.ToString() + "-----------------");
                    sw.WriteLine("-------------------------------------------------------------------------------------");
                    sw.WriteLine(line);
                    sw.WriteLine(error);
                    sw.WriteLine("--------------------------------*end*------------------------------------------");
                    sw.WriteLine(line);
                    sw.Flush();
                    sw.Close();
                }
            }
            catch (Exception e)
            {
                e.ToString();
            }
        }
        public static bool IsFileEmpty(string fileName)
        {
            var f = new FileInfo(fileName);
            return f.Length == 0 || f.Length < 10 && File.ReadAllText(fileName).Length == 0;
        }
    }
}