using System.Data;

namespace azap.util
{
public class importLogTable : DataTable
{
    public importLogTable()
    {
            Columns.Add("date_id",typeof(string));
            //Columns.Add("kw",typeof(int));            
            Columns.Add("rows",typeof(string));
            Columns.Add("filesize",typeof(string));            
            }
    }
   
}


