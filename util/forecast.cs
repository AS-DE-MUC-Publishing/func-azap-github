using System.Data;

namespace azap.util
{
public class amazon_forecastTable : DataTable
{
    public amazon_forecastTable()
    {
            Columns.Add("ASIN",typeof(string));
            Columns.Add("EAN",typeof(string));
            Columns.Add("date_id",typeof(string));
            Columns.Add("Woche",typeof(string));           
            Columns.Add("Forecast",typeof(string));
            }
    }
   
}


