using System.Text.RegularExpressions;

namespace azap.util{       

        public class convertCurrency
    {
        public static string convertEuroString(string fieldvalue)
        {
            string returnvalue=fieldvalue;
            Regex r = new Regex(@"^[\d\.]*\,[0-9]{2}.€$");           
               
            if (r.IsMatch(fieldvalue))
            {
                returnvalue=fieldvalue.Substring(0,fieldvalue.Length-2).Replace(".","").Replace(",","."); 
            } 
            return returnvalue;
        }
    }
}