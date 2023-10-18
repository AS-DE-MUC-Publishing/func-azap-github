using System.Text.RegularExpressions;

namespace azap.util{       

        public class customer
    {
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