using System;
//using System.Collections.Generic;
//using System.Linq;
using System.Text.RegularExpressions;
using System.Globalization;
    
namespace azap.util
{
    public class ConvertDate
    {
        public static String Date2Convert(string fileWithSinkDate)
        {
            //Your code goes here
             string pattern = "[0-9]";
            Regex r = new Regex(pattern);
            //MatchCollection mc = r.Matches("dies ist ein Text vom 15/05/2023");
            MatchCollection mc = r.Matches(fileWithSinkDate);            
           

            string retVal = string.Empty;
            for (int i = 0; i < mc.Count; i++)
            { retVal += mc[i].Value; }
            if (retVal.Length > 10) { retVal = retVal.Substring(0,8).Replace("-", "").Replace(".", "").Replace("/", ""); }
            Console.WriteLine(retVal);
            
            DateTime d;

              bool chValidity;
            
              chValidity= DateTime.TryParseExact(retVal, "dd.MM.yyyy", CultureInfo.InvariantCulture, DateTimeStyles.None, out d);

              if (chValidity == true) 
              {
                 retVal = retVal.Substring(6, 4) + retVal.Substring(3, 2) + retVal.Substring(0, 2);       
              } 
            
              chValidity = DateTime.TryParseExact(retVal, "dd/MM/yyyy", CultureInfo.InvariantCulture, DateTimeStyles.None, out d);

              if (chValidity == true) 
              {
                 retVal = retVal.Substring(6, 4) + retVal.Substring(3, 2) + retVal.Substring(0, 2);           
              } 
            
             chValidity = DateTime.TryParseExact(retVal, "dd-MM-yyyy", CultureInfo.InvariantCulture, DateTimeStyles.None, out d);
             if (chValidity == true) 
              {
                 retVal = retVal.Substring(6, 4) + retVal.Substring(3, 2) + retVal.Substring(0, 2);              
              }  

             chValidity = DateTime.TryParseExact(retVal, "MM.dd.yyyy", CultureInfo.InvariantCulture, DateTimeStyles.None, out d);
             if (chValidity == true) 
              {
                 retVal = retVal.Substring(6, 4) + retVal.Substring(3, 2) + retVal.Substring(0, 2);              
              }  
            
            chValidity = DateTime.TryParseExact(retVal, "MM/dd/yyyy", CultureInfo.InvariantCulture, DateTimeStyles.None, out d);
             if (chValidity == true) 
              {
                 retVal = retVal.Substring(6, 4) + retVal.Substring(3, 2) + retVal.Substring(0, 2);              
              }  
            
             chValidity = DateTime.TryParseExact(retVal, "MM-dd-yyyy", CultureInfo.InvariantCulture, DateTimeStyles.None, out d);
             if (chValidity == true) 
              {
                 retVal = retVal.Substring(6, 4) + retVal.Substring(3, 2) + retVal.Substring(0, 2);              
              }              
 
             chValidity = DateTime.TryParseExact(retVal, "yyyyMMdd", CultureInfo.InvariantCulture, DateTimeStyles.None, out d);
             if (chValidity == true) 
              {
                 retVal = retVal.Substring(0,8);            
              } 
            
             chValidity = DateTime.TryParseExact(retVal, "yyyyMMddmmhh", CultureInfo.InvariantCulture, DateTimeStyles.None, out d);
             if (chValidity == true) 
              {
                 retVal = retVal.Substring(0,8);            
              }     
            
             chValidity = DateTime.TryParseExact(retVal.Substring(0, 8), "yyyy-MM-ddyyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out d);
             if (chValidity == true) 
              {
                 retVal = retVal.Substring(0,8);              
              }    
            
             chValidity = DateTime.TryParseExact(retVal.Substring(0, 8), "ddMMyyyy", CultureInfo.InvariantCulture, DateTimeStyles.None, out d);
             if (chValidity == true) 
              {
                 retVal = retVal.Substring(4, 4) + retVal.Substring(2, 2) + retVal.Substring(0, 2);              
              }      
            
             chValidity = DateTime.TryParseExact(retVal, "MMddyyyy", CultureInfo.InvariantCulture, DateTimeStyles.None, out d);
             if (chValidity == true) 
              {
                 retVal = retVal.Substring(4, 4) + retVal.Substring(0, 2) + retVal.Substring(2, 2);             
              }    

             chValidity = DateTime.TryParseExact(retVal.Substring(0,8), "yyyyMMdd", CultureInfo.InvariantCulture, DateTimeStyles.None, out d);
             if (chValidity == true) 
              {
                 retVal = retVal.Substring(0, 8);             
              }                         
  
            //else {
            //     retVal = retVal.Replace(".", ""); 
            //  }    
            
      
              //Console.WriteLine(retVal);
            return retVal;  
        }

        public static String Date2ConvertWithPattern(string fileWithSinkDate, string regexPattern, string dateInFilename)
        {
            //Your code goes here
            string pattern = regexPattern;
            Regex r = new Regex(pattern);
            //MatchCollection mc = r.Matches("dies ist ein Text vom 15/05/2023");
            MatchCollection mc = r.Matches(fileWithSinkDate);            
           

            string retVal = string.Empty;
            for (int i = 0; i < mc.Count; i++)
            { retVal += mc[i].Value; }
            if (retVal.Length > 10) { retVal = retVal.Substring(0,8).Replace("-", "").Replace(".", "").Replace("/", ""); }
            //Console.WriteLine(retVal);
            
            DateTime d;

              bool chValidity;
            
              if(dateInFilename.ToLower() == "j")
              {             
                  chValidity= DateTime.TryParseExact(retVal, "dd.MM.yyyy", CultureInfo.InvariantCulture, DateTimeStyles.None, out d);

                  if (chValidity == true) 
                  {
                     retVal = retVal.Substring(6, 4) + retVal.Substring(3, 2) + retVal.Substring(0, 2);       
                  } 
                  
                  chValidity = DateTime.TryParseExact(retVal, "dd/MM/yyyy", CultureInfo.InvariantCulture, DateTimeStyles.None, out d);

                  if (chValidity == true) 
                  {
                     retVal = retVal.Substring(6, 4) + retVal.Substring(3, 2) + retVal.Substring(0, 2);           
                  } 
                  
                  chValidity = DateTime.TryParseExact(retVal, "dd-MM-yyyy", CultureInfo.InvariantCulture, DateTimeStyles.None, out d);
                  if (chValidity == true) 
                  {
                     retVal = retVal.Substring(6, 4) + retVal.Substring(3, 2) + retVal.Substring(0, 2);              
                  }  

                  chValidity = DateTime.TryParseExact(retVal, "MM.dd.yyyy", CultureInfo.InvariantCulture, DateTimeStyles.None, out d);
                  if (chValidity == true) 
                  {
                     retVal = retVal.Substring(6, 4) + retVal.Substring(3, 2) + retVal.Substring(0, 2);              
                  }  
                  
                  chValidity = DateTime.TryParseExact(retVal, "MM/dd/yyyy", CultureInfo.InvariantCulture, DateTimeStyles.None, out d);
                  if (chValidity == true) 
                  {
                     retVal = retVal.Substring(6, 4) + retVal.Substring(3, 2) + retVal.Substring(0, 2);              
                  }  
                  
                  chValidity = DateTime.TryParseExact(retVal, "MM-dd-yyyy", CultureInfo.InvariantCulture, DateTimeStyles.None, out d);
                  if (chValidity == true) 
                  {
                     retVal = retVal.Substring(6, 4) + retVal.Substring(3, 2) + retVal.Substring(0, 2);              
                  }              
      
                  chValidity = DateTime.TryParseExact(retVal, "yyyyMMdd", CultureInfo.InvariantCulture, DateTimeStyles.None, out d);
                  if (chValidity == true) 
                  {
                     retVal = retVal.Substring(0,8);            
                  } 
                  
                  chValidity = DateTime.TryParseExact(retVal, "yyyyMMddmmhh", CultureInfo.InvariantCulture, DateTimeStyles.None, out d);
                  if (chValidity == true) 
                  {
                     retVal = retVal.Substring(0,8);            
                  }     
                  
                  chValidity = DateTime.TryParseExact(retVal.Substring(0, 8), "yyyy-MM-ddyyyy-MM-dd", CultureInfo.InvariantCulture, DateTimeStyles.None, out d);
                  if (chValidity == true) 
                  {
                     retVal = retVal.Substring(0,8);              
                  }    
                  
                  chValidity = DateTime.TryParseExact(retVal.Substring(0, 8), "ddMMyyyy", CultureInfo.InvariantCulture, DateTimeStyles.None, out d);
                  if (chValidity == true) 
                  {
                     retVal = retVal.Substring(4, 4) + retVal.Substring(2, 2) + retVal.Substring(0, 2);              
                  }      
                  
                  chValidity = DateTime.TryParseExact(retVal, "MMddyyyy", CultureInfo.InvariantCulture, DateTimeStyles.None, out d);
                  if (chValidity == true) 
                  {
                     retVal = retVal.Substring(4, 4) + retVal.Substring(0, 2) + retVal.Substring(2, 2);             
                  }    

                  chValidity = DateTime.TryParseExact(retVal.Substring(0,8), "yyyyMMdd", CultureInfo.InvariantCulture, DateTimeStyles.None, out d);
                  if (chValidity == true) 
                  {
                     retVal = retVal.Substring(0, 8);             
                  }                         
      
                  //else {
                  //     retVal = retVal.Replace(".", ""); 
                  //  }    
             } else {
                  retVal = DateTime.Now.ToString("yyyyMMdd"); 
              }     
         
              //Console.WriteLine(retVal);
            return retVal;  
        }        
    }    
}