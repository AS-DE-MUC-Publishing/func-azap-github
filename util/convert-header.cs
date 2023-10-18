using System;
//using System.Collections.Generic;
//using System.Linq;
using System.Text.RegularExpressions;
using System.Globalization;
    
namespace azap.util
{
    public class ConvertHeader
    {
        public static String Header2Convert(string header_source)
        {
              
            string retVal = string.Empty;
            //bool chValidity;
            
              if (header_source == "Datum der Berichterstattung") 
              {
                 retVal = "report_date";       
              } 
            
               if (header_source == "Suchfrequenz-Rang") 
              {
                 retVal = "search_rank";       
              }

              if (header_source == "Suchbegriff") 
              {
                 retVal = "search_term";       
              }

              if (header_source == "Meistgeklickte Kategorie #1") 
              {
                 retVal = "category_1";       
              }
              
              if (header_source == "Meistgeklickte Kategorie #2") 
              {
                 retVal = "category_2";       
              }

              if (header_source == "Meistgeklickte Kategorie #3") 
              {
                 retVal = "category_3";       
              }

              if (header_source == "Am häufigsten aufgerufenes Produkt Nr. 1: ASIN") 
              {
                 retVal = "product_asin_1";       
              }

              if (header_source == "Am häufigsten aufgerufenes Produkt Nr. 2: ASIN") 
              {
                 retVal = "product_asin_2";       
              }

              if (header_source == "Am häufigsten aufgerufenes Produkt Nr. 3: ASIN") 
              {
                 retVal = "product_asin_3";       
              }    

              if (header_source == "Am häufigsten aufgerufenes Produkt Nr. 1: Produktbezeichnung") 
              {
                 retVal = "product_name_1";       
              }

              if (header_source == "Am häufigsten aufgerufenes Produkt Nr. 2: Produktbezeichnung") 
              {
                 retVal = "product_name_2";       
              }

              if (header_source == "Am häufigsten aufgerufenes Produkt Nr. 3: Produktbezeichnung") 
              {
                 retVal = "product_name_3";       
              }                                                                                                                                
  
            //else {
            //     retVal = retVal.Replace(".", ""); 
            //  }    
            
      
              //Console.WriteLine(retVal);
            return retVal;  
        }
    }

}       