namespace azap.util
{
    public class blobcopy
    {
      
        public static bool GetCheckBlobItemName (string thema, string BlobItemName)
        {
        bool checkedItem=false;
        switch (thema)
            {
                // case "amazon/glance_views":
                //     if (BlobItemName.ToLower().Contains("#ORIGINAL.csv".ToLower())) 
                //     {
                //     checkedItem=true;
                //     }
                //     break; 
                case "amazon/searchterm":
                    if (BlobItemName.Contains("#T#") && (BlobItemName.ToLower().Contains("#amazon_de.txt".ToLower()) || BlobItemName.ToLower().Contains("#Books.txt".ToLower()) || BlobItemName.ToLower().Contains("#Kindle_Store.txt".ToLower())) )
                    {
                    checkedItem=true;
                    }
                    break;  
                    
                default:
                    checkedItem=true;
                    break;
            }
            return checkedItem;

        }

        public static string GetNewBlobItemName (string thema, string BlobItemName)
        {
        string Datum="";
        string newBlobItemName="";

        switch (thema)
            {
                case "amazon/searchterm":
                        Datum=BlobItemName.Substring(BlobItemName.LastIndexOf("#T#")+3,8);
                        string  Department=BlobItemName.Substring(BlobItemName.LastIndexOf("#")+1,BlobItemName.Length-(BlobItemName.LastIndexOf("#")+1)-".txt".Length);
                        newBlobItemName = thema + "/" + Department + "_T_" + Datum + ".txt";  
                    break;  
                // case "amazon/glance_views":
                //         Datum=BlobItemName.Substring(BlobItemName.LastIndexOf("#T#")+3,8);
                //         newBlobItemName = thema + "/cleaning/glance_views_"  + Datum + ".csv";                          
                //         break;                 
                default:
                    newBlobItemName=BlobItemName;
                    break;
            }
            return newBlobItemName;
        }

    }    

}