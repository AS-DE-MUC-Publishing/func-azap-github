using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Text.RegularExpressions;




namespace azap.util{  
        public class SelectedColumn
        {
            public SelectedColumn(string name, int position, int colnum)
            {
                this.Name = name;
                this.Position = position;
                this.Colnum = colnum;
            }
            public string Name { set; get; }
            public int Position { set; get; }
            public int Colnum { set; get; }
            public int Count { get; }
        }

}