using System;
using System.Data;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TextToSQL
{
    class Program
    {
        static void Main(string[] args)
        {
            //You should set your database connection string
            string connectionstring = @"Data Source=.\SQLINSTANCE;Initial Catalog=tempdb;integrated security=SSPI;";
            //You should set the text files directory
            string directory = @"E:\TextFiles";
            using (SQLExport sqlExp = new SQLExport(connectionstring, "upload"))
            {
                //if you don't want to traverse subfolders use System.IO.SearchOption.TopDirectoryOnly
                foreach (string filename in System.IO.Directory.GetFiles(directory,
                    "*.txt",System.IO.SearchOption.AllDirectories)){

                    using(TextImport txtimp = new TextImport(filename, true, 0))
                    {
                        txtimp.BuildDataTableStructure();
                        DataTable dt = txtimp.FillDataTable();
                        dt.TableName = System.IO.Path.GetFileName(filename);
                        sqlExp.CreateDestinationTable(dt);
                        //Insert using BULK INSERT
                        sqlExp.InsertUsingSQLBulk(dt);
                        
                        //Creates and Execute an INSERT INTO statment 
                        //sqlExp.InsertIntoDb(dt);
                    }
                }            
            }

        }
    }
}
