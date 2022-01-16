using System;
using System.Data.SqlClient;
using System.Data;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace TextToSQL
{
    public class SQLExport : IDisposable
    {
        public string ConnectionString { get; set; }
        public string SchemaName { get; set; }
        public SQLExport(string connectionstring, string schema)
        {
            SchemaName = schema;
            ConnectionString = connectionstring;
        }

        #region create destination table

        public string BuildCreateTableQuery(DataTable dt)
        {

            string strQuery = "if not exists(select * from information_schema.tables where table_name = '" + dt.TableName +
                "' and table_schema = '" + SchemaName + "')" +
                "create table [" + SchemaName + "].[" + dt.TableName + "](";


            foreach (DataColumn Col in dt.Columns)
            {
            strQuery += "[" + Col.ColumnName + "] [nvarchar](255) NULL ,";

            }

            strQuery = strQuery.TrimEnd(',');

            strQuery += ") ON [PRIMARY]";


            return strQuery;
        }

        public  int CreateDestinationTable(DataTable dt)
        {

            string cmd = BuildCreateTableQuery(dt);
            int result = 0;
            try
            {
                using (SqlConnection sqlcon = new SqlConnection(ConnectionString))
                {

                    if (sqlcon.State != ConnectionState.Open)
                        sqlcon.Open();

                    using (SqlCommand cmdCreateTable = new SqlCommand(cmd))
                    {

                        cmdCreateTable.CommandTimeout = 0;
                        cmdCreateTable.Connection = sqlcon;
                        result = cmdCreateTable.ExecuteNonQuery();

                    }


                }

            }
            catch (Exception ex)
            {
                throw ex;

            }

            return result;
        }

        #endregion

        #region Insert to Db using SQLBulk

        public void InsertUsingSQLBulk(DataTable dt)
        {


            try
            {
                using (var bulkCopy = new SqlBulkCopy(ConnectionString, SqlBulkCopyOptions.KeepIdentity))
                {

                    foreach (DataColumn col in dt.Columns)
                    {
                        bulkCopy.ColumnMappings.Add(col.ColumnName, col.ColumnName);
                    }

                    bulkCopy.BulkCopyTimeout = 600;
                    bulkCopy.DestinationTableName = "[" + SchemaName + "].[" + dt.TableName + "]";
                    bulkCopy.WriteToServer(dt);
                }

            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

        #endregion

        #region Insert using SQL statement

        private string BuildInsertStatement(DataTable dt, int startindex, int rowscount)
        {

            string strQuery = "INSERT INTO [" + SchemaName + "].[" + dt.TableName + "] (";

            foreach (DataColumn dc in dt.Columns)
            {

                strQuery = strQuery + "[" + dc.ColumnName + "],";

            }

            strQuery = strQuery.TrimEnd(',') + ")  VALUES ";

            int i = startindex ;
            int lastrowindex = startindex + rowscount - 1;

            for (i = startindex; i <= lastrowindex; i++)
            {
                strQuery = strQuery + "(";
                foreach (DataColumn Col in dt.Columns)
                {
                 strQuery += "'" + dt.Rows[i][Col.ColumnName].ToString() + "',";
                }

                strQuery = strQuery.TrimEnd(',') + "),";
            }

            strQuery = strQuery.TrimEnd(',');
            return strQuery;
        }


        public  void InsertIntoDb(DataTable dt)
        {
            int rowsperbatch = 999;
            try
            {
                using (SqlConnection sqlcon = new SqlConnection(ConnectionString))
                {

                    if (sqlcon.State != ConnectionState.Open)
                        sqlcon.Open();


                    int totalcount = dt.Rows.Count;
                    int currentindex = 0;



                    while (currentindex < totalcount)
                    {
                      
                        string strQuery = "";

                        if ((currentindex + rowsperbatch) >= totalcount)
                            rowsperbatch = totalcount - currentindex;

                        if (rowsperbatch == 0)
                            break;
                        try
                        {

                            strQuery = BuildInsertStatement(dt, currentindex, rowsperbatch);
                        }
                        catch(Exception ex)
                        {

                        }
                        using (SqlCommand sqlcmd = new SqlCommand(strQuery, sqlcon))
                        {
                            sqlcmd.ExecuteNonQuery();
                        }


                        currentindex = currentindex + rowsperbatch;
                    }

                }
            }
            catch (Exception ex)
            {
                throw ex;
            }
        }

     

        #endregion

        public void Dispose()
        {

        }
    }
}