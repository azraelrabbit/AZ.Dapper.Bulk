using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Security.AccessControl;
using System.Text;
using MySql.Data.MySqlClient;

namespace AZ.Dapper.Bulk
{
    public static partial class DapperBulk
    {
        public static void BulkInsert(this MySqlConnection conn, DataTable sourceData)
        {
            var csvFile = string.Empty;
            csvFile = CreateCsvFile(sourceData);

            DoBulkInert(conn, csvFile, sourceData.TableName);
        }

        public static void BulkInsert<T>(this MySqlConnection conn, List<T> sourceData, string destTableName)
        {
            var csvFile = string.Empty;
            csvFile = CreateCsvFile(sourceData);

            DoBulkInert(conn, csvFile, destTableName);
        }

        public static void BulkInsert(this MySqlConnection conn, List<List<object>> sourceData, string destTableName)
        {
            var csvFile = string.Empty;
            csvFile = CreateCsvFile(sourceData);

            DoBulkInert(conn, csvFile, destTableName);
        }

        private static void DoBulkInert(MySqlConnection conn, string csvFile, string tablename)
        {
            try
            {
                var bulkLoader = new MySqlBulkLoader(conn);
                bulkLoader.TableName = tablename;
                bulkLoader.CharacterSet = "UTF-8";
                bulkLoader.FieldTerminator = FieldTerminator;
                bulkLoader.LineTerminator = LineTerminator;
                bulkLoader.NumberOfLinesToSkip = 0;
                bulkLoader.FileName = csvFile;
                var ret = bulkLoader.Load();
            }
            catch (Exception ex)
            {
                throw ex;
            }
            finally
            {
                if (!string.IsNullOrEmpty(csvFile))
                {
                    if (File.Exists(csvFile))
                    {
                        try
                        {
                            File.Delete(csvFile);
                        }
                        catch
                        {
                        }
                    }
                }
            }
        }
    }
}
