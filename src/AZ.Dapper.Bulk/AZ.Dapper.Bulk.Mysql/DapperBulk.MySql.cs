using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Security.AccessControl;
using System.Text;
using MySql.Data.MySqlClient;

namespace AZ.Dapper.Bulk.Mysql
{
    public partial class DapperBulk
    {

        private const string FieldTerminator = ",";
        private const string LineTerminator = "\r\n";

        public static void BulkInsert(MySqlConnection conn, DataTable sourceData)
        {
            var csvFile = string.Empty;
            try
            {
                csvFile = CreateCsvFile(sourceData);

                var bulkLoader = new MySqlBulkLoader(conn);
                bulkLoader.TableName = sourceData.TableName;
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

        public static void BulkInsert<T>(MySqlConnection conn, List<T> sourceData, string destTableName)
        {
            var csvFile = string.Empty;
            try
            {
                csvFile = CreateCsvFile(sourceData);

                var bulkLoader = new MySqlBulkLoader(conn);
                bulkLoader.TableName = destTableName;
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

        public static void BulkInsert<T>(MySqlConnection conn, List<List<object>> sourceData, string destTableName)
        {
            var csvFile = string.Empty;
            try
            {
                csvFile = CreateCsvFile(sourceData);

                var bulkLoader = new MySqlBulkLoader(conn);
                bulkLoader.TableName = destTableName;
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

        static string CreateCsvFile(DataTable dataSource)
        {
            var basePath = AppDomain.CurrentDomain.BaseDirectory;
            var baseTempPath = Path.Combine(basePath, "App_Data");
            var tempPath = Path.Combine(baseTempPath, "temp");

            if (!Directory.Exists(tempPath))
            {
                Directory.CreateDirectory(tempPath);
            }

            var tmpFilePath = Path.Combine(tempPath, Guid.NewGuid().ToString("n") + ".csv");

            foreach (DataRow row in dataSource.Rows)
            {
                var line = string.Join(FieldTerminator, row.ItemArray.ToArray());
                File.AppendAllText(tmpFilePath, line + LineTerminator, Encoding.UTF8);
            }

            return tmpFilePath;
        }

        static string CreateCsvFile<T>(List<T> dataSource)
        {
            var basePath = AppDomain.CurrentDomain.BaseDirectory;
            var baseTempPath = Path.Combine(basePath, "App_Data");
            var tempPath = Path.Combine(baseTempPath, "temp");

            if (!Directory.Exists(tempPath))
            {
                Directory.CreateDirectory(tempPath);
            }

            var tmpFilePath = Path.Combine(tempPath, Guid.NewGuid().ToString("n") + ".csv");

            foreach (var item in dataSource)
            {

                var value = GetItemValue(item);
                var line = string.Join(FieldTerminator, value);
                File.AppendAllText(tmpFilePath, line + LineTerminator, Encoding.UTF8);
            }

            return tmpFilePath;
        }

        static string CreateCsvFile (List<List<object>> dataSource)
        {
            var basePath = AppDomain.CurrentDomain.BaseDirectory;
            var baseTempPath = Path.Combine(basePath, "App_Data");
            var tempPath = Path.Combine(baseTempPath, "temp");

            if (!Directory.Exists(tempPath))
            {
                Directory.CreateDirectory(tempPath);
            }

            var tmpFilePath = Path.Combine(tempPath, Guid.NewGuid().ToString("n") + ".csv");

            foreach (var item in dataSource)
            {
                var line = string.Join(FieldTerminator, item);
                File.AppendAllText(tmpFilePath, line + LineTerminator, Encoding.UTF8);
            }

            return tmpFilePath;
        }

        static List<object> GetItemValue<T>(T item)
        {
            var values=new List<object>();

            var type = item.GetType();
            var props=type.GetProperties();
            foreach (var prop in props)
            {
                var ret=prop.GetValue(item, null);
                values.Add(ret);
            }

            return values;
        } 
    }
}
