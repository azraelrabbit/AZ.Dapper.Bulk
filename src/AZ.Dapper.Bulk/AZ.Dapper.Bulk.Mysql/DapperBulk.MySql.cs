using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlTypes;
using System.IO;
using System.Linq;
using System.Reflection;
using System.Security.AccessControl;
using System.Text;
using Dapper;
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


        public static void BulkInsertCreateTable(this MySqlConnection conn, DataTable sourceData, int varcharLength=64)
        {
            var csvFile = string.Empty;

            CreateTable(conn,sourceData,varcharLength);

            csvFile = CreateCsvFile(sourceData);

            DoBulkInert(conn, csvFile, sourceData.TableName);
        }

        public static void BulkInsertCreateTable<T>(this MySqlConnection conn, List<T> sourceData, string destTableName, int varcharLength = 64)
        {
            var csvFile = string.Empty;
            CreateTable<T>(conn, destTableName, varcharLength);
            csvFile = CreateCsvFile(sourceData);

            DoBulkInert(conn, csvFile, destTableName);
        }

        static void CreateTable(MySqlConnection conn, DataTable sourceData,int varcharLength)
        {
            var mysqlPrefix = "CREATE TABLE if not exists ";
            var mysqlSuffix = "ENGINE=InnoDB DEFAULT CHARSET=utf8;";

            var createSqlSb=new StringBuilder();
            createSqlSb.Append(mysqlPrefix);
            createSqlSb.Append(sourceData.TableName);
            createSqlSb.Append(" ( ");

            foreach (DataColumn  column in sourceData.Columns)
            {
                var dbtype = dbtypeMap[column.DataType];
                var columnStr = string.Format(" {0} {1} DEFAULT NULL,", column.ColumnName, GetDbTypeStr(dbtype, varcharLength));
                createSqlSb.Append(columnStr);
            }

            createSqlSb.Remove(createSqlSb.Length - 1, 1);//remove the last ','.
            createSqlSb.Append(" )");
            createSqlSb.Append(mysqlSuffix);

            var createSql = createSqlSb.ToString();

            conn.Execute(createSql);
        }

        static void CreateTable<T>(MySqlConnection conn,string tableName, int varcharLength)
        {
            var mysqlPrefix = "CREATE TABLE if not exists ";
            var mysqlSuffix = "ENGINE=InnoDB DEFAULT CHARSET=utf8;";

            var createSqlSb = new StringBuilder();
            createSqlSb.Append(mysqlPrefix);
            createSqlSb.Append(tableName);
            createSqlSb.Append(" ( ");

            var type = typeof (T);

            var props = type.GetProperties();

            foreach (var propertyInfo in props)
            {
                var name = propertyInfo.Name;
                var dbtype = GetDbTypeStr(dbtypeMap[propertyInfo.PropertyType],varcharLength);
                var columnStr = string.Format(" {0} {1} DEFAULT NULL,", name, dbtype);
                createSqlSb.Append(columnStr);
            }

            createSqlSb.Remove(createSqlSb.Length - 1, 1);//remove the last ','.
            createSqlSb.Append(" )");
            createSqlSb.Append(mysqlSuffix);

            var createSql = createSqlSb.ToString();

            conn.Execute(createSql);
        }

        static string GetDbTypeStr(DbType dbtype,int varcharLength=64)
        {
            if (varcharLength > 255)
            {
                varcharLength = 255;
            }

            switch (dbtype)
            {
                case DbType.String:
                    return "varchar("+varcharLength+")";
                case DbType.Int16:
                    return "int(11)";
                case DbType.Int32:
                    return "int(11)";
                case DbType.Int64:
                    return "bigint(20)";
                case DbType.Double:
                    return "float";
                case DbType.Decimal:
                    return "decimal(18,0)";
                default:
                    return "varchar(255)";
            }
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
