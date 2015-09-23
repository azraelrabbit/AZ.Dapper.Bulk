using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;
using Dapper;

namespace AZ.Dapper.Bulk
{
    public static partial class DapperBulk
    {
        public static void BulkInsert(this SqlConnection conn, DataTable sourceData, int batchSize = 5000)
        {
            using (var bulkCopy = new SqlBulkCopy(conn))
            {
                bulkCopy.BatchSize = batchSize;
                bulkCopy.DestinationTableName = sourceData.TableName;

                foreach (DataColumn column in sourceData.Columns)
                {
                    bulkCopy.ColumnMappings.Add(column.ColumnName, column.ColumnName);
                }

                bulkCopy.WriteToServer(sourceData);
            }
        }

        public static void BulkInsert(this SqlConnection conn, DataTable sourceData, string destTableName,int batchSize = 5000)
        {
            if (!string.IsNullOrEmpty(destTableName))
            {
                sourceData.TableName = destTableName;
            }
            BulkInsert(conn, sourceData, batchSize);
        }

        public static void BulkInsertCreateTable(this SqlConnection conn, DataTable sourceData, string destTableName,int batchSize = 5000, int varcharLength = 64)
        {
            if (!string.IsNullOrEmpty(destTableName))
            {
                sourceData.TableName = destTableName;
            }

            CreateTable(conn, sourceData, varcharLength);

            BulkInsert(conn, sourceData, batchSize);
        }
        public static void BulkInsertCreateTable(this SqlConnection conn, DataTable sourceData,int batchSize = 5000, int varcharLength = 64)
        {
            CreateTable(conn, sourceData, varcharLength);

            BulkInsert(conn, sourceData, batchSize);
        }


        public static void BulkInsertCreateTable(this SqlConnection conn, DataTable sourceData,List<SqlBulkCopyColumnMapping> columnMappingCollection, int batchSize = 5000, int varcharLength = 64)
        {
            CreateTable(conn, sourceData,columnMappingCollection, varcharLength);

            BulkInsert(conn, sourceData, batchSize);
        }

        public static void BulkInsertCreateTable(this SqlConnection conn, DataTable sourceData, string destTableName, List<SqlBulkCopyColumnMapping> columnMappingCollection, int batchSize = 5000, int varcharLength = 64)
        {
            if (!string.IsNullOrEmpty(destTableName))
            {
                sourceData.TableName = destTableName;
            }

            CreateTable(conn, sourceData, columnMappingCollection, varcharLength);

            BulkInsert(conn, sourceData, batchSize);
        }

        public static void BulkInsert(this SqlConnection conn, DataTable sourceData,List<SqlBulkCopyColumnMapping> columnMappingCollection, int batchSize = 5000)
        {
            using (var bulkCopy = new SqlBulkCopy(conn))
            {
                bulkCopy.BatchSize = batchSize;
                bulkCopy.DestinationTableName = sourceData.TableName;

                foreach (var cm in columnMappingCollection)
                {
                    bulkCopy.ColumnMappings.Add(cm);
                }

                bulkCopy.WriteToServer(sourceData);
            }
        }

        public static void BulkInsert(this SqlConnection conn, DataTable sourceData, string destTableName,List<SqlBulkCopyColumnMapping> columnMappingCollection, int batchSize = 5000)
        {
            if (!string.IsNullOrEmpty(destTableName))
            {
                sourceData.TableName = destTableName;
            }

            BulkInsert(conn, sourceData, columnMappingCollection, batchSize);
        }
        static void CreateTable(SqlConnection conn, DataTable sourceData, int varcharLength)
        {
            var mssqlPrefix = "if(exists(select * from sysobjects  where name='" + sourceData.TableName + "')) CREATE TABLE  ";
            var mssqlSuffix = ";";

            var createSqlSb = new StringBuilder();
            createSqlSb.Append(mssqlPrefix);
            createSqlSb.Append(sourceData.TableName);
            createSqlSb.Append(" ( ");

            foreach (DataColumn column in sourceData.Columns)
            {
                var dbtype = dbtypeMap[column.DataType];
                var columnStr = string.Format(" {0} {1}  NULL,", column.ColumnName, GetDbTypeStr(dbtype, varcharLength));
                createSqlSb.Append(columnStr);
            }

            createSqlSb.Remove(createSqlSb.Length - 1, 1);//remove the last ','.
            createSqlSb.Append(" )");
            createSqlSb.Append(mssqlSuffix);

            var createSql = createSqlSb.ToString();

            conn.Execute(createSql);
        }


        private static void CreateTable(SqlConnection conn,DataTable sourceData, List<SqlBulkCopyColumnMapping> columnMappingCollection, int varcharLength)
        {
            var mssqlPrefix = "if(exists(select * from sysobjects  where name='" + sourceData.TableName + "')) CREATE TABLE  ";
            var mssqlSuffix = ";";

            var createSqlSb = new StringBuilder();
            createSqlSb.Append(mssqlPrefix);
            createSqlSb.Append(sourceData.TableName);
            createSqlSb.Append(" ( ");

            foreach (DataColumn column in sourceData.Columns)
            {
                var dbtype = dbtypeMap[column.DataType];
                var columnName= columnMappingCollection.FirstOrDefault(p=>p.SourceColumn==column.ColumnName).DestinationColumn;
                var columnStr = string.Format(" {0} {1}  NULL,", columnName, GetDbTypeStr(dbtype, varcharLength));
                createSqlSb.Append(columnStr);
            }

            createSqlSb.Remove(createSqlSb.Length - 1, 1);//remove the last ','.
            createSqlSb.Append(" )");
            createSqlSb.Append(mssqlSuffix);

            var createSql = createSqlSb.ToString();

            conn.Execute(createSql);
        }

        static string GetDbTypeStr(DbType dbtype, int varcharLength = 64)
        {
            if (varcharLength > 255)
            {
                varcharLength = 255;
            }

            switch (dbtype)
            {
                case DbType.String:
                    return "varchar(" + varcharLength + ")";
                case DbType.Int16:
                    return "int";
                case DbType.Int32:
                    return "int";
                case DbType.Int64:
                    return "bigint";
                case DbType.Double:
                    return "float";
                case DbType.Decimal:
                    return "decimal(18,0)";
                default:
                    return "varchar(255)";
            }
        }
    }
}
