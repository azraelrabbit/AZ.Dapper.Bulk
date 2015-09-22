using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;

namespace AZ.Dapper.Bulk
{
    public static partial class DapperBulk
    {
        public static void BulkInsert(this SqlConnection conn,DataTable sourceData,int batchSize=5000)
        {
            using (var bulkCopy = new SqlBulkCopy(conn))
            {
                bulkCopy.BatchSize = batchSize;
                bulkCopy.DestinationTableName = sourceData.TableName;

                foreach ( DataColumn column in sourceData.Columns)
                {
                    bulkCopy.ColumnMappings.Add(column.ColumnName, column.ColumnName);
                }

				bulkCopy.WriteToServer(sourceData);
            }
        }
        public static void BulkInsert(this SqlConnection conn, DataTable sourceData,SqlBulkCopyColumnMappingCollection columnMappingCollection, int batchSize = 5000)
        {
            using (var bulkCopy = new SqlBulkCopy(conn))
            {
                bulkCopy.BatchSize = batchSize;
                bulkCopy.DestinationTableName = sourceData.TableName;

                foreach (SqlBulkCopyColumnMapping cm in columnMappingCollection)
                {
                    bulkCopy.ColumnMappings.Add(cm);
                }

                bulkCopy.WriteToServer(sourceData);
            }
        }

    }
}
