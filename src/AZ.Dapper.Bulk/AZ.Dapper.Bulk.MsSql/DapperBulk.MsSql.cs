using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.Linq;
using System.Text;

namespace AZ.Dapper.Bulk.MsSql
{
    public partial class DapperBulk
    {
        public static void BulkInsert(SqlConnection conn,DataTable sourceData,int batchSize=5000)
        {
            using (var bulkCopy = new SqlBulkCopy(conn))
            {
                bulkCopy.BatchSize = batchSize;
                bulkCopy.DestinationTableName = sourceData.TableName;
				bulkCopy.WriteToServer(sourceData);
            }
        }

       
    }
}
