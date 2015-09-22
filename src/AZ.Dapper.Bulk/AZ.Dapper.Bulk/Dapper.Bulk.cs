using System;
using System.Collections.Generic;
using System.Data;
using System.IO;
using System.Linq;
using System.Text;

namespace AZ.Dapper.Bulk
{
    public static partial  class DapperBulk
    {
        private const string FieldTerminator = ",";
        private const string LineTerminator = "\r\n";

        static string CreateCsvFile(DataTable dataSource)
        {
            var tmpFilePath = GenerateTempFileName();

            foreach (DataRow row in dataSource.Rows)
            {
                var line = string.Join(FieldTerminator, row.ItemArray.ToArray());
                File.AppendAllText(tmpFilePath, line + LineTerminator, Encoding.UTF8);
            }

            return tmpFilePath;
        }

        static string CreateCsvFile<T>(List<T> dataSource)
        {
            var tmpFilePath = GenerateTempFileName();

            foreach (var item in dataSource)
            {
                var value = GetItemValue(item);
                var line = string.Join(FieldTerminator, value);
                File.AppendAllText(tmpFilePath, line + LineTerminator, Encoding.UTF8);
            }

            return tmpFilePath;
        }

        static string CreateCsvFile(List<List<object>> dataSource)
        {
            var tmpFilePath = GenerateTempFileName();

            foreach (var item in dataSource)
            {
                var line = string.Join(FieldTerminator, item);
                File.AppendAllText(tmpFilePath, line + LineTerminator, Encoding.UTF8);
            }

            return tmpFilePath;
        }

        private static string GenerateTempFileName()
        {
            var basePath = AppDomain.CurrentDomain.BaseDirectory;
            var baseTempPath = Path.Combine(basePath, "App_Data");
            var tempPath = Path.Combine(baseTempPath, "temp");

            if (!Directory.Exists(tempPath))
            {
                Directory.CreateDirectory(tempPath);
            }

            var tmpFilePath = Path.Combine(tempPath, Guid.NewGuid().ToString("n") + ".csv");
            return tmpFilePath;
        }

        static List<object> GetItemValue<T>(T item)
        {
            var values = new List<object>();

            var type = item.GetType();
            var props = type.GetProperties();
            foreach (var prop in props)
            {
                var ret = prop.GetValue(item, null);
                values.Add(ret);
            }

            return values;
        }
    }
}
