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

        private static Dictionary<Type, DbType> dbtypeMap = new Dictionary<Type, DbType>();

        static DapperBulk()
        {
            
        }

        static void InitDbTypeMap()
        {
            dbtypeMap[typeof(byte)] = DbType.Byte;
            dbtypeMap[typeof(sbyte)] = DbType.SByte;
            dbtypeMap[typeof(short)] = DbType.Int16;
            dbtypeMap[typeof(ushort)] = DbType.UInt16;
            dbtypeMap[typeof(int)] = DbType.Int32;
            dbtypeMap[typeof(uint)] = DbType.UInt32;
            dbtypeMap[typeof(long)] = DbType.Int64;
            dbtypeMap[typeof(ulong)] = DbType.UInt64;
            dbtypeMap[typeof(float)] = DbType.Single;
            dbtypeMap[typeof(double)] = DbType.Double;
            dbtypeMap[typeof(Decimal)] = DbType.Decimal;
            dbtypeMap[typeof(bool)] = DbType.Boolean;
            dbtypeMap[typeof(string)] = DbType.String;
            dbtypeMap[typeof(char)] = DbType.StringFixedLength;
            dbtypeMap[typeof(Guid)] = DbType.Guid;
            dbtypeMap[typeof(DateTime)] = DbType.DateTime;
            dbtypeMap[typeof(DateTimeOffset)] = DbType.DateTimeOffset;
            dbtypeMap[typeof(TimeSpan)] = DbType.Time;
            dbtypeMap[typeof(byte[])] = DbType.Binary;
            dbtypeMap[typeof(byte?)] = DbType.Byte;
            dbtypeMap[typeof(sbyte?)] = DbType.SByte;
            dbtypeMap[typeof(short?)] = DbType.Int16;
            dbtypeMap[typeof(ushort?)] = DbType.UInt16;
            dbtypeMap[typeof(int?)] = DbType.Int32;
            dbtypeMap[typeof(uint?)] = DbType.UInt32;
            dbtypeMap[typeof(long?)] = DbType.Int64;
            dbtypeMap[typeof(ulong?)] = DbType.UInt64;
            dbtypeMap[typeof(float?)] = DbType.Single;
            dbtypeMap[typeof(double?)] = DbType.Double;
            dbtypeMap[typeof(Decimal?)] = DbType.Decimal;
            dbtypeMap[typeof(bool?)] = DbType.Boolean;
            dbtypeMap[typeof(char?)] = DbType.StringFixedLength;
            dbtypeMap[typeof(Guid?)] = DbType.Guid;
            dbtypeMap[typeof(DateTime?)] = DbType.DateTime;
            dbtypeMap[typeof(DateTimeOffset?)] = DbType.DateTimeOffset;
            dbtypeMap[typeof(TimeSpan?)] = DbType.Time;
            dbtypeMap[typeof(object)] = DbType.Object;
        }

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
