using System;
using System.Data.SqlClient;
using System.Linq;
using PetaPoco;

namespace DbConnectionProfiler
{
    public static class Program
    {
        public static void Main()
        {
            var cs = @"Server={Your Server};Database={Your Database};Uid={Your Uid};Pwd={Your Pwd};";
            using (var db = new Database(DbProfiler.Wrap(new SqlConnection(cs))))
            {
                var sql = "SELECT * FROM sys.databases WHERE name LIKE @0";
                var databases = db.Query<dynamic>(sql, "%").ToList();
                Console.WriteLine("Database Count: {0}", databases.Count);
            }
            Console.ReadKey();
        }
    };
}
