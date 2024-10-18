using System;
using Oracle.ManagedDataAccess.Client;
using System.Collections.Generic;


namespace EBSSecurity
{
    class Program
    {
        static void Main()
        {
            string schemaStr1 = "User Id=nv71_ebs_rpl_vg;Password=vg;Data Source=(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=hydebsdbdev09.noetix.local)(PORT=1521)))(CONNECT_DATA=(SERVICE_NAME=HYD1225D)))";
            string schemaStr2 = "User Id=nv71_ebs_rpl2_vg;Password=vg;Data Source=(DESCRIPTION=(ADDRESS_LIST=(ADDRESS=(PROTOCOL=TCP)(HOST=hydebsdbdev09.noetix.local)(PORT=1521)))(CONNECT_DATA=(SERVICE_NAME=HYD1225D)))";

            try
            {
                using (OracleConnection conn1 = new OracleConnection(schemaStr1))
                using (OracleConnection conn2 = new OracleConnection(schemaStr2))
                {
                    conn1.Open();
                    conn2.Open();

                    // Retrieve tables names from both schemas
                    List<string> schema1 = GetTableNames(conn1, "nv71_ebs_rpl_vg");
                    List<string> schema2 = GetTableNames(conn2, "nv71_ebs_rpl2_vg");

                    // Compare tables
                    ComparisonTask(conn1, conn2, schema1, schema2);

                    Console.WriteLine("Comparison task completed successfully");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Comparison Task failed: " + ex.Message);
            }
        }

        // Retrieve tables names from schema
        public static List<string> GetTableNames(OracleConnection conn, string schema)
        {
            var tableNames = new List<string>();
            Console.WriteLine("Printing tables-------------------------------------------------------------------");
            int count = 1;
            string query = @"SELECT TABLE_NAME FROM USER_TABLES ORDER BY TABLE_NAME";
            using (OracleCommand cmd = new OracleCommand(query, conn))
            {
                using (OracleDataReader reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        string objectName = reader.GetString(0);
                        tableNames.Add($"{objectName}");
                        Console.WriteLine(count++ + $" {objectName}");
                    }
                }
            }
            return tableNames;
        }

        // Compare tables between two schemas
        static void ComparisonTask(OracleConnection conn1, OracleConnection conn2, List<string> schema1, List<string> schema2)
        {
            int count = 1;
            foreach (string tableName in schema1)
            {
                if (schema2.Contains(tableName))
                {
                    Console.WriteLine(count++ + $" Comparing table: {tableName}");
                    Helper.CompareTablesData(conn1, conn2, tableName);
                    if (tableName.Contains("N_SM") || tableName.Contains("N_SEG") || tableName.Contains("N_SECURITY"))
                        Helper.CompareWholeData(conn1, conn2, tableName);
                    Console.WriteLine("-----------------------------------------------------------------------------------------------------");
                }
                else
                {
                    Console.WriteLine($"Table {tableName} is missing in Schema 2");
                }
            }
        }
    }
}
