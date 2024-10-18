using Oracle.ManagedDataAccess.Client;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Threading.Tasks;

namespace EBSSecurity
{
    public static class Helper
    {
        
        //Compare row count of tables in both schemas
        public static void CompareTablesData(OracleConnection conn1, OracleConnection conn2, string tableName)
        {
            string schema1 = "nv71_ebs_rpl_vg";
            string schema2 = "nv71_ebs_rpl2_vg";
            int count1 = 0;
            int count2 = 0;

            try
            {
                string query1 = $@"SELECT COUNT(*) FROM {schema1}.{tableName}";
                using (OracleCommand cmd1 = new OracleCommand(query1, conn1))
                {
                    count1 = Convert.ToInt32(cmd1.ExecuteScalar());
                }

                string query2 = $@"SELECT COUNT(*) FROM {schema2}.{tableName}";
                using (OracleCommand cmd2 = new OracleCommand(query2, conn2))
                {
                    count2 = Convert.ToInt32(cmd2.ExecuteScalar());
                }

                if (count1 != count2)
                {
                    Console.WriteLine($"Data not matched for table {tableName}. Schema 1 has {count1} rows and Schema 2 has {count2} rows.");
                }
                else
                {
                    Console.WriteLine($"Data matched for table {tableName}. Both tables have {count1} rows.");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error: " + ex.Message);
            }
        }

        // Compare full data between tables in two schemas
        public static void CompareWholeData(OracleConnection conn1, OracleConnection conn2, string tableName)
        {
            string schema1 = "nv71_ebs_rpl_vg";
            string schema2 = "nv71_ebs_rpl2_vg";

            List<string> columns = GetFilteredColumns(conn1, tableName);

            if (columns.Count == 0)
            {
                Console.WriteLine($"No columns to compare for table {tableName}");
                return;
            }

            string columnList = string.Join(",", columns);
            string query1 = $@"SELECT {columnList} FROM {schema1}.{tableName} MINUS SELECT {columnList} FROM {schema2}.{tableName}";
            string query2 = $@"SELECT {columnList} FROM {schema2}.{tableName} MINUS SELECT {columnList} FROM {schema1}.{tableName}";

            try
            {
                using (OracleCommand cmd1 = new OracleCommand(query1, conn1))
                using (OracleCommand cmd2 = new OracleCommand(query2, conn2))
                {
                    using (OracleDataReader reader1 = cmd1.ExecuteReader())
                    {
                        if (reader1.HasRows)
                        {
                            Console.WriteLine($"Mismatches found in {tableName} from Schema 1 compared to Schema 2");
                        }
                        else
                        {
                            Console.WriteLine($"No differences found from Schema 1 to Schema 2 for {tableName}");
                        }
                    }

                    using (OracleDataReader reader2 = cmd2.ExecuteReader())
                    {
                        if (reader2.HasRows)
                        {
                            Console.WriteLine($"Mismatches found in {tableName} from Schema 2 compared to Schema 1");
                        }
                        else
                        {
                            Console.WriteLine($"No differences found from Schema 2 to Schema 1 for {tableName}");
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"Error comparing tables {tableName}: {ex.Message}");
            }
        }

        //Filter desired columns among list of columns
        public static List<string> GetFilteredColumns(OracleConnection conn, string tableName)
        {
            var columns = new List<string>();
            string query = $@"SELECT COLUMN_NAME, DATA_TYPE FROM USER_TAB_COLUMNS WHERE TABLE_NAME = '{tableName.ToUpper()}'";
            using (OracleCommand cmd = new OracleCommand(query, conn))
            {
                using (OracleDataReader reader = cmd.ExecuteReader())
                {
                    while (reader.Read())
                    {
                        string columnName = reader.GetString(0);
                        string columnType = reader.GetString(1);
                        if (!IsExcludedColumn(columnName, columnType))
                        {
                            columns.Add(columnName);
                        }
                    }
                }
            }

            return columns;
        }

        //Exclude unwanted columns
        public static bool IsExcludedColumn(string columnName, string columnType)
        {
            List<string> excludeColumns = new List<string> { "CREATED_BY", "LAST_UPDATED_BY" };
            return excludeColumns.Contains(columnName.ToUpper()) ||
                   columnType.ToUpper().Contains("DATE") ||
                   columnType.ToUpper().Contains("ROWID") ||
                   columnName.ToUpper().Contains("_ID");
        }
    }
}
