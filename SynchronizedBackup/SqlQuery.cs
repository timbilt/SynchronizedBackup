using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.Runtime.CompilerServices;
using System.Configuration;

using System.Data.SqlClient;

namespace CCSI.SqlServer
{
    public class SqlQuery
    {
        public static void ExecuteNonQuery(String command)
        {
            using (var query = new Query(command))
            {
                query.ExecuteNonQuery();
            }            
        }

        public static Int32 ExecuteQueryInt32(String command)
        {
            using (var query = new Query(command))
            {
                return query.ExecuteQueryInt32();
            }             
        }

        public static Boolean ExecuteQueryBoolean(String command)
        {
            using (var query = new Query(command))
            {
                return query.ExecuteQueryBoolean();
            }             
        }

        public static DateTime ExecuteQueryDateTime(String command)
        {
            using (var query = new Query(command))
            {
                return query.ExecuteQueryDateTime();
            }
        }

        class Query : IDisposable
        {
            private SqlConnection sql_connection;
            private SqlCommand sql_command;

            public Query(String command)
                : this("context connection=true", command)
            {
            }

            public Query(String connection, String command)
            {
                sql_connection = new SqlConnection(connection);
                sql_connection.Open();
                sql_command = new SqlCommand(command, sql_connection);
            }

            public void ExecuteNonQuery()
            {
                sql_command.ExecuteNonQuery();
            }

            public Int32 ExecuteQueryInt32()
            {
                using (SqlDataReader reader = sql_command.ExecuteReader())
                {
                    if (reader.Read())
                        return reader.GetInt32(0);
                    else
                        return 0;
                }
            }

            public Boolean ExecuteQueryBoolean()
            {
                using (SqlDataReader reader = sql_command.ExecuteReader())
                {
                    if (reader.Read())
                        return reader.GetBoolean(0);
                    else
                        return false;
                }
            }

            public DateTime ExecuteQueryDateTime()
            {
                using (SqlDataReader reader = sql_command.ExecuteReader())
                {
                    if (reader.Read())
                        return reader.GetDateTime(0);
                    else
                        return new DateTime(1900, 1, 1);
                }
            }

            public void Dispose()
            {
                sql_connection.Dispose();
            }
        }
    }
}
