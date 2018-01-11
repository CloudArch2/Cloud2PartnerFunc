using Microsoft.Azure.WebJobs;
using Microsoft.Azure.WebJobs.Host;
using Microsoft.ServiceBus.Messaging;
using System.Data;
using System.Data.SqlClient;
using System.Text;
using Newtonsoft.Json;


namespace Cloud2PartnerFunc
{
    public static class Function1
    {
        [FunctionName("Function1")]
        public static void Run([ServiceBusTrigger("cloud2fidotopic", "Partner", AccessRights.Manage, Connection = "Connection")]string mySbMsg, TraceWriter log)
        {
            log.Info($"C# ServiceBus topic trigger function processed message: {mySbMsg}");

            DataSet dataSet = JsonConvert.DeserializeObject<DataSet>(mySbMsg);

            DataTable dataTable = dataSet.Tables["Sources"];

            foreach (DataRow row in dataTable.Rows)
            {
                log.Info($"{row["name"]} ");
            }

            try
            {
                SqlConnectionStringBuilder builder = new SqlConnectionStringBuilder();
                builder.DataSource = "cloud2fidosqlsvr.database.windows.net";
                builder.UserID = "cloud2fido";
                builder.Password = "Starbucks1234!";
                builder.InitialCatalog = "cloud2fidosqldb2";

                using (SqlConnection connection = new SqlConnection(builder.ConnectionString))
                {
                    log.Info($"\nQuery data example:");
                    log.Info($"=========================================\n");

                    connection.Open();
                    StringBuilder sb = new StringBuilder();
                    sb.Append("SELECT TOP 20 pc.PERSONNUM as PersonNumber, pc.PERSONFULLNAME as PersonFullName ");
                    sb.Append("FROM [Partner] pc ");
                    string sql = sb.ToString();

                    using (SqlCommand command = new SqlCommand(sql, connection))
                    {
                        using (SqlDataReader reader = command.ExecuteReader())
                        {
                            while (reader.Read())
                            {
                                log.Info($"{reader.GetString(0)} {reader.GetString(1)}");
                            }
                        }
                    }
                }
            }
            catch (SqlException e)
            {
                log.Info($"{e.ToString()}");
            }
            
        }

         
    }
}
