using Newtonsoft.Json.Linq;
using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SqlClient;
using System.IO;
using unirest_net.http;


/// <summary>
/// Pulls a list of deals moved from 1st Appt Scheduled to another stage from the NOE database, then calls search query using BASE API
/// to read loss reasons. This is for the purpose of filtering leads 1st Appointments Done.
/// </summary>
namespace LostDealReasonCheck {
    class Program {

        public static DateTime myMonday;
        public static DateTime mySunday;
        public static StreamWriter log; //log file written to \\NAS3\NOE_Docs$\RALIM\Logs\Base\
        public static string token = "";
        public static string startingURL = @"https://api.getbase.com/v3/deals/search";
        public static string connString = "Data Source=RALIMSQL1;Initial Catalog=CAMSRALFG;Integrated Security=SSPI;";

        static void Main(string[] args) {
            myMonday = GetLastMonday(DateTime.Now).ToUniversalTime();
            mySunday = myMonday.AddDays(7).AddMinutes(-1).ToUniversalTime(); //Sunday at 11:59:59

            DateTime now = DateTime.Now;
            string logPath = @"\\NAS3\NOE_Docs$\RALIM\Logs\Base\ProgFrom1stAppt_" + now.ToString("ddMMyyyy") + ".txt";

            if (!File.Exists(logPath)) {
                using (StreamWriter sw = File.CreateText(logPath)) {
                    sw.WriteLine("Creating log file for " + now.ToString("ddMMyyyy") + " at " + now.ToString());
                }
            }

            log = File.AppendText(logPath);
            log.WriteLine("\n\nStarting Progressed from 1st Appt Fetch at " + now);
            Console.WriteLine("Starting Progressed from 1st Appt Fetch at " + now);

            List<long> dealsFound = getDeals(myMonday, mySunday);
            //owners = new List<Owner>();
            var fs = new FileStream(@"C:\apps\NiceOffice\token", FileMode.Open, FileAccess.Read, FileShare.ReadWrite);

            using (var sr = new StreamReader(fs)) {
                token = sr.ReadToEnd();
            }
            List<ResultLine> lines = new List<ResultLine>();

            string payload = payLoad(myMonday, mySunday, dealsFound);//build Query string
            string rawJSON = Post(startingURL, token, payload.ToString()); //make post call

            var jObj = JObject.Parse(rawJSON) as JObject;//load response
            var jArr = jObj["items"][0]["items"];//extract data

            foreach (var item in jArr) {//load results into a list
                ResultLine tLine = new ResultLine(
                    Convert.ToInt64(item["data"]["id"]),
                    Convert.ToInt64(item["data"]["owner"]["id"]), 
                    item["data"]["loss_reason"]["name"].ToString());
                lines.Add(tLine);
                log.WriteLine(tLine);
            }
            SendLines(lines); // makes the calls
        }

        /// <summary>
        /// Fetches a list of deals moved from 1st appointment to any other stage.
        /// </summary>
        /// <param name="mon"></param>
        /// <param name="sun"></param>
        /// <returns>List<long> of deal id numbers</long></returns>
        static List<long> getDeals(DateTime mon, DateTime sun) {
            List<long> dealsFound = new List<long>();
            string sqlStr = "SELECT [deal_id] FROM Base_StageChanges " +
                            "WHERE [previous_stage_id] = 4517459 " +
                            "AND [changed_at] BETWEEN '" + mon.ToString("MM/dd/yyyy") + "' And '" + sun.ToString("MM/dd/yyyy") + "';";

            using (SqlConnection connection = new SqlConnection(connString)) {
                using (SqlCommand command = new SqlCommand(sqlStr, connection)) {
                    try {
                        connection.Open();
                        SqlDataReader reader = command.ExecuteReader();

                        while (reader.Read()) {
                            dealsFound.Add(Convert.ToInt64(reader["deal_id"]));
                            //Console.WriteLine("DealID: \t" + reader["deal_id"].ToString());
                            log.WriteLine("DealID: \t" + reader["deal_id"].ToString());
                            log.Flush();
                        }
                    } catch (Exception ex) {
                        log.WriteLine("\n\n" + ex + "\n\n********************************************************************");
                        Console.WriteLine("\n\n" + ex + "\n\n********************************************************************");
                    } finally {
                        connection.Close();
                    }
                }
            }
            return dealsFound;
        }

        /// <summary>
        /// Walk backwards until the date for the last Monday is found.
        /// </summary>
        /// <param name="dt">DateTime starting time to walk back from</param>
        /// <returns>DateTime of the last Monday</returns>
        public static DateTime GetLastMonday(DateTime dt) {
            if (dt.DayOfWeek == DayOfWeek.Monday) {
                return dt.AddDays(-7).Date;
            }

            bool stop = false;
            DateTime temp = dt.AddDays(-1);
            while (!stop) {
                if (temp.DayOfWeek == DayOfWeek.Monday) {
                    stop = true;
                } else {
                    temp = temp.AddDays(-1);
                }
            }
            return temp.Date;
        }

        /// <summary>
        /// 
        /// </summary>
        /// <param name="mon"></param>
        /// <param name="sun"></param>
        /// <param name="dealIDs"></param>
        /// <returns></returns>
        static string payLoad(DateTime mon, DateTime sun, List<long> dealIDs) {
            string pStr = @"";
            string idStr = @"";

            foreach (long dID in dealIDs) {
                idStr += dID + ", ";
            }

            idStr = idStr.Substring(0, (idStr.Length - 2));
            //Console.WriteLine(idStr);

            pStr = "{\"items\": [{\"data\": {\"query\": {\"projection\": [{\"name\": \"created_at\"}, " +
                "{\"name\": \"owner\"}, {\"name\": \"loss_reason\"}, {\"name\": \"contact\"}, {\"name\": \"stage\"}]," + "" +
                "\"filter\": {\"filter\": {\"attribute\": {\"name\": \"id\"},\"parameter\": {\"any\": [" + idStr +
                "]}}}},\"per_page\": 100,\"hits\": true}}]}";

            return pStr;
        }

        public static string Get(string url, string token) {
            string body = "";
            try {
                HttpResponse<string> jsonReponse = Unirest.get(url)
                    .header("accept", "application/json")
                    .header("Authorization", "Bearer " + token)
                    .asJson<string>();
                body = jsonReponse.Body.ToString();
                return body;
            } catch (Exception ex) {
                //log.WriteLine(ex);
                //log.Flush();
                Console.WriteLine(ex);
                return body;
            }
        }

        public static string Post(string url, string token, string payload) {
            string body = "";
            try {
                HttpResponse<string> jsonReponse = Unirest.post(url)
                    .header("accept", "application/json")
                    .header("Authorization", "Bearer " + token)
                    .body(payload)
                    .asJson<string>();
                body = jsonReponse.Body.ToString();
                return body;
            } catch (Exception ex) {
                //log.WriteLine(ex);
                //log.Flush();
                Console.WriteLine(ex);
                return body;
            }
        }

        public static int CleanTable() {
            int result = -1;
            using (SqlConnection connection = new SqlConnection(connString)) {
                string sqlStr = "DELETE FROM [CAMSRALFG].[dbo].[Base_1AppProg] WHERE [CAMSRALFG].[dbo].[Base_1AppProg].[lineID] is not null";
                using (SqlCommand command = new SqlCommand(sqlStr, connection)) {
                    try {
                        connection.Open();
                        result = command.ExecuteNonQuery();
                        if (result == 0) {
                            log.WriteLine("WARNING: No lines were removed from table. Empty table or error?");
                            Console.WriteLine("WARNING: No lines were removed from table. Empty table or error?");
                        } else {
                            log.WriteLine("deleted " + result + " previous records");
                            Console.WriteLine("deleted " + result + " previous records");
                        }
                    } catch (Exception ex) {
                        log.WriteLine("\n\n" + ex + "\n\n********************************************************************");
                        Console.WriteLine("\n\n" + ex + "\n\n********************************************************************");
                    } finally {
                        connection.Close();
                    }
                }
            }
            return result;
        }

        public static void SendLines(List<ResultLine> lines) {
            CleanTable(); // DELETE to remove old entries
            string sqlStr = @"INSERT INTO [CAMSRALFG].[dbo].[Base_1AppProg] ([owner_id], [deal_id], [loss_reason]) " +
                "VALUES (@owner_id, @deal_id, @loss_reason)";

            using (SqlConnection connection = new SqlConnection(connString)) {
                foreach (ResultLine line in lines) {
                    using (SqlCommand command = new SqlCommand(sqlStr, connection)) {
                        command.Parameters.Add("@owner_id", SqlDbType.Int).Value = line.OwnerID;
                        command.Parameters.Add("@deal_id", SqlDbType.Int).Value = line.DealID;
                        command.Parameters.Add("@loss_reason", SqlDbType.NVarChar).Value = line.LossName;

                        try {
                            connection.Open();
                            int result = command.ExecuteNonQuery();
                            if (result <= 0) {
                                Console.WriteLine("INSERT failed for " + command.ToString());
                            } else {
                                Console.WriteLine("Inserted records: " + result);
                            }
                        } catch (Exception ex) {
                            Console.WriteLine(ex);
                        } finally {
                            connection.Close();
                        }
                    }
                }
                log.WriteLine("Done! Writing to Table");
                log.Flush();
                log.Close();
                Console.WriteLine("Done! Writing to Table");
            }
        }
    }
}
