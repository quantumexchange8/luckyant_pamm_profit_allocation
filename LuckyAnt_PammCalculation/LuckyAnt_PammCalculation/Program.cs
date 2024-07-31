// See https://aka.ms/new-console-template for more information
using System;
using System.Data;
using System.Threading.Tasks;
using System.Diagnostics;

using MetaQuotes.MT5CommonAPI;
using MetaQuotes.MT5ManagerAPI;

using System.Text.Json;
using System.Collections.Generic;
using System.Linq;

using MySql.Data.MySqlClient;
using MySqlX.XDevAPI.Relational;
/* 
using Telegram.Bot;
using Telegram.Bot.Types; */

namespace LuckyAnt_PammFormula
{
    internal class Program
    {
        private static CIMTManagerAPI API_mManager = null;
        private static uint MT5_CONNECT_TIMEOUT = 1000; // Delay for 1 seconds before checking again
        private static int delay_time = 500; // Delay for 0.5 seconds before checking again
        private static DateTime default_time = new(2024, 7, 20);
        
        private static string db_name = "luckyant-pamm";
        private static string conn = $"server = 68.183.177.155; uid = ctadmin; pwd = CTadmin!123; database = {db_name}; port = 3306;";

        // ----- 
        private static string serverName = "103.21.90.162";
        private static int server_port = 443;
        private static ulong adminLogin = 3001;
        private static string adminPassword = "CkVw+fQ8";

        private static long demo_mt5_account = 457282;

        // Switch 
        private static bool paid_into_MT5Live_program = false;
        private static bool paid_into_MT5Demo_program = true; // if not, live will not proceed
        
        private static bool retrieve_pamm_trades_program = true;
        private static bool pamm_calculation_allocate_program = true;
        //private static bool retrieve_copy_trades_program = false; // not 
        
        static async Task Main()
        {
            Console.WriteLine("Current database:" + conn);
            Console.WriteLine("================================================================================");
            DateTime currentDate = DateTime.Now;
            currentDate = new DateTime(currentDate.Year, currentDate.Month, currentDate.Day, 8, 0, 0);

            string input = await AwaitConsoleReadLine(1000);

            if (input == "Y" || input == "y" || input == null)
            {
                var gbl_taskStopwatch = Stopwatch.StartNew();
                //var GB_taskStopwatch = Stopwatch.StartNew();

                if (retrieve_pamm_trades_program == true)
                {
                    var taskStopwatch = Stopwatch.StartNew();
                    Console.WriteLine("");
                    Console.WriteLine(" ------------------------------------------------------------------------------------------");
                    Console.WriteLine("Retrieve Pamm Trades program started...");

                    bool initialize_flag = await Loop_InitializeMT5_PammTrades();

                    taskStopwatch.Stop();
                    Console.WriteLine("");
                    Console.WriteLine($"Task retrieve_pamm_trades_program completed in {taskStopwatch.Elapsed.TotalSeconds} seconds ({taskStopwatch.Elapsed.TotalMinutes})");
                }

                if (pamm_calculation_allocate_program == true)
                {
                    var taskStopwatch = Stopwatch.StartNew();
                    Console.WriteLine("");
                    Console.WriteLine(" ------------------------------------------------------------------------------------------");
                    Console.WriteLine("Pamm Calculation Allocation program started...");

                    await Allocate_Pamm_Calculation(); //currentDate

                    taskStopwatch.Stop();
                    Console.WriteLine("");
                    Console.WriteLine($"Task pamm_calculation_allocate_program completed in {taskStopwatch.Elapsed.TotalSeconds} seconds ({taskStopwatch.Elapsed.TotalMinutes})");
                }
                
                if (paid_into_MT5Live_program == true || paid_into_MT5Demo_program == true)
                {
                    var taskStopwatch = Stopwatch.StartNew();
                    Console.WriteLine("");
                    Console.WriteLine(" ------------------------------------------------------------------------------------------");
                    Console.WriteLine("Pamm Dividend Deposit In MT5 accounts program started...");

                    //await PammDividend_Deposit_In(); //currentDate

                    taskStopwatch.Stop();
                    Console.WriteLine("");
                    Console.WriteLine($"Task MT5_PammDividend_Deposit_In completed in {taskStopwatch.Elapsed.TotalSeconds} seconds ({taskStopwatch.Elapsed.TotalMinutes})");
                }

                /* if (retrieve_copy_trades_program == true)
                {
                    var taskStopwatch = Stopwatch.StartNew();
                    Console.WriteLine("Retrieve Copy Trades program started...");
                    
                    if(API_mManager != null ) { 
                        bool isSuccess = false;
                        proceed_MT5_CopytradesToDB(ref isSuccess);    }
                    else {  Loop_InitializeMT5_CopyTrades();   }

                    taskStopwatch.Stop();
                    Console.WriteLine("");
                    Console.WriteLine($"Task proceed_MT5_CopytradesToDB completed in {taskStopwatch.Elapsed.TotalSeconds} seconds ({taskStopwatch.Elapsed.TotalMinutes})");
                } */

                gbl_taskStopwatch.Stop();
                Console.WriteLine("");
                Console.WriteLine($"All Task completed in {gbl_taskStopwatch.Elapsed.TotalSeconds} seconds ({gbl_taskStopwatch.Elapsed.TotalMinutes})");
            }
            else if (input == "N" || input == "n")
            {
                Console.WriteLine("operation cancelled!");
                return;
            }
            else {  Console.WriteLine("Invalid input! Please try again!");  }
        }
        
        private static async Task PammDividend_Deposit_In() 
        {
            Console.WriteLine("");
            Console.WriteLine("");
            Console.WriteLine(" ------------------------------------------------------------------------------------------");
            Console.WriteLine("MT5_PammDividend_Deposit_In ... ");
            try
            {
                List<object[]> mt5_acc_List = new List<object[]>();
                List<object[]> no_mt5_acc_List = new List<object[]>();
                string sqlstr = "SELECT master_meta_login, ticket, symbol, user_id, meta_login, subscription_id, trade_profit  "+    
                                "FROM trade_pamm_investor_allocate where deleted_at is null and pamm_allocate_status = 'Pending' ; ";
                
                Console.WriteLine($"MT5_PammDividend_Deposit_In - sqlstr: {sqlstr}");
                using (MySqlConnection sql_conn = new MySqlConnection(conn))
                {
                    sql_conn.Open(); // Open the connection
                    MySqlCommand select_cmd = new MySqlCommand(sqlstr, sql_conn);
                    MySqlDataReader reader = select_cmd.ExecuteReader();
                    while (reader.Read())
                    {
                        // 0-master login, 1- ticket, 2-symbol， 3-user_id, 4-meta login, 5-subscription_id, 6-trade_profit    
                        if(!reader.IsDBNull(1) && reader.GetInt64(1) > 0 && !reader.IsDBNull(3) && reader.GetInt64(3) > 0)
                        {
                            object[] PammData =  { reader.GetInt64(0), reader.GetInt64(1), reader.GetString(2),  reader.GetInt64(3), reader.GetInt64(4), reader.GetInt64(5), reader.GetDouble(6)};
                            //if(!reader.IsDBNull(4) && reader.GetInt64(4) > 0)
                            //{
                                mt5_acc_List.Add(PammData);
                            //}
                            /* else
                            {
                                no_mt5_acc_List.Add(PammData);
                            } */
                        }
                        //Console.WriteLine($"upline_id:{reader.GetInt64(0)}, downline_id:{reader.GetInt64(1)}, meta_login:{reader.GetInt64(2)}, sym_group_id:{reader.GetInt64(3)}, close_time:{reader.GetDateTime(4)}, status:{reader.GetString(5)}, trade_volume:{reader.GetInt64(6)}, rebate_final_amt_get:{reader.GetInt64(7)}");
                    }
                } 

                Console.WriteLine($"MT5_PammDividend_Deposit_In - mt5_acc_List: {mt5_acc_List.Count} -- no_mt5_acc_List: {no_mt5_acc_List.Count}");     
                if(mt5_acc_List.Count > 0)
                {
                    if(API_mManager != null ) { DividendPamm_PamInMT5(mt5_acc_List);    }
                    else {  Loop_InitializeMT5_MT5(mt5_acc_List);   }
                } 
                /* if(no_mt5_acc_List.Count > 0)
                {
                    if(API_mManager != null ) { DividendPamm_PamInMT5(no_mt5_acc_List);    }
                    else {  Loop_InitializeMT5_MT5(no_mt5_acc_List);   }
                } */
            }
            catch (Exception ex)
            {
                Console.WriteLine($"An exception occurred: {ex}");
            }
        }

        private static void DividendPamm_PamInMT5(List<object[]> mt5_list)
        {
            Console.WriteLine("DividendPamm_PamInMT5 ... ");
            foreach (var mt5 in mt5_list)
            {
                // 0-master login, 1- ticket, 2-symbol，3-user_id, 4-meta login, 5-subscription_id, 6-trade_profit   
                long master_metalogin = (long) mt5[0];
                long master_ticket = (long) mt5[1];
                string master_symbol = (string) mt5[2];
                long subs_user_id = (long) mt5[3];
                double subs_profit = (double) mt5[6];
                subs_profit = Math.Round(subs_profit, 2);

                //if(mt5[4] != null && (long) mt5[4]  > 0) // have metalogin
                
                    long subs_metalogin = (mt5[4] != null) ? (long)mt5[4] : 0;
                    if(paid_into_MT5Demo_program == true) {  subs_metalogin = demo_mt5_account;   }
                    
                    string remarks = $"from master #{master_ticket}, {master_symbol}";
                    Console.WriteLine($"DividendPamm_PamInMT5 subs_metalogin: {subs_metalogin} -- remarks: {remarks} -- subs_profit: {subs_profit}");
                    
                    Console.WriteLine($"Select * FROM trade_pamm_investor_allocate where deleted_at is null and master_meta_login = {master_metalogin} and user_id = {subs_user_id} and ticket = {master_ticket};");
                    MTRetCode balstatus = API_mManager.DealerBalance((ulong)subs_metalogin, subs_profit, (uint)CIMTDeal.EnDealAction.DEAL_BALANCE, remarks, out ulong deal_id);
                    balstatus = MTRetCode.MT_RET_REQUEST_DONE;
                    if (balstatus == MTRetCode.MT_RET_REQUEST_DONE)
                    {
                        subs_metalogin = (mt5[4] != null) ? (long)mt5[4] : 0;
                        update_pamm_allocate( subs_metalogin , subs_user_id, master_ticket, master_symbol);
                    } 
                //}

                //else
               // {


               // }
            }
        }

        private static void update_pamm_allocate( long subs_metalogin, long subs_user_id, long pamm_ticket, string pamm_symbol )
        {
            using (MySqlConnection sql_conn = new MySqlConnection(conn))
            {
                sql_conn.Open(); // Open the connection
                //SELECT id FROM users WHERE deleted_at is null and role='member' and status = 'Active' and id not in (7)  and ( top_leader_id is null or top_leader_id not in (7) )
                string sqlstr = $"update trade_pamm_investor_allocate set pamm_allocate_status = 'Completed' where deleted_at is null and pamm_allocate_status = 'Pending' " +
                                $"and ticket = {pamm_ticket} and symbol = '{pamm_symbol}' and user_id = {subs_user_id} and meta_login = {subs_metalogin} and id > 0; ";
                Console.WriteLine($"update_pamm_allocate sqlstr: {sqlstr}");
                MySqlCommand update_cmd = new MySqlCommand(sqlstr, sql_conn);
                update_cmd.ExecuteScalar();

                Console.WriteLine($"ConnectionString: {sql_conn.ConnectionTimeout}");
            }
        } 

        /* public static double RoundDown(double number, int decimalPlaces)
        {
            double factor = Math.Pow(10, decimalPlaces);
            return Math.Floor(number * factor) / factor;
        } */

        private static async Task Allocate_Pamm_Calculation() //DateTime TimeNow
        {
            Console.WriteLine("Allocate_Pamm_Calculation ... ");
            try
            {
                List<object[]> pamm_List = new List<object[]>();

                string sqlstr = "SELECT t1.meta_login, t2.id, t1.volume, t1.trade_profit, t1.trade_swap, t1.symbol, t1.ticket, t1.trade_type, t1.time_close, t1.price_close  "+    
                                "FROM trade_pamm_masters_histories t1 INNER JOIN masters t2 ON t1.meta_login = t2.meta_login AND t1.deleted_at is null AND t2.deleted_at is null  "+
                                "WHERE t1.pamm_calculate_status = 'Pending' AND t2.status = 'Active' AND t2.category = 'pamm' ORDER BY t1.id asc ;";
                
                Console.WriteLine($"Allocate_Pamm_Calculation - sqlstr: {sqlstr}");
                using (MySqlConnection sql_conn = new MySqlConnection(conn))
                {
                    sql_conn.Open(); // Open the connection
                    MySqlCommand select_cmd = new MySqlCommand(sqlstr, sql_conn);
                    MySqlDataReader reader = select_cmd.ExecuteReader();
                    while (reader.Read())
                    {
                        // 0-master login, 1- master id, 2- master lot， 3-master pnl, 4-master swap, 5-symbol, 6-ticket, 7-trade_type, 8-close_time, 9-close_price
                        if(!reader.IsDBNull(1) && reader.GetInt64(1) > 0)
                        {
                            object[] PammData =  { reader.GetInt64(0), reader.GetInt64(1), reader.GetDouble(2), reader.GetDouble(3), reader.GetDouble(4), 
                                                reader.GetString(5), reader.GetInt64(6), reader.GetString(7), reader.GetDateTime(8), reader.GetDouble(9)
                                                };
                            pamm_List.Add(PammData);
                        }
                        //Console.WriteLine($"upline_id:{reader.GetInt64(0)}, downline_id:{reader.GetInt64(1)}, meta_login:{reader.GetInt64(2)}, sym_group_id:{reader.GetInt64(3)}, close_time:{reader.GetDateTime(4)}, status:{reader.GetString(5)}, trade_volume:{reader.GetInt64(6)}, rebate_final_amt_get:{reader.GetInt64(7)}");
                    }
                }
                
                long pamm_count = 1;
                foreach (var pamm in pamm_List)
                {
                    long pamm_master_id = (long) pamm[1];
                    if(pamm[1] != null && pamm_master_id > 0)
                    {
                        long pamm_metalogin = (long) pamm[0];
                        double pamm_lot = (double) pamm[2];
                        double pamm_pnl = (double) pamm[3];
                        double pamm_swap = (double) pamm[4];
                        string pamm_symbol = (string) pamm[5];
                        long pamm_ticket = (long) pamm[6];
                        string pamm_trade_type = (string) pamm[7];
                        DateTime pamm_close_time = (DateTime) pamm[8];
                        double pamm_close_price = (double) pamm[9];

                        long pamm_user_id = 0; long pamm_rank = 0; long pamm_active = 0; 
                        retrieve_userid_based_metalogin(pamm_metalogin, ref pamm_user_id, ref pamm_rank, ref pamm_active);

                        Console.WriteLine($"Allocate_Pamm_Calculation - pamm_count: {pamm_count}/ pamm_List: {pamm_List.Count}"); pamm_count = pamm_count+1;

                        if(pamm_user_id > 0 && pamm_active > 0)
                        {
                            insert_master_trades(pamm_user_id, pamm_master_id, pamm_metalogin, pamm_lot, pamm_pnl, pamm_swap, pamm_symbol, pamm_ticket, pamm_trade_type, pamm_close_time, pamm_close_price);
                            insert_subscriber_allocation (pamm_user_id, pamm_master_id, pamm_metalogin, pamm_lot, pamm_pnl, pamm_swap, pamm_symbol, pamm_ticket, pamm_trade_type, pamm_close_time, pamm_close_price);
                            update_master_pamm_status(pamm_metalogin, pamm_ticket, pamm_symbol);
                        }
                    }                
                }    
            }
            catch (Exception ex)
            {
                Console.WriteLine($"An exception occurred: {ex}");
            }
        }
        
        private static void update_master_pamm_status(long pamm_metalogin, long pamm_ticket, string pamm_symbol )
        {
            using (MySqlConnection sql_conn = new MySqlConnection(conn))
            {
                sql_conn.Open(); // Open the connection
                //SELECT id FROM users WHERE deleted_at is null and role='member' and status = 'Active' and id not in (7)  and ( top_leader_id is null or top_leader_id not in (7) )
                string sqlstr = $"update trade_pamm_masters_histories set pamm_calculate_status = 'Completed' where deleted_at is null and pamm_calculate_status = 'Pending' " +
                                $"and meta_login = {pamm_metalogin} and ticket = {pamm_ticket} and symbol = '{pamm_symbol}' and id > 0; ";
                Console.WriteLine($"update_pamm_subscriptions sqlstr: {sqlstr}");
                MySqlCommand update_cmd = new MySqlCommand(sqlstr, sql_conn);
                update_cmd.ExecuteScalar();

                Console.WriteLine($"ConnectionString: {sql_conn.ConnectionTimeout}");
            }
        } 

        private static double retrieve_summary_info(long pamm_master_id)
        {
            double total_funds = 0; 
            try
            {
                using (MySqlConnection sql_conn = new MySqlConnection(conn))
                {
                    sql_conn.Open(); // Open the connection  //count( distinct user_id), 
                    string sqlstr = $"select sum(subscription_amount) from pamm_subscriptions where deleted_at is null and approval_date is not null " +
                                    $"and status = 'Active' and master_id = {pamm_master_id} group by master_id, master_meta_login ; ";

                    Console.WriteLine($"sretrieve_summary_info -- sqlstr: {sqlstr} ");  
                    MySqlCommand select_cmd = new MySqlCommand(sqlstr, sql_conn);
                    object result = select_cmd.ExecuteScalar();
                    if (result != null) { total_funds = Convert.ToDouble(result);  }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"An exception occurred: {ex}");
            }
            return(total_funds);
        }

        private static List<object[]> retrieve_pamm_subscribers_tradehist_exist(long pamm_master_id, long pamm_master_ticket)
        {
            List<object[]> local_List = new List<object[]>(); 
            try
            {
                using (MySqlConnection sql_conn = new MySqlConnection(conn))
                {
                    sql_conn.Open(); // Open the connection
                    string sqlstr = $"select user_id, meta_login, ticket from trade_histories where deleted_at is null and ticket = {pamm_master_ticket}; "; // +
                                    //$"and master_id = {pamm_master_id}  ";
                    Console.WriteLine($"retrieve_pamm_subscribers_tradehist_exist -- sqlstr: {sqlstr} ");  
                    MySqlCommand select_cmd = new MySqlCommand(sqlstr, sql_conn);
                    using (MySqlDataReader reader = select_cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            object[] localData = { reader.GetInt64(0), reader.GetInt64(1), reader.GetInt64(2)  };
                            local_List.Add(localData);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"An exception occurred: {ex}");
            }
            return(local_List);
        }

        private static List<object[]> retrieve_pamm_subscribers_exist(long pamm_master_id, long pamm_master_ticket)
        {
            List<object[]> local_List = new List<object[]>(); 
            try
            {
                using (MySqlConnection sql_conn = new MySqlConnection(conn))
                {
                    sql_conn.Open(); // Open the connection
                    string sqlstr = $"select user_id, meta_login, ticket from trade_pamm_investor_allocate where deleted_at is null  " +
                                    $"and master_id = {pamm_master_id} and ticket = {pamm_master_ticket}; ";
                    Console.WriteLine($"retrieve_pamm_subscribers_exist -- sqlstr: {sqlstr} ");  
                    MySqlCommand select_cmd = new MySqlCommand(sqlstr, sql_conn);
                    using (MySqlDataReader reader = select_cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            object[] localData = { reader.GetInt64(0), reader.GetInt64(1), reader.GetInt64(2)  };
                            local_List.Add(localData);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"An exception occurred: {ex}");
            }
            return(local_List);
        }
        
        private static List<object[]> retrieve_pamm_subscribers(long pamm_master_id,  long pamm_master_ticket)
        {
            List<object[]> local_List = new List<object[]>(); 
            try
            {
                using (MySqlConnection sql_conn = new MySqlConnection(conn))
                {
                    sql_conn.Open(); // Open the connection
                    string sqlstr = $"select id, user_id, subscription_amount, coalesce(meta_login,0) from pamm_subscriptions where deleted_at is null and approval_date is not null " +
                                    $"and status = 'Active' and master_id = {pamm_master_id}; ";
                    Console.WriteLine($"retrieve_pamm_subscribers -- sqlstr: {sqlstr} ");  
                    MySqlCommand select_cmd = new MySqlCommand(sqlstr, sql_conn);
                    using (MySqlDataReader reader = select_cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            object[] localData = { reader.GetInt64(0), reader.GetInt64(1), reader.GetDouble(2), reader.GetInt64(3), pamm_master_ticket  };
                            local_List.Add(localData);
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"An exception occurred: {ex}");
            }
            return(local_List);
        }
        
        private static void insert_subscriber_allocation(long pamm_user_id, long pamm_master_id, long pamm_metalogin, double pamm_lot, double pamm_pnl, double pamm_swap, 
                                                         string pamm_symbol, long pamm_ticket, string pamm_trade_type, DateTime pamm_close_time, double pamm_close_price)
        {
            try
            {
                Console.WriteLine($"");
                List<string> subscriber_trades_List = new List<string>();
                List<string> subscriber_tradehist_List = new List<string>();

                double pamm_profit = pamm_pnl+pamm_swap;
                double subscriber_totalfunds = retrieve_summary_info(pamm_master_id);   // new List<object[]>();
                Console.WriteLine($"insert_subscriber_allocation -- list - total funds: {subscriber_totalfunds}");
                if(subscriber_totalfunds > 0)
                {
                    List<object[]> subscribers_List = retrieve_pamm_subscribers(pamm_master_id, pamm_ticket);
                    List<object[]> subscribers_tradehist_List = subscribers_List;

                    List<object[]> subscribers_exist_List = retrieve_pamm_subscribers_exist(pamm_master_id, pamm_ticket);
                    List<object[]> subscribers_tradehist_exist_List = retrieve_pamm_subscribers_tradehist_exist(pamm_master_id, pamm_ticket); //retrieve_pamm_subscribers_exist(pamm_master_id, pamm_ticket);

                    Console.WriteLine($"subscribers_List.Count: {subscribers_List.Count} - subscribers_exist_List.Count: {subscribers_exist_List.Count}");
                    Console.WriteLine($"subscribers_tradehist_List.Count: {subscribers_List.Count} - subscribers_tradehist_exist_List.Count: {subscribers_exist_List.Count}");

                    subscribers_List = RemoveRecordsWithTickets(subscribers_List, subscribers_exist_List);
                    foreach (var subscribers in subscribers_List)
                    {
                        long subscriber_subs_id = (long) subscribers[0]; 
                        long subscriber_user_id = (long) subscribers[1]; 
                        double subscriber_funds = (double) subscribers[2]; 
                        long subscriber_meta_login = (long)  subscribers[3] != null ? (long)subscribers[3] : 0;
                            
                        double subscriber_pct = (double) (subscriber_funds / subscriber_totalfunds);
                        double subscriber_pct_db = (double) subscriber_pct * 100;
                        double allocate_volume = (double) pamm_lot * subscriber_pct;
                        double allocate_profit = (double) pamm_profit * subscriber_pct;

                        Console.WriteLine($"list - user: {subscriber_user_id}, subs:{subscriber_subs_id}, fund:{subscriber_funds} ");

                        subscriber_trades_List.Add($"( {pamm_master_id}, {pamm_metalogin}, {pamm_lot}, Round({(pamm_profit)},4), ROUND({subscriber_totalfunds},2), {subscriber_user_id}, {subscriber_meta_login}, "+
                        $"{subscriber_subs_id}, ROUND({subscriber_funds},2), ROUND({subscriber_pct_db},5), {pamm_ticket}, '{pamm_symbol}', '{pamm_trade_type}', ROUND({allocate_volume},5), " +
                        $" '{pamm_close_time.ToString("yyyy-MM-dd HH:mm:ss")}', ROUND({allocate_profit},5), '{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}')");
                    }

                    if(subscriber_trades_List.Count > 0)
                    {
                        using (MySqlConnection sql_conn = new MySqlConnection(conn))
                        {
                            sql_conn.Open();
                            string sqlstr = $"INSERT INTO trade_pamm_investor_allocate (master_id, master_meta_login, master_lot, master_pnl, investor_total_funds, user_id, meta_login, subscription_id, "+
                                     $"subscription_funds, subs_pamm_percent, ticket, symbol, trade_type, volume, time_close, trade_profit, created_at) "+
                                     $" VALUES " + string.Join(", ", subscriber_trades_List)+
                                     " ;";
                            Console.WriteLine($"subscriber_trades_List sqlstr: {sqlstr}");
                            MySqlCommand insert_cmd = new MySqlCommand(sqlstr, sql_conn);
                            insert_cmd.ExecuteScalar();
                        }
                    }

                    // ---------------------------------------------------------------------------------------------------------------------------------------------
                    // ---------------------------------------------------------------------------------------------------------------------------------------------

                    subscribers_tradehist_List = RemoveRecordsWithTickets(subscribers_tradehist_List, subscribers_tradehist_exist_List);
                    Console.WriteLine($"subscribers_tradehist_List: {subscribers_tradehist_List.Count}");
                    foreach (var subscribers_tradehist in subscribers_tradehist_List)
                    {
                        long subscriber_user_id = (long) subscribers_tradehist[1]; 
                        long subscriber_meta_login = (long)  subscribers_tradehist[3] != null ? (long)subscribers_tradehist[3] : 0;

                        long subscriber_subs_id = (long) subscribers_tradehist[0]; 
                        double subscriber_funds = (double) subscribers_tradehist[2];     
                        double subscriber_pct = (double) (subscriber_funds / subscriber_totalfunds);
                        double subscriber_pct_db = (double) subscriber_pct * 100;
                        double allocate_volume = (double) pamm_lot * subscriber_pct;
                        double allocate_profit = (double) pamm_profit * subscriber_pct;

                        Console.WriteLine($"list - user: {subscriber_user_id}, subs:{subscriber_subs_id}, fund:{subscriber_funds} ");

                        subscriber_tradehist_List.Add($"( {subscriber_subs_id}, {subscriber_user_id}, {subscriber_meta_login}, '{pamm_symbol}', {pamm_ticket}, '{pamm_close_time.ToString("yyyy-MM-dd HH:mm:ss")}', '{pamm_trade_type}', ROUND({allocate_volume},5), " +
                        $" {pamm_close_price}, '{pamm_close_time.ToString("yyyy-MM-dd HH:mm:ss")}', {pamm_close_price}, 'Closed', ROUND({allocate_profit},5), 'Pending', {pamm_master_id}, 'pamm', '{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}')");
                    }

                    if(subscribers_tradehist_List.Count > 0)
                    {
                        using (MySqlConnection sql_conn = new MySqlConnection(conn))
                        {
                            sql_conn.Open();
                            string sqlstr = $"INSERT INTO trade_histories (subscription_id, user_id, meta_login, symbol, ticket, time_open, trade_type, volume, price_open, time_close, price_close, trade_status, trade_profit, rebate_status, master_id, master_acc_type, created_at) "+
                                     $" VALUES " + string.Join(", ", subscriber_tradehist_List)+
                                     " ;";
                            Console.WriteLine($"subscribers_tradehist_List sqlstr: {sqlstr}");
                            MySqlCommand insert_cmd = new MySqlCommand(sqlstr, sql_conn);
                            insert_cmd.ExecuteScalar();
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"An exception occurred: {ex}");
                //Telegram_Send($"Unhandled exception: {ex.Message}");         
            } 
        }

        public static List<object[]> RemoveRecordsWithTickets(List<object[]> records, List<object[]> ticketsToRemove)
        {   
            //records -- id, user_id, subscription_amount, coalesce(meta_login,0),ticket
            //ticketsToRemove -- user_id, meta_login, ticket
            return records.Where(record => !ticketsToRemove.Any(t => (long)t[0] == (long)record[1] && (long)t[2] == (long)record[4] && (long)t[1] == (long)record[3])).ToList();
        }
        
        private static void insert_master_trades(long pamm_user_id, long pamm_master_id, long pamm_metalogin, double pamm_lot, double pamm_pnl, double pamm_swap, 
                                                 string pamm_symbol, long pamm_ticket, string pamm_trade_type, DateTime pamm_close_time, double pamm_close_price)
        {
            try
            {
                Console.WriteLine($"insert_master_trades ");
                //Console.WriteLine($" ");
                double pamm_profit = pamm_pnl+pamm_swap;
                using (MySqlConnection sql_conn = new MySqlConnection(conn))
                {
                    sql_conn.Open(); // Open the connection
                    
                    /// ------------------------------- into trade_histories
                    long count = 0 ;
                    string sqlstr =  $"SELECT count(*) FROM trade_histories where deleted_at is null and user_id = {pamm_user_id} and ticket = {pamm_ticket};";
                    MySqlCommand select_cmd = new MySqlCommand(sqlstr, sql_conn);
                    object result = select_cmd.ExecuteScalar();
                    if (result != null) { count = Convert.ToInt64(result);  }
                    
                    Console.WriteLine($"insert_cmd exist count: {count}");
                    if(count == 0)
                    {                    
                        sqlstr = $"INSERT INTO trade_histories(master_id, subscription_id, user_id, meta_login, symbol, ticket, "+
                                 $"trade_type, volume, time_close, price_close, trade_profit, trade_status, rebate_status, created_at, master_acc_type ) VALUES ( " +
                                 $"{pamm_master_id}, 0, {pamm_user_id}, {pamm_metalogin}, '{pamm_symbol}', {pamm_ticket}, "+
                                 $"'{pamm_trade_type}', Round({pamm_lot},4), '{pamm_close_time.ToString("yyyy-MM-dd HH:mm:ss")}', " +
                                 $"{pamm_close_price}, Round({pamm_profit},4), 'Closed', 'Approved', '{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}', 'pamm_master'); ";

                        Console.WriteLine($"insert_cmd sqlstr: {sqlstr}");
                        MySqlCommand insert_cmd = new MySqlCommand(sqlstr, sql_conn);
                        insert_cmd.ExecuteScalar();
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"An exception occurred: {ex}");
            }
        }

        private static void retrieve_userid_based_metalogin(long login, ref long user_id, ref long user_rank, ref long user_active)
        {
            try
            {
                using (MySqlConnection sql_conn = new MySqlConnection(conn))
                {
                    sql_conn.Open(); // Open the connection
                    string sqlstr = $"SELECT t1.user_id, t2.setting_rank_id, t2.status FROM trading_accounts t1 inner join users t2 on t1.user_id = t2.id " +
                                    $"where t1.deleted_at is null and t2.deleted_at is null and t1.meta_login = {login}; ";
                    
                    Console.WriteLine($"retrieve_userid_based_metalogin -- sqlstr: {sqlstr} ");  
                    MySqlCommand select_cmd = new MySqlCommand(sqlstr, sql_conn);
                    using (MySqlDataReader reader = select_cmd.ExecuteReader())
                    {
                        while (reader.Read())
                        {
                            user_id = (long)reader.GetInt64(0); 
                            user_rank = (long)reader.GetInt64(1);
                            string status = reader.GetString(2);
                            if (status == "Active") { user_active = 1; } else { user_active = 0; }  
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"An exception occurred: {ex}");
            }
        }

        private static List<string> MT5_trades_to_list_trades(CIMTDealArray Deals)
        {
            List<string> tradesList = new List<string>();
            Console.WriteLine($"Deals: {Deals.Total()}");
            for (uint i = 0; i < Deals.Total(); i++)
            {
                CIMTDeal m_Deal = Deals.Next(i);
                if (m_Deal == null) { break; }

                string Symbol = m_Deal?.Symbol() ?? "";
                ulong Order = m_Deal?.Order() ?? 0;
                double ClosedPrice = m_Deal?.PricePosition() ?? 0;

                if (Symbol.Length > 0 && Order > 0 && ClosedPrice > 0)
                {
                    uint Digits = m_Deal?.Digits() ?? 5;
                    ulong Login = m_Deal?.Login() ?? 0;
                    uint Action = m_Deal?.Action() ?? 0;
                    string Action_str = Action == 0 ? "SELL" : ((Action == 1) ? "BUY" : "");

                    uint Entry = m_Deal?.Entry() ?? 0;
                    long Time = m_Deal?.Time() ?? 0;
                    DateTime DT_Timestamp = DateTimeOffset.FromUnixTimeSeconds(Time).UtcDateTime;
                    double Swap = m_Deal?.Storage() ?? 0;

                    // documents volume is standard and 4 decimal place
                    ulong standard_volume = m_Deal?.Volume() ?? 0;
                    double Volume = (standard_volume > 0) ? ((double)standard_volume / 10000) : 0;
                    double Price = m_Deal?.Price() ?? 0;
                    Price = (double)Math.Round((decimal)Price, (int)Digits);
                    double Profit = m_Deal?.Profit() ?? 0;
                    //double Price_Close = m_Deal?.PricePosition() ?? 0;
                    ClosedPrice = (double)Math.Round((decimal)ClosedPrice, (int)Digits); 
                    ulong Position_ID = m_Deal?.PositionID() ?? 0;

                    //Console.WriteLine($"Order: {Order} - Price: {Price}, ClosedPrice: {ClosedPrice} - Position_ID: {Position_ID} - Swap: {Swap}");
                    // '{time_action.ToString("yyyy-MM-dd HH:mm:ss")}'

                    tradesList.Add($"({Login}, '{Symbol}', {Position_ID}, '{Action_str}', ROUND({Volume},2)," +
                             $" '{DT_Timestamp.ToString("yyyy-MM-dd HH:mm:ss")}', {ClosedPrice}, ROUND({Profit},2), ROUND({Swap},2), '{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}')");
                    
                    //{ Login, Entry, Symbol, Order, dt_Timestamp, Action, volume, Price, Position_ID, PricePosition, Profit};
                    //object[] tradeData = { Login, Entry, Symbol, Order, dt_Timestamp, Action, volume, Price, Position_ID, ClosedPrice, Profit, Swap };
                    //tradesList.Add(tradeData);
                }
                m_Deal.Release();
            }

            return tradesList;
        }
        
        private static void proceed_MT5_PammtradesToDB(ref bool status_code)
        {
            Console.WriteLine($"proceed_MT5_PammtradesToDB .. ");

            DateTime last_date = get_lastdate_from_pamm_tradehist().AddDays(-1); //AddMinutes(-5);
            long server_timestamp = 0;
            API_mManager.TimeServerRequest(out server_timestamp);
            DateTime current_date = DateTimeOffset.FromUnixTimeMilliseconds(server_timestamp).DateTime.AddMinutes(5);

            List<ulong> pamm_accList = get_pamm_accounts();
            ulong[] pamm_accounts = pamm_accList.ToArray();

            // Implement logic for retrieve_trades_fromMT5 
            if (pamm_accList.Count > 0)
            {
                Console.WriteLine("");
                CIMTDealArray PammAcc_Deals = API_mManager.DealCreateArray();
                MTRetCode res1 = API_mManager.DealRequestByLogins(pamm_accounts, SMTTime.FromDateTime(last_date), SMTTime.FromDateTime(current_date), PammAcc_Deals);
                if (res1 == MTRetCode.MT_RET_ERR_NOTFOUND)
                {
                    Console.WriteLine($"PammAcc_Deals - total: 0 -- {MTRetCode.MT_RET_ERR_NOTFOUND}");
                    status_code = true;
                }
                else if (res1 != MTRetCode.MT_RET_OK)
                {
                    Console.WriteLine("PammAcc_Deals total- {0} - flag: {1}", PammAcc_Deals.Total(), res1);
                }
                else
                {

                    //{ Login, Entry, Symbol, Order, dt_Timestamp, Action, volume, Price, Position_ID, PricePosition, Profit};
                    List<string> PammAcc_tradesList = MT5_trades_to_list_trades(PammAcc_Deals);
                    // save to csv file
                    //save_to_csv(tradeAcc_tradesList); // List<object[]>
                    Console.WriteLine("saved PammAcc_Deals : " + PammAcc_Deals.Total());
                    if (PammAcc_Deals.Total() > 0)
                    {
                        using (MySqlConnection sql_conn = new MySqlConnection(conn))
                        {
                            sql_conn.Open();
                            string sqlstr = "";
                            try
                            {
                                sqlstr = $"INSERT INTO trade_pamm_masters_histories (meta_login, symbol, ticket, trade_type, volume, time_close, price_close, " +
                                         $"trade_profit,trade_swap,created_at) VALUES " + string.Join(", ", PammAcc_tradesList)+
                                         " AS new_values ON DUPLICATE KEY UPDATE symbol = new_values.symbol, trade_type = new_values.trade_type, volume = new_values.volume, time_close = new_values.time_close," +
                                         " price_close = new_values.price_close, trade_profit = new_values.trade_profit, trade_swap = new_values.trade_swap;";
                                Console.WriteLine($"sqlstr: {sqlstr}");
                                MySqlCommand insert_cmd = new MySqlCommand(sqlstr, sql_conn);
                                insert_cmd.ExecuteScalar();
                            }
                            catch (Exception ex)
                            {
                                Console.WriteLine($"An exception occurred: {ex}");
                                //Telegram_Send($"Unhandled exception: {ex.Message}");         
                            } 
                        }
                    }
                    status_code = true;
                }
            }
            else
            {
                status_code = true;
            }
        }
        
        private static void save_to_csv( List<object[]> tradeAcc_tradesList )
        {
            var projectDirectory = AppDomain.CurrentDomain.BaseDirectory;
            var csvFilePath = Path.Combine(projectDirectory, "TradingAcc_Deals.csv");

            using (var writer = new StreamWriter(csvFilePath))
            {
                foreach (var item in tradeAcc_tradesList)
                {
                    var line = string.Join(",", item);
                    writer.WriteLine(line);
                }
            }
            Console.WriteLine($"List saved to CSV file at {csvFilePath}");        
        }

        private static List<ulong> get_trading_accounts()
        {
            List<ulong> login_List = new List<ulong>();
            //login_List.Add(457284);
            string sqlstr = "SELECT meta_login FROM trading_accounts where deleted_at is null and user_id in ( SELECT id FROM users WHERE deleted_at is null and status = 'Active' ) and (" +
                            "meta_login not in (SELECT distinct meta_login FROM subscriptions where deleted_at is null and approval_date is not null and status = 'Active') " +
                            "or meta_login not in (select distinct meta_login from masters where deleted_at is null and category not in ('pamm') and status = 'Active') );";

            Console.WriteLine($"get_trading_accounts sqlstr: {sqlstr}");

            using (MySqlConnection sql_conn = new MySqlConnection(conn))
            {
                sql_conn.Open(); // Open the connection
                MySqlCommand select_cmd = new MySqlCommand(sqlstr, sql_conn);
                MySqlDataReader reader = select_cmd.ExecuteReader();

                while (reader.Read())
                {
                    login_List.Add((ulong)reader.GetInt64(0));
                }
                //Console.WriteLine($"ConnectionString: {sql_conn.ConnectionTimeout}");
            }
            return login_List;
        }

        private static List<ulong> get_pamm_accounts()
        {
            List<ulong> login_List = new List<ulong>();
            //login_List.Add(457284);
            string sqlstr = "SELECT meta_login FROM masters where deleted_at is null and category = 'pamm' and status = 'Active' and user_id in ( " +
                            "SELECT id FROM users WHERE deleted_at is null and status = 'Active' ) ;";

            Console.WriteLine($"insert_cmd sqlstr: {sqlstr}");

            using (MySqlConnection sql_conn = new MySqlConnection(conn))
            {
                sql_conn.Open(); // Open the connection
                MySqlCommand select_cmd = new MySqlCommand(sqlstr, sql_conn);
                MySqlDataReader reader = select_cmd.ExecuteReader();

                while (reader.Read())
                {
                    login_List.Add((ulong)reader.GetInt64(0));
                }
                //Console.WriteLine($"ConnectionString: {sql_conn.ConnectionTimeout}");
            }
            return login_List;
        }

        private static DateTime get_lastdate_from_tradehist()
        {
            DateTime last_date = default_time;
            try
            {
                using (MySqlConnection sql_conn = new MySqlConnection(conn))
                {
                    sql_conn.Open(); // Open the connection
                    //string sqlstr = $"SELECT time_close FROM trade_histories order by time_close desc limit 1;";
                    string sqlstr = $"SELECT MIN(frmtime) FROM ( SELECT time_close AS frmtime FROM (SELECT time_close FROM trade_histories ORDER BY time_close DESC LIMIT 1) AS sub1 "+
                                    $"UNION ALL SELECT time_open AS frmtime FROM (SELECT time_open FROM trade_histories WHERE trade_status = 'Opening' ORDER BY time_open ASC LIMIT 1) AS sub2 "+
                                    $") AS combined;";
                    //Console.WriteLine($"sqlstr: {sqlstr}");

                    MySqlCommand select_cmd = new MySqlCommand(sqlstr, sql_conn);
                    object result = select_cmd.ExecuteScalar();
                    if (result != null)
                    {
                        last_date = Convert.ToDateTime(result);
                        //Console.WriteLine($"1. last_date: {last_date}");
                    }
                    //Console.WriteLine($"ConnectionString: {sql_conn.ConnectionTimeout}");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"An exception occurred: {ex}");
                //Telegram_Send($"Unhandled exception: {ex.Message}");         
            }

            return last_date;
        }
        private static DateTime get_lastdate_from_pamm_tradehist()
        {
            DateTime last_date = default_time;
            try
            {
                using (MySqlConnection sql_conn = new MySqlConnection(conn))
                {
                    sql_conn.Open(); // Open the connection
                    string sqlstr = $"SELECT time_close FROM trade_pamm_masters_histories where deleted_at is null ORDER BY time_close DESC LIMIT 1; ";
                    Console.WriteLine($"sqlstr: {sqlstr}");

                    MySqlCommand select_cmd = new MySqlCommand(sqlstr, sql_conn);
                    object result = select_cmd.ExecuteScalar();
                    if (result != null)
                    {
                        last_date = Convert.ToDateTime(result);
                        Console.WriteLine($"1. last_date: {last_date}");
                    }
                    //Console.WriteLine($"ConnectionString: {sql_conn.ConnectionTimeout}");
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"An exception occurred: {ex}");
                //Telegram_Send($"Unhandled exception: {ex.Message}");         
            }

            return last_date;
        }

        private static async Task<bool> Loop_InitializeMT5_PammTrades()
        {
            var taskStopwatch = Stopwatch.StartNew();
            bool isMainMonitoring = true;
            int MainFailures = 0;
            string remarks = "";
            bool task_flag = false;
            
            while (isMainMonitoring)
            {
                bool isMonitoring = true;
                bool isSuccess = false;
                int consecutiveFailures = 0;
                        
                try
                {
                    while (isMonitoring)
                    {
                        //Console.WriteLine($"Check Initialize MetaTrader5: {taskStopwatch.Elapsed.TotalSeconds} seconds ({taskStopwatch.Elapsed.TotalMinutes}");
                        MTRetCode status = MTRetCode.MT_RET_OK;
                        API_mManager = InitializeMetaTrader5API(out remarks, out status);
                        if (status != MTRetCode.MT_RET_OK)
                        {
                            Console.WriteLine($"Initialize failed : {status} - {remarks}");
                            consecutiveFailures++;
                            if (consecutiveFailures >= 30)
                            {
                                // Send a Telegram message
                                //await Telegram_Send("\n" + $"MTRetCode is still {status} in Initialize after 10 minutes of consecutive failures");
                                Console.WriteLine($"MTRetCode is still {status} in Initialize after 10 minutes of consecutive failures");
                                isMonitoring = false; // Exit the loop
                                task_flag = false;
                            }
                        }
                        else
                        {
                            Console.WriteLine($"Check isSuccess: {taskStopwatch.Elapsed.TotalSeconds} seconds ({taskStopwatch.Elapsed.TotalMinutes}");
                            proceed_MT5_PammtradesToDB(ref isSuccess);    //isSuccess = false;

                            if (isSuccess == false) { consecutiveFailures++;  task_flag = false; }
                            if (consecutiveFailures >= 30)
                            {
                                // Send a Telegram message
                                //await Telegram_Send("\n" + $"proceed_MT5_PammtradesToDB trades after 10 minutes of consecutive failures");
                                Console.WriteLine($"proceed_MT5_PammtradesToDB  trades after 10 minutes of consecutive failures");
                                isMonitoring = false; // Exit the loop
                                task_flag = false;
                            }
                            if (isSuccess == true) { isMonitoring = false; task_flag = true; }
                        }
                        
                        if (isMonitoring == true) { task_flag = false; await Task.Delay(10000); }// Wait for 10 seconds before checking again }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"An exception occurred: {ex}");
                }

                if (isMonitoring == false)
                {
                    if (isMonitoring == false && isSuccess == true)
                    {
                        isMainMonitoring = false; task_flag = true;
                    }
                    if (isMonitoring == false && isSuccess == false)
                    {
                        isMonitoring = true; task_flag = false;
                        await Task.Delay(60000 * 5); // Wait for 5 minutes before checking again
                    }

                    MainFailures++;
                    if (MainFailures >= 3)
                    {
                        //await Telegram_Send("\n" + $"proceed_MT5_tradesToDB after 3 times of 10 minutes consecutive failures. Operation STOP, Waiting Instruction from Admin");
                        Console.WriteLine($"proceed_MT5_tradesToDB after 3 times of 10 minutes consecutive failures. Operation STOP, Waiting Future Instruction ");
                        isMainMonitoring = false; task_flag = false;
                    }
                }
                else
                {
                    await Task.Delay(60000 * 5); // Wait for 5 minutes before checking again
                }
            }

            taskStopwatch.Stop();
            Console.WriteLine("");
            Console.WriteLine($"Task Loop_InitializeMT5_PammTrades completed in {taskStopwatch.Elapsed.TotalSeconds} seconds ({taskStopwatch.Elapsed.TotalMinutes})");
                
            return (task_flag);
        }

        private static async Task<bool> Loop_InitializeMT5_CopyTrades()
        {
            var taskStopwatch = Stopwatch.StartNew();
            bool isMainMonitoring = true;
            int MainFailures = 0;
            string remarks = "";
            bool task_flag = false;
            
            while (isMainMonitoring)
            {
                bool isMonitoring = true;
                bool isSuccess = false;
                int consecutiveFailures = 0;
                        
                try
                {
                    while (isMonitoring)
                    {
                        //Console.WriteLine($"Check Initialize MetaTrader5: {taskStopwatch.Elapsed.TotalSeconds} seconds ({taskStopwatch.Elapsed.TotalMinutes}");
                        MTRetCode status = MTRetCode.MT_RET_OK;
                        API_mManager = InitializeMetaTrader5API(out remarks, out status);
                        if (status != MTRetCode.MT_RET_OK)
                        {
                            Console.WriteLine($"Initialize failed : {status} - {remarks}");
                            consecutiveFailures++;
                            if (consecutiveFailures >= 30)
                            {
                                // Send a Telegram message
                                //await Telegram_Send("\n" + $"MTRetCode is still {status} in Initialize after 10 minutes of consecutive failures");
                                Console.WriteLine($"MTRetCode is still {status} in Initialize after 10 minutes of consecutive failures");
                                isMonitoring = false; // Exit the loop
                                task_flag = false;
                            }
                        }
                        else
                        {
                            Console.WriteLine($"Check isSuccess: {taskStopwatch.Elapsed.TotalSeconds} seconds ({taskStopwatch.Elapsed.TotalMinutes}");
                            proceed_MT5_CopytradesToDB(ref isSuccess);    //isSuccess = false;

                            if (isSuccess == false) { consecutiveFailures++;  task_flag = false; }
                            if (consecutiveFailures >= 30)
                            {
                                // Send a Telegram message
                                //await Telegram_Send("\n" + $"proceed_MT5_tradesToDB trades after 10 minutes of consecutive failures");
                                Console.WriteLine($"proceed_MT5_tradesToDB  trades after 10 minutes of consecutive failures");
                                isMonitoring = false; // Exit the loop
                                task_flag = false;
                            }
                            if (isSuccess == true) { isMonitoring = false; task_flag = true; }
                        }
                        
                        if (isMonitoring == true) { task_flag = false; await Task.Delay(10000); }// Wait for 10 seconds before checking again }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"An exception occurred: {ex}");
                }

                if (isMonitoring == false)
                {
                    if (isMonitoring == false && isSuccess == true)
                    {
                        isMainMonitoring = false; task_flag = true;
                    }
                    if (isMonitoring == false && isSuccess == false)
                    {
                        isMonitoring = true; task_flag = false;
                        await Task.Delay(60000 * 5); // Wait for 5 minutes before checking again
                    }

                    MainFailures++;
                    if (MainFailures >= 3)
                    {
                        //await Telegram_Send("\n" + $"proceed_MT5_tradesToDB after 3 times of 10 minutes consecutive failures. Operation STOP, Waiting Instruction from Admin");
                        Console.WriteLine($"proceed_MT5_tradesToDB after 3 times of 10 minutes consecutive failures. Operation STOP, Waiting Future Instruction ");
                        isMainMonitoring = false; task_flag = false;
                    }
                }
                else
                {
                    await Task.Delay(60000 * 5); // Wait for 5 minutes before checking again
                }
            }

            taskStopwatch.Stop();
            Console.WriteLine("");
            Console.WriteLine($"Task Loop_InitializeMT5_CopyTrades completed in {taskStopwatch.Elapsed.TotalSeconds} seconds ({taskStopwatch.Elapsed.TotalMinutes})");
                
            return (task_flag);
        }

        private static async Task<bool> Loop_InitializeMT5_MT5(List<object[]> mt5_list)
        {
            var taskStopwatch = Stopwatch.StartNew();
            bool isMainMonitoring = true;
            int MainFailures = 0;
            string remarks = "";
            bool task_flag = false;
            
            while (isMainMonitoring)
            {
                bool isMonitoring = true;
                bool isSuccess = false;
                int consecutiveFailures = 0;
                        
                try
                {
                    while (isMonitoring)
                    {
                        //Console.WriteLine($"Check Initialize MetaTrader5: {taskStopwatch.Elapsed.TotalSeconds} seconds ({taskStopwatch.Elapsed.TotalMinutes}");
                        MTRetCode status = MTRetCode.MT_RET_OK;
                        API_mManager = InitializeMetaTrader5API(out remarks, out status);
                        if (status != MTRetCode.MT_RET_OK)
                        {
                            Console.WriteLine($"Initialize failed : {status} - {remarks}");
                            consecutiveFailures++;
                            if (consecutiveFailures >= 30)
                            {
                                // Send a Telegram message
                                //await Telegram_Send("\n" + $"MTRetCode is still {status} in Initialize after 10 minutes of consecutive failures");
                                Console.WriteLine($"MTRetCode is still {status} in Initialize after 10 minutes of consecutive failures");
                                isMonitoring = false; // Exit the loop
                                task_flag = false;
                            }
                        }
                        else
                        {
                            Console.WriteLine($"Check isSuccess: {taskStopwatch.Elapsed.TotalSeconds} seconds ({taskStopwatch.Elapsed.TotalMinutes}");
                            DividendPamm_PamInMT5(mt5_list);   //isSuccess = false;

                            if (isSuccess == false) { consecutiveFailures++;  task_flag = false; }
                            if (consecutiveFailures >= 30)
                            {
                                // Send a Telegram message
                                //await Telegram_Send("\n" + $"proceed_MT5_tradesToDB trades after 10 minutes of consecutive failures");
                                Console.WriteLine($"proceed_MT5_tradesToDB  trades after 10 minutes of consecutive failures");
                                isMonitoring = false; // Exit the loop
                                task_flag = false;
                            }
                            if (isSuccess == true) { isMonitoring = false; task_flag = true; }
                        }
                        
                        if (isMonitoring == true) { task_flag = false; await Task.Delay(10000); }// Wait for 10 seconds before checking again }
                    }
                }
                catch (Exception ex)
                {
                    Console.WriteLine($"An exception occurred: {ex}");
                }

                if (isMonitoring == false)
                {
                    if (isMonitoring == false && isSuccess == true)
                    {
                        isMainMonitoring = false; task_flag = true;
                    }
                    if (isMonitoring == false && isSuccess == false)
                    {
                        isMonitoring = true; task_flag = false;
                        await Task.Delay(60000 * 5); // Wait for 5 minutes before checking again
                    }

                    MainFailures++;
                    if (MainFailures >= 3)
                    {
                        //await Telegram_Send("\n" + $"proceed_MT5_tradesToDB after 3 times of 10 minutes consecutive failures. Operation STOP, Waiting Instruction from Admin");
                        Console.WriteLine($"proceed_MT5_tradesToDB after 3 times of 10 minutes consecutive failures. Operation STOP, Waiting Future Instruction ");
                        isMainMonitoring = false; task_flag = false;
                    }
                }
                else
                {
                    await Task.Delay(60000 * 5); // Wait for 5 minutes before checking again
                }
            }

            taskStopwatch.Stop();
            Console.WriteLine("");
            Console.WriteLine($"Task Loop_InitializeMT5_PammTrades completed in {taskStopwatch.Elapsed.TotalSeconds} seconds ({taskStopwatch.Elapsed.TotalMinutes})");
                
            return (task_flag);
        }

        private static string ConvertListToString(List<ulong> list)
        {
            // Use string.Join to concatenate the ulong values with commas
            return string.Join(",", list);
        }

        private static void proceed_MT5_CopytradesToDB(ref bool status_code)
        {
            Console.WriteLine($"proceed_MT5_CopytradesToDB .. ");
            DateTime last_date = get_lastdate_from_tradehist().AddDays(-10);
            long server_timestamp = 0;
            API_mManager.TimeServerRequest(out server_timestamp);
            DateTime current_date = DateTimeOffset.FromUnixTimeMilliseconds(server_timestamp).DateTime.AddDays(1);
            
            List<ulong> trading_accList = get_trading_accounts();
            ulong[] trading_accounts = trading_accList.ToArray();

            // Implement logic for retrieve_trades_fromMT5 
            if (trading_accList.Count > 0)
            {
                //string tradingacc_strlist = ConvertListToString(trading_accList);
                //Console.WriteLine($"tradingacc_strlist: {tradingacc_strlist}");
 
                Console.WriteLine("");
                CIMTDealArray tradingAcc_Deals = API_mManager.DealCreateArray();
                //CIMTDealArray tradingAcc_Deals00 = mManager.DealCreateArray();

                MTRetCode res1 = API_mManager.DealRequestByLogins(trading_accounts, SMTTime.FromDateTime(last_date), SMTTime.FromDateTime(current_date), tradingAcc_Deals);
                if (res1 == MTRetCode.MT_RET_ERR_NOTFOUND)
                {
                    Console.WriteLine($"tradingAcc_Deals - total: 0 -- {MTRetCode.MT_RET_ERR_NOTFOUND}");
                    status_code = true;
                }
                else if (res1 != MTRetCode.MT_RET_OK)
                {
                    Console.WriteLine("tradingAcc_Deals total- {0} - flag: {1}", tradingAcc_Deals.Total(), res1);
                }
                else
                {
                    Console.WriteLine("saved tradingAcc_Deals : " + tradingAcc_Deals);
                    //{ Login, Entry, Symbol, Order, dt_Timestamp, Action, volume, Price, Position_ID, PricePosition, Profit};
                    List<object[]> tradeAcc_tradesList = MT5_trades_to_List_copytrades(tradingAcc_Deals);
                    /* var projectDirectory = AppDomain.CurrentDomain.BaseDirectory;
                    var csvFilePath = Path.Combine(projectDirectory, "TradingAcc_Deals.csv");
                    using (var writer = new StreamWriter(csvFilePath))
                    {
                        foreach (var item in tradeAcc_tradesList)
                        {
                            var line = string.Join(",", item);
                            writer.WriteLine(line);
                        }
                    }
                    Console.WriteLine($"List saved to CSV file at {csvFilePath}"); */
                    
                    tradeAcc_tradesList = tradeAcc_tradesList.OrderBy(arr => (DateTime)arr[4]).ToList();
                    //tradingAcc_Deals.Release();
                    /* foreach (var tradeAcc in tradeAcc_tradesList)
                    {
                        // Deserialize each string back to object[]
                        Console.WriteLine($"Deal: {string.Join(", ", tradeAcc)}");
                    } */
                    insert_updateMT5_into_tradehist(tradeAcc_tradesList);
                    status_code = true;
                }
            }
            else
            {
                status_code = true;
            }
        }

        private static List<object[]> MT5_trades_to_List_copytrades(CIMTDealArray Deals)
        {
            List<object[]> tradesList = new List<object[]>();
            for (uint i = 0; i < Deals.Total(); i++)
            {
                CIMTDeal m_Deal = Deals.Next(i);
                if (m_Deal == null) { break; }

                string Symbol = m_Deal?.Symbol() ?? "";
                ulong Order = m_Deal?.Order() ?? 0;

                if (Symbol.Length > 0 && Order > 0)
                {
                    uint Digits = m_Deal?.Digits() ?? 5;
                    ulong Login = m_Deal?.Login() ?? 0;
                    uint Action = m_Deal?.Action() ?? 0;
                    uint Entry = m_Deal?.Entry() ?? 0;
                    long Time = m_Deal?.Time() ?? 0;
                    DateTime dt_Timestamp = DateTimeOffset.FromUnixTimeSeconds(Time).UtcDateTime;
                    double Swap = m_Deal?.Storage() ?? 0;

                    // documents volume is standard and 4 decimal place
                    ulong standard_volume = m_Deal?.Volume() ?? 0;
                    double volume = (standard_volume > 0) ? ((double)standard_volume / 10000) : 0;
                    double Price = m_Deal?.Price() ?? 0;
                    Price = (double)Math.Round((decimal)Price, (int)Digits);
                    double Profit = m_Deal?.Profit() ?? 0;
                    double PricePosition = m_Deal?.PricePosition() ?? 0;
                    PricePosition = (double)Math.Round((decimal)PricePosition, (int)Digits);
                    double RateProfit = m_Deal?.RateProfit() ?? 0;
                    ulong Position_ID = m_Deal?.PositionID() ?? 0;

                    Console.WriteLine($"Order: {Order} - Price: {Price}, PricePosition: {PricePosition} - Position_ID: {Position_ID} - Swap: {Swap}");
                    //{ Login, Entry, Symbol, Order, dt_Timestamp, Action, volume, Price, Position_ID, PricePosition, Profit};
                    object[] tradeData = { Login, Entry, Symbol, Order, dt_Timestamp, Action, volume, Price, Position_ID, PricePosition, Profit, Swap };
                    tradesList.Add(tradeData);
                }
                m_Deal.Release();
            }
            return tradesList;
        }

        private static void insert_updateMT5_into_tradehist(List<object[]> tradeAccInfo) // action_tkt, ticket, time_close, price_close 
        {
            Console.WriteLine($"insert_update_into_tradehist - tradeAccInfo: {tradeAccInfo.Count}");
            //return ;
            using (MySqlConnection sql_conn = new MySqlConnection(conn))
            {
                sql_conn.Open(); // Open the connection

                //Console.WriteLine($"insert_update_into_tradehist sql_conn open ");
                //{ 0-Login, 1-Entry, 2-Symbol, 3-Order, 4-dt_Timestamp, 5-Action, 6-volume, 7-Price, 8-Position_ID, 9-PricePosition, 10-Profit, 11-Swap};
                foreach (var tradeAcc in tradeAccInfo)
                {
                    string sqlstr = "";
                    // Deserialize each string back to object[]
                    try
                    {
                        ulong meta_login = (ulong)tradeAcc[0];
                        string symbol = (string)tradeAcc[2];
                        ulong deal_ticket = (ulong)tradeAcc[3];
                        DateTime time_action = (DateTime)tradeAcc[4];
                        string action = (uint)tradeAcc[5] == 0 ? "BUY" : (((uint)tradeAcc[5] == 1) ? "SELL" : "");
                        double volume = (double)tradeAcc[6];
                        double price_open = (double)tradeAcc[7];
                        //double price_close = (double)tradeAcc[9];
                        ulong pos_ticket = (ulong)tradeAcc[8];
                        double deal_profit = (double)tradeAcc[10];
                        
                        int count = 0;

                        if (deal_ticket == pos_ticket && action.Length >= 0)
                        {
                            long count0 = -1;
                            sqlstr = $"SELECT count(ticket) FROM trade_histories WHERE deleted_at is null and ticket = {deal_ticket}; ";
                            //Console.WriteLine($"price_open sqlstr: {sqlstr}");
                            MySqlCommand select_cmd = new MySqlCommand(sqlstr, sql_conn);
                            object result = select_cmd.ExecuteScalar();
                            if (result != null) { count0 = Convert.ToInt64(result); }

                            if (count0 == 0)
                            {
                                //Console.WriteLine($"Open Deal: {string.Join(", ", tradeAcc)}"); 
                                sqlstr = $"INSERT INTO trade_histories (subscription_id, meta_login, symbol, ticket, time_open, trade_type, volume, price_open, rebate_status, trade_status, created_at) " +
                                         $"VALUES(0,{meta_login}, '{symbol}' COLLATE utf8mb4_unicode_ci, {deal_ticket}, " +
                                         $"'{time_action.ToString("yyyy-MM-dd HH:mm:ss")}', '{action}', ROUND({volume},2), {price_open}, 'Unclosed', 'Opening', '{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}');";
                                
                                /* if(meta_login == 457347)
                                {
                                    Console.WriteLine($"insert_cmd sqlstr: {sqlstr}");
                                } */
                                MySqlCommand insert_cmd = new MySqlCommand(sqlstr, sql_conn);
                                insert_cmd.ExecuteScalar();
                                count = count + 1;
                                /* if (meta_login == 457286)
                                {
                                    Console.WriteLine($"Open Deal: {string.Join(", ", tradeAcc)}");
                                    Console.WriteLine($"insert_cmd sqlstr: {sqlstr}");
                                } */
                            }

                            if (count0 > 0)
                            {
                                // udpate time_open
                                sqlstr = $"UPDATE trade_histories SET time_open = '{time_action.ToString("yyyy-MM-dd HH:mm:ss")}', updated_at = '{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}' " +
                                        $"WHERE ( time_open not in ('{time_action.ToString("yyyy-MM-dd HH:mm:ss")}') OR time_open is null) and ticket = {deal_ticket} and id > 0";
                                MySqlCommand update2_cmd = new MySqlCommand(sqlstr, sql_conn);
                                update2_cmd.ExecuteScalar();

                                // udpate trade_type
                                sqlstr = $"UPDATE trade_histories SET trade_type = '{action}', updated_at = '{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}' " +
                                        $"WHERE (trade_type not in ('{action}') OR trade_type is null) and ticket = {deal_ticket} and id > 0";
                                MySqlCommand update3_cmd = new MySqlCommand(sqlstr, sql_conn);
                                update3_cmd.ExecuteScalar();

                                // udpate volume
                                sqlstr = $"UPDATE trade_histories SET volume = ROUND({volume},2), updated_at = '{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}' " +
                                        $"WHERE ( volume not in (ROUND({volume},2)) OR volume is null) and ticket = {deal_ticket} and id > 0";
                                MySqlCommand update4_cmd = new MySqlCommand(sqlstr, sql_conn);
                                update4_cmd.ExecuteScalar();

                                // udpate price_open
                                sqlstr = $"UPDATE trade_histories SET price_open = {price_open}, updated_at = '{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}' " +
                                        $"WHERE (price_open not in({price_open})  OR price_open is null) and ticket = {deal_ticket} and id > 0";
                                MySqlCommand update5_cmd = new MySqlCommand(sqlstr, sql_conn);
                                update5_cmd.ExecuteScalar();

                            }
                            //Console.WriteLine($"ConnectionString: {sql_conn.ConnectionTimeout}");
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"An exception occurred: {ex}");
                        //Telegram_Send($"Unhandled exception: {ex.Message}");         
                    }
                }

                foreach (var tradeAcc in tradeAccInfo)
                {
                    string sqlstr = "";
                    // Deserialize each string back to object[]
                    try
                    {
                        ulong meta_login = (ulong)tradeAcc[0];
                        string symbol = (string)tradeAcc[2];
                        ulong deal_ticket = (ulong)tradeAcc[3];
                        DateTime time_action = (DateTime)tradeAcc[4];
                        string action = (uint)tradeAcc[5] == 0 ? "BUY" : (((uint)tradeAcc[5] == 1) ? "SELL" : "");
                        double volume = (double)tradeAcc[6];
                        double price_open = (double)tradeAcc[7];
                        //double price_close = (double)tradeAcc[9];
                        ulong pos_ticket = (ulong)tradeAcc[8];
                        double deal_profit = (double)tradeAcc[10];
                        double deal_swap = (double)tradeAcc[11];

                        // if(meta_login == 457347)
                        // {
                        //     Console.WriteLine($"Close Deal: {string.Join(", ", tradeAcc)}");
                        // }

                        if (deal_ticket != pos_ticket)
                        {
                            //Console.WriteLine($"Close Deal: {string.Join(", ", tradeAcc)}");
                            double price_o = 0; double gain_pct = 0;
                            sqlstr = $"SELECT price_open FROM trade_histories WHERE deleted_at IS NULL AND ticket = {pos_ticket}; ";
                            //Console.WriteLine($"price_open sqlstr: {sqlstr}");
                            MySqlCommand select_cmd = new MySqlCommand(sqlstr, sql_conn);
                            object result = select_cmd.ExecuteScalar();
                            if (result != null) { price_o = Convert.ToDouble(result); }

                            //Console.WriteLine($"price_o: {price_o} - price_close: {price_open}");
                            if (price_o > 0)
                            {
                                gain_pct = (price_open - price_o) / price_o * 100;
                                //Console.WriteLine($"1st gain_pct: {gain_pct}");
                                if (action == "BUY")
                                {
                                    gain_pct = gain_pct * -1;
                                }
                            }

                            sqlstr = $"UPDATE trade_histories SET time_close = '{time_action.ToString("yyyy-MM-dd HH:mm:ss")}', trade_profit = ROUND({deal_profit},2), rebate_status = 'Pending', " +
                                $"trade_status='Closed', price_close = {price_open}, trade_profit_pct = ROUND({gain_pct},2), trade_swap = ROUND({deal_swap},2), updated_at = '{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}' " +
                                $"WHERE ticket = {pos_ticket} AND trade_status = 'Opening' AND id > 0";
                            if(meta_login == 457347)
                            {
                                //Console.WriteLine($"update_cmd sqlstr: {sqlstr}");
                            }
                            MySqlCommand update_cmd = new MySqlCommand(sqlstr, sql_conn);
                            update_cmd.ExecuteScalar();

                            /* if (meta_login == 457286 && pos_ticket == 459814)
                            {
                                Console.WriteLine($"Close Deal: {string.Join(", ", tradeAcc)}");
                                Console.WriteLine($"update_cmd sqlstr: {sqlstr}");
                            } */

                            // update time_close
                            sqlstr = $"UPDATE trade_histories SET time_close = '{time_action.ToString("yyyy-MM-dd HH:mm:ss")}', updated_at = '{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}' " +
                                $"WHERE ticket = {pos_ticket} AND trade_status='Closed' AND time_close not in ('{time_action.ToString("yyyy-MM-dd HH:mm:ss")}') AND id > 0";
                            /* if(meta_login == 457347)
                            {
                                //Console.WriteLine($"update_cmd sqlstr: {sqlstr}");
                            } */
                            MySqlCommand update_cmd1 = new MySqlCommand(sqlstr, sql_conn);
                            update_cmd1.ExecuteScalar();

                            // update time_close
                            sqlstr = $"UPDATE trade_histories SET trade_profit = ROUND({deal_profit},2), price_close = {price_open}, trade_profit_pct = ROUND({gain_pct},2), " +
                                     $"updated_at = '{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}' WHERE ticket = {pos_ticket} AND trade_status='Closed' " +
                                     $" AND ( price_close not in ({price_open}) OR trade_profit not in ({deal_profit}) OR trade_profit_pct not in (ROUND({gain_pct},2)) ) AND id > 0";
                            /* if(meta_login == 457347)
                            {
                                //Console.WriteLine($"trade_profit update_cmd sqlstr: {sqlstr}");
                            } */
                            MySqlCommand update_cmd2 = new MySqlCommand(sqlstr, sql_conn);
                            update_cmd2.ExecuteScalar();

                            // udpate volume
                            sqlstr = $"UPDATE trade_histories SET volume = ROUND({volume},2), updated_at = '{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}' " +
                                     $"WHERE ( volume not in (ROUND({volume},2)) OR volume is null) and ticket = {deal_ticket} AND rebate_status = 'Pending' AND trade_status='Closed' and id > 0";
                            
                            /* if(meta_login == 457347)
                            {
                                //Console.WriteLine($"volume update_cmd sqlstr: {sqlstr}");
                            } */
                            
                            MySqlCommand update3_cmd = new MySqlCommand(sqlstr, sql_conn);
                            update3_cmd.ExecuteScalar();

                            // update time_close
                            sqlstr = $"UPDATE trade_histories SET trade_swap = ROUND({deal_swap},2), updated_at = '{DateTime.Now.ToString("yyyy-MM-dd HH:mm:ss")}' WHERE ticket = {pos_ticket} AND trade_status='Closed' " +
                                     $" AND ( trade_swap not in ({deal_swap}) OR trade_swap is null ) AND id > 0";
                            /* if(meta_login == 457347)
                            {
                                //Console.WriteLine($"trade_profit update_cmd sqlstr: {sqlstr}");
                            } */
                            MySqlCommand update_cmd4 = new MySqlCommand(sqlstr, sql_conn);
                            update_cmd4.ExecuteScalar();

                            //Console.WriteLine($"ConnectionString: {sql_conn.ConnectionTimeout}");
                        }
                    }
                    catch (Exception ex)
                    {
                        Console.WriteLine($"An exception occurred: {ex}");
                        //Telegram_Send($"Unhandled exception: {ex.Message}");         
                    }
                }
            }
        }
        /* private static async Task Telegram_Send(string messages)
        {
            //Console.WriteLine("Enter Telegram_Send - "+messages);
            string telegramApiToken_0 = telegramApiToken;
            long chatId_0 = chatId;
            var botClient = new TelegramBotClient(telegramApiToken_0);

            Console.WriteLine(" Telegram_Send " + (title_name + messages));
            try
            {
                await botClient.SendTextMessageAsync(chatId_0, (title_name + messages));
                Console.WriteLine("Message sent successfully.");
            }
            catch (Exception ex)
            {
                Console.WriteLine("Error sending message: " + ex.Message);
            }
        } */

        private static CIMTManagerAPI InitializeMetaTrader5API(out string str, out MTRetCode res)
        {
            str = "";
            CIMTManagerAPI API_mManager = null;
            res = MTRetCode.MT_RET_OK_NONE;
            try
            {
                MTRetCode ret = MTRetCode.MT_RET_OK_NONE;
                res = SMTManagerAPIFactory.Initialize(@"Libs\'MetaQuotes.MT5ManagerAPI64.dll"); ;
                if (res != MTRetCode.MT_RET_OK)
                {
                    str = string.Format("Initialize error ({0})", ret);
                    return API_mManager;
                }
                //Console.WriteLine($"Part 2"); 
                API_mManager = SMTManagerAPIFactory.CreateManager(SMTManagerAPIFactory.ManagerAPIVersion, out ret);
                if (ret == MTRetCode.MT_RET_OK && API_mManager != null)
                {
                    Console.WriteLine($"SMTManagerAPIFactory.CreateManager : " + ret);
                    res = API_mManager.Connect(serverName, adminLogin, adminPassword, null, CIMTManagerAPI.EnPumpModes.PUMP_MODE_FULL, MT5_CONNECT_TIMEOUT);
                    Console.WriteLine($"SMTManagerAPIFactory.CreateManager : " + res);

                    if (res != MTRetCode.MT_RET_OK)
                    {
                        str = string.Format("UserAccountRequest error ({0})", res);
                        //LogOut(EnMTLogCode.MTLogErr,server,"Connection by Managed API to {0} failed: {1}",serverName,res); 
                        return API_mManager;
                    }
                }
            }
            catch (Exception ex)
            {
                Console.WriteLine($"An exception occurred: {ex}");
            }
            return API_mManager;
        }

        private static async Task<string> AwaitConsoleReadLine(int timeoutms)
        {
            Task<string> readLineTask = Task.Run(() => Console.ReadLine());

            if (await Task.WhenAny(readLineTask, Task.Delay(timeoutms)) == readLineTask)
            {
                return readLineTask.Result;
            }
            else
            {
                Console.WriteLine("Timeout!");
                return null;
            }
        }
    }

}