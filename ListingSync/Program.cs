using Google.Protobuf.WellKnownTypes;
using Microsoft.Extensions.Configuration;
using MySql.Data.MySqlClient;
using MySqlX.XDevAPI;
using MySqlX.XDevAPI.Relational;
using Newtonsoft.Json;
using Newtonsoft.Json.Linq;
using Org.BouncyCastle.Pqc.Crypto.Lms;
using ListingSync.Models;
using RestSharp;
using RestSharp.Authenticators;
using System.Collections.Generic;
using System.Net.Http.Headers;
using System.Xml;

namespace ListingSync
{
    internal class Program
    {
        //static string limit_search = "2";

        static async Task Main(string[] args)
        {
            try
            {
                IConfigurationRoot config = new ConfigurationBuilder().AddJsonFile("appconfig.json", optional: false).Build();

                string? mysql_connection_string = config.GetConnectionString("mysql");
                string? rets_base_url = config.GetSection("AppIdentitySettings").GetSection("rets_base_url").Value;
                string? rets_username = config.GetSection("AppIdentitySettings").GetSection("rets_username").Value;
                string? rets_password = config.GetSection("AppIdentitySettings").GetSection("rets_password").Value;
                string? rets_version = config.GetSection("AppIdentitySettings").GetSection("rets_version").Value;
                string? rets_image_path = config.GetSection("AppIdentitySettings").GetSection("rets_image_path").Value;

                if (String.IsNullOrWhiteSpace(mysql_connection_string))
                {
                    Log_Messages("\n\n************An Error Occured************", "Empty Connection String\n");
                }
                else if (String.IsNullOrWhiteSpace(rets_base_url))
                {
                    Log_Messages("\n\n************An Error Occured************", "Empty RETS Base URL\n");
                }
                else if (String.IsNullOrWhiteSpace(rets_username))
                {
                    Log_Messages("\n\n************An Error Occured************", "Empty RETS Username\n");
                }
                else if (String.IsNullOrWhiteSpace(rets_password))
                {
                    Log_Messages("\n\n************An Error Occured************", "Empty RETS Password\n");
                }
                else if (String.IsNullOrWhiteSpace(rets_version))
                {
                    Log_Messages("\n\n************An Error Occured************", "Empty RETS Version\n");
                }
                else if (String.IsNullOrWhiteSpace(rets_image_path))
                {
                    Log_Messages("\n\n************An Error Occured************", "Empty RETS Image Path\n");
                }
                else if (!Directory.Exists(rets_image_path))
                {
                    Log_Messages("\n\n************An Error Occured************", "RETS Image Path Doesn't Exists\n");
                }
                else
                {
                    Log_Question("How many days(s) do you want to search: ");

                    int? days_to_be_search = Convert_String_To_Integer(Console.ReadLine());

                    if (days_to_be_search == null)
                    {
                        Log_Messages("\n\n************An Error Occured************", "Invalid Days(s) Value\n");
                    }
                    else if (days_to_be_search.Value <= 0)
                    {
                        Log_Messages("\n\n************An Error Occured************", "Minimum Day(s) value is 1 Day\n");
                    }
                    else
                    {
                        days_to_be_search = days_to_be_search.Value * -1;

                        bool listing_table_exists = await Check_Listing_Table(mysql_connection_string);

                        if (listing_table_exists == true)
                        {
                            string? login_response_cookies = await RETS_Login(rets_base_url, rets_username, rets_password, rets_version);

                            if (!String.IsNullOrWhiteSpace(login_response_cookies))
                            {
                                DateTime current_time = DateTime.Now;

                                JArray? residential_response = await RETS_Search_Residents(rets_base_url, rets_username, rets_password, rets_version, days_to_be_search.Value, current_time, login_response_cookies);

                                if (residential_response != null)
                                {
                                    Log_Messages("Property Count: " + residential_response.Count, null);

                                    await Populate_Listing_Table(mysql_connection_string, residential_response);
                                }

                                JArray? condo_response = await RETS_Search_Condos(rets_base_url, rets_username, rets_password, rets_version, days_to_be_search.Value, current_time, login_response_cookies);

                                if (condo_response != null)
                                {
                                    Log_Messages("Property Count: " + condo_response.Count, null);

                                    await Populate_Listing_Table(mysql_connection_string, condo_response);
                                }

                                JArray? commercial_response = await RETS_Search_Commercials(rets_base_url, rets_username, rets_password, rets_version, days_to_be_search.Value, current_time, login_response_cookies);

                                if (commercial_response != null)
                                {
                                    Log_Messages("Property Count: " + commercial_response.Count, null);

                                    await Populate_Listing_Table(mysql_connection_string, commercial_response);
                                }

                                List<string>? image_download_pending_mls_list = await Get_Pending_Image_Download_MLS(mysql_connection_string);

                                if (image_download_pending_mls_list != null)
                                {
                                    foreach(string mls in image_download_pending_mls_list)
                                    {
                                        await RETS_Get_Images(rets_base_url, rets_username, rets_password, rets_version, mls, login_response_cookies, rets_image_path, mysql_connection_string);
                                    }
                                }

                                await RETS_Logout(rets_base_url, rets_username, rets_password, rets_version, login_response_cookies);
                            }
                        }
                    }
                }
            }
            catch (Exception ex)
            {
                Log_Messages("\n\n************An Error Occured************", ex.ToString() + "\n");
            }
        }

        private static async Task<bool> Check_Listing_Table(string ConnectionString)
        {
            Log_Messages("\n\n************Checking Listing Table************", null);

            try
            {
                using MySqlConnection connection = new(ConnectionString);

                await connection.OpenAsync();

                using MySqlCommand command = new("select case when exists((select * from information_schema.tables where table_name = 'listing')) then 1 else 0 end", connection);

                bool exists = ((long?)await command.ExecuteScalarAsync()) == 1;

                await command.DisposeAsync();

                if (exists == true)
                {
                    Log_Messages("Listing Table Exists", null);
                }
                else
                {
                    exists = await Create_Listing_Table(connection);
                }

                await connection.CloseAsync();
                await connection.DisposeAsync();

                return exists;
            }
            catch(Exception ex)
            {
                Log_Messages("Checking Listing Table Failed. Error:", ex.ToString() + "\n");
                return false;
            }
        }

        private static async Task<bool> Create_Listing_Table(MySqlConnection Connection)
        {
            try
            {
                string query = @"CREATE TABLE `listing` (`prop_id` bigint(20) NOT NULL AUTO_INCREMENT,`Acres` varchar(8) DEFAULT NULL,`Ad_text` varchar(500) DEFAULT NULL,`Addl_mo_fee` varchar(20) DEFAULT NULL,`Addr` varchar(35) DEFAULT NULL,`All_inc` varchar(20) DEFAULT NULL,`Apt_num` varchar(20) DEFAULT NULL,`Ass_year` varchar(20) DEFAULT NULL,`Bath_tot` varchar(20) DEFAULT NULL,`Br` varchar(20) DEFAULT NULL,`Br_plus` varchar(20) DEFAULT NULL,`Bsmt1_out` varchar(20) DEFAULT NULL,`Bsmt2_out` varchar(20) DEFAULT NULL,`Cable` varchar(20) DEFAULT NULL,`Cac_inc` varchar(20) DEFAULT NULL,`Cd` varchar(25) DEFAULT NULL,`Central_vac` varchar(20) DEFAULT NULL,`Cndsold_xd` varchar(25) DEFAULT NULL,`Com_coopb` varchar(40) DEFAULT NULL,`Comel_inc` varchar(20) DEFAULT NULL,`Comp_pts` varchar(20) DEFAULT NULL,`Cond` varchar(24) DEFAULT NULL,`Constr1_out` varchar(20) DEFAULT NULL,`Constr2_out` varchar(20) DEFAULT NULL,`County` varchar(20) DEFAULT NULL,`Cross_st` varchar(30) DEFAULT NULL,`Den_fr` varchar(20) DEFAULT NULL,`Depth` varchar(20) DEFAULT NULL,`Disp_addr` varchar(20) DEFAULT NULL,`Dom` varchar(20) DEFAULT NULL,`Drive` varchar(20) DEFAULT NULL,`Dt_sus` varchar(25) DEFAULT NULL,`Dt_ter` varchar(25) DEFAULT NULL,`Elec` varchar(20) DEFAULT NULL,`Elevator` varchar(20) DEFAULT NULL,`Extras` varchar(250) DEFAULT NULL,`Farm_agri` varchar(20) DEFAULT NULL,`Fpl_num` varchar(20) DEFAULT NULL,`Front_ft` varchar(20) DEFAULT NULL,`Fuel` varchar(20) DEFAULT NULL,`Furnished` varchar(20) DEFAULT NULL,`Gar_spaces` varchar(20) DEFAULT NULL,`Gar_type` varchar(20) DEFAULT NULL,`Gas` varchar(20) DEFAULT NULL,`Heat_inc` varchar(20) DEFAULT NULL,`Heating` varchar(20) DEFAULT NULL,`Hydro_inc` varchar(20) DEFAULT NULL,`Input_date` varchar(25) DEFAULT NULL,`Internet` varchar(20) DEFAULT NULL,`Irreg` varchar(40) DEFAULT NULL,`Kit_plus` varchar(20) DEFAULT NULL,`Laundry` varchar(20) DEFAULT NULL,`Laundry_lev` varchar(20) DEFAULT NULL,`Ld` varchar(25) DEFAULT NULL,`Lease` varchar(20) DEFAULT NULL,`Lease_term` varchar(20) DEFAULT NULL,`Legal_desc` varchar(50) DEFAULT NULL,`Level1` varchar(8) DEFAULT NULL,`Level10` varchar(8) DEFAULT NULL,`Level11` varchar(8) DEFAULT NULL,`Level12` varchar(8) DEFAULT NULL,`Level2` varchar(8) DEFAULT NULL,`Level3` varchar(8) DEFAULT NULL,`Level4` varchar(8) DEFAULT NULL,`Level5` varchar(8) DEFAULT NULL,`Level6` varchar(8) DEFAULT NULL,`Level7` varchar(8) DEFAULT NULL,`Level8` varchar(8) DEFAULT NULL,`Level9` varchar(8) DEFAULT NULL,`Lot_fr_inc` varchar(20) DEFAULT NULL,`Lotsz_code` varchar(20) DEFAULT NULL,`Lp_dol` varchar(20) DEFAULT NULL,`Lsc` varchar(20) DEFAULT NULL,`Lse_terms` varchar(20) DEFAULT NULL,`Ml_num` varchar(20) DEFAULT NULL,`Mmap_col` varchar(20) DEFAULT NULL,`Mmap_page` varchar(20) DEFAULT NULL,`Mmap_row` varchar(20) DEFAULT NULL,`Num_kit` varchar(20) DEFAULT NULL,`Occ` varchar(20) DEFAULT NULL,`Oh_date` varchar(25) DEFAULT NULL,`Orig_dol` varchar(20) DEFAULT NULL,`Oth_struc1_out` varchar(20) DEFAULT NULL,`Oth_struc2_out` varchar(20) DEFAULT NULL,`Outof_area` varchar(20) DEFAULT NULL,`Parcel_id` varchar(20) DEFAULT NULL,`Park_chgs` varchar(20) DEFAULT NULL,`Park_spcs` varchar(20) DEFAULT NULL,`Pay_freq` varchar(20) DEFAULT NULL,`Perc_dif` varchar(20) DEFAULT NULL,`Pool` varchar(20) DEFAULT NULL,`Pr_lsc` varchar(20) DEFAULT NULL,`Prkg_inc` varchar(20) DEFAULT NULL,`Prop_feat1_out` varchar(20) DEFAULT NULL,`Prop_feat2_out` varchar(20) DEFAULT NULL,`Prop_feat3_out` varchar(20) DEFAULT NULL,`Prop_feat4_out` varchar(20) DEFAULT NULL,`Prop_feat5_out` varchar(20) DEFAULT NULL,`Prop_feat6_out` varchar(20) DEFAULT NULL,`Prop_mgmt` varchar(60) DEFAULT NULL,`Pvt_ent` varchar(20) DEFAULT NULL,`Retirement` varchar(20) DEFAULT NULL,`Rltr` varchar(80) DEFAULT NULL,`Rm1_dc1_out` varchar(20) DEFAULT NULL,`Rm1_dc2_out` varchar(20) DEFAULT NULL,`Rm1_dc3_out` varchar(20) DEFAULT NULL,`Rm1_len` varchar(20) DEFAULT NULL,`Rm1_out` varchar(20) DEFAULT NULL,`Rm1_wth` varchar(20) DEFAULT NULL,`Rm10_dc1_out` varchar(20) DEFAULT NULL,`Rm10_dc2_out` varchar(20) DEFAULT NULL,`Rm10_dc3_out` varchar(20) DEFAULT NULL,`Rm10_len` varchar(20) DEFAULT NULL,`Rm10_out` varchar(20) DEFAULT NULL,`Rm10_wth` varchar(20) DEFAULT NULL,`Rm11_dc1_out` varchar(20) DEFAULT NULL,`Rm11_dc2_out` varchar(20) DEFAULT NULL,`Rm11_dc3_out` varchar(20) DEFAULT NULL,`Rm11_len` varchar(20) DEFAULT NULL,`Rm11_out` varchar(20) DEFAULT NULL,`Rm11_wth` varchar(20) DEFAULT NULL,`Rm12_dc1_out` varchar(20) DEFAULT NULL,`Rm12_dc2_out` varchar(20) DEFAULT NULL,`Rm12_dc3_out` varchar(20) DEFAULT NULL,`Rm12_len` varchar(20) DEFAULT NULL,`Rm12_out` varchar(20) DEFAULT NULL,`Rm12_wth` varchar(20) DEFAULT NULL,`Rm2_dc1_out` varchar(20) DEFAULT NULL,`Rm2_dc2_out` varchar(20) DEFAULT NULL,`Rm2_dc3_out` varchar(20) DEFAULT NULL,`Rm2_len` varchar(20) DEFAULT NULL,`Rm2_out` varchar(20) DEFAULT NULL,`Rm2_wth` varchar(20) DEFAULT NULL,`Rm3_dc1_out` varchar(20) DEFAULT NULL,`Rm3_dc2_out` varchar(20) DEFAULT NULL,`Rm3_dc3_out` varchar(20) DEFAULT NULL,`Rm3_len` varchar(20) DEFAULT NULL,`Rm3_out` varchar(20) DEFAULT NULL,`Rm3_wth` varchar(20) DEFAULT NULL,`Rm4_dc1_out` varchar(20) DEFAULT NULL,`Rm4_dc2_out` varchar(20) DEFAULT NULL,`Rm4_dc3_out` varchar(20) DEFAULT NULL,`Rm4_len` varchar(20) DEFAULT NULL,`Rm4_out` varchar(20) DEFAULT NULL,`Rm4_wth` varchar(20) DEFAULT NULL,`Rm5_dc1_out` varchar(20) DEFAULT NULL,`Rm5_dc2_out` varchar(20) DEFAULT NULL,`Rm5_dc3_out` varchar(20) DEFAULT NULL,`Rm5_len` varchar(20) DEFAULT NULL,`Rm5_out` varchar(20) DEFAULT NULL,`Rm5_wth` varchar(20) DEFAULT NULL,`Rm6_dc1_out` varchar(20) DEFAULT NULL,`Rm6_dc2_out` varchar(20) DEFAULT NULL,`Rm6_dc3_out` varchar(20) DEFAULT NULL,`Rm6_len` varchar(20) DEFAULT NULL,`Rm6_out` varchar(20) DEFAULT NULL,`Rm6_wth` varchar(20) DEFAULT NULL,`Rm7_dc1_out` varchar(20) DEFAULT NULL,`Rm7_dc2_out` varchar(20) DEFAULT NULL,`Rm7_dc3_out` varchar(20) DEFAULT NULL,`Rm7_len` varchar(20) DEFAULT NULL,`Rm7_out` varchar(20) DEFAULT NULL,`Rm7_wth` varchar(20) DEFAULT NULL,`Rm8_dc1_out` varchar(20) DEFAULT NULL,`Rm8_dc2_out` varchar(20) DEFAULT NULL,`Rm8_dc3_out` varchar(20) DEFAULT NULL,`Rm8_len` varchar(20) DEFAULT NULL,`Rm8_out` varchar(20) DEFAULT NULL,`Rm8_wth` varchar(20) DEFAULT NULL,`Rm9_dc1_out` varchar(20) DEFAULT NULL,`Rm9_dc2_out` varchar(20) DEFAULT NULL,`Rm9_dc3_out` varchar(20) DEFAULT NULL,`Rm9_len` varchar(20) DEFAULT NULL,`Rm9_out` varchar(20) DEFAULT NULL,`Rm9_wth` varchar(20) DEFAULT NULL,`Rms` varchar(20) DEFAULT NULL,`Rooms_plus` varchar(20) DEFAULT NULL,`S_r` varchar(20) DEFAULT NULL,`Sewer` varchar(20) DEFAULT NULL,`Sp_dol` varchar(20) DEFAULT NULL,`Spec_des1_out` varchar(27) DEFAULT NULL,`Spec_des2_out` varchar(27) DEFAULT NULL,`Spec_des3_out` varchar(27) DEFAULT NULL,`Spec_des4_out` varchar(27) DEFAULT NULL,`Spec_des5_out` varchar(27) DEFAULT NULL,`Spec_des6_out` varchar(27) DEFAULT NULL,`Sqft` varchar(20) DEFAULT NULL,`St` varchar(20) DEFAULT NULL,`St_dir` varchar(20) DEFAULT NULL,`St_num` varchar(20) DEFAULT NULL,`St_sfx` varchar(20) DEFAULT NULL,`Status` varchar(20) DEFAULT NULL,`Style` varchar(20) DEFAULT NULL,`Taxes` varchar(20) DEFAULT NULL,`Td` varchar(25) DEFAULT NULL,`Tour_url` varchar(100) DEFAULT NULL,`Community_code` varchar(20) DEFAULT NULL,`Area_code` varchar(20) DEFAULT NULL,`Tv` varchar(20) DEFAULT NULL,`Type_own_srch` varchar(20) DEFAULT NULL,`Type_own1_out_1` varchar(20) DEFAULT NULL,`Uffi` varchar(20) DEFAULT NULL,`Unavail_dt` varchar(25) DEFAULT NULL,`Util_cable` varchar(20) DEFAULT NULL,`Util_tel` varchar(20) DEFAULT NULL,`Vend_pis` varchar(20) DEFAULT NULL,`Vtour_updt` varchar(25) DEFAULT NULL,`Water` varchar(20) DEFAULT NULL,`Water_inc` varchar(20) DEFAULT NULL,`Waterfront` varchar(20) DEFAULT NULL,`Wcloset_p1` varchar(20) DEFAULT NULL,`Wcloset_p2` varchar(20) DEFAULT NULL,`Wcloset_p3` varchar(20) DEFAULT NULL,`Wcloset_p4` varchar(20) DEFAULT NULL,`Wcloset_p5` varchar(20) DEFAULT NULL,`Wcloset_t1` varchar(20) DEFAULT NULL,`Wcloset_t1lvl` varchar(20) DEFAULT NULL,`Wcloset_t2` varchar(20) DEFAULT NULL,`Wcloset_t2lvl` varchar(20) DEFAULT NULL,`Wcloset_t3` varchar(20) DEFAULT NULL,`Wcloset_t3lvl` varchar(20) DEFAULT NULL,`Wcloset_t4` varchar(20) DEFAULT NULL,`Wcloset_t4lvl` varchar(20) DEFAULT NULL,`Wcloset_t5` varchar(20) DEFAULT NULL,`Wcloset_t5lvl` varchar(20) DEFAULT NULL,`Wtr_suptyp` varchar(20) DEFAULT NULL,`Xd` varchar(25) DEFAULT NULL,`Xdtd` varchar(25) DEFAULT NULL,`Yr` varchar(20) DEFAULT NULL,`Yr_built` varchar(20) DEFAULT NULL,`Zip` varchar(20) DEFAULT NULL,`Zoning` varchar(40) DEFAULT NULL,`Timestamp_sql` varchar(25) DEFAULT NULL,`Municipality_code` varchar(20) DEFAULT NULL,`Area` varchar(40) DEFAULT NULL,`Community` varchar(44) DEFAULT NULL,`Cert_lvl` varchar(25) DEFAULT NULL,`Energy_cert` varchar(20) DEFAULT NULL,`Handi_equipped` varchar(20) DEFAULT NULL,`Municipality_district` varchar(44) DEFAULT NULL,`Municipality` varchar(40) DEFAULT NULL,`Pix_updt` varchar(25) DEFAULT NULL,`Oh_date1` varchar(25) DEFAULT NULL,`Oh_date2` varchar(25) DEFAULT NULL,`Oh_date3` varchar(25) DEFAULT NULL,`Oh_dt_stamp` varchar(25) DEFAULT NULL,`Oh_to1` varchar(20) DEFAULT NULL,`Oh_to2` varchar(20) DEFAULT NULL,`Oh_to3` varchar(20) DEFAULT NULL,`Oh_from1` varchar(20) DEFAULT NULL,`Oh_from2` varchar(20) DEFAULT NULL,`Oh_from3` varchar(20) DEFAULT NULL,`A_c` varchar(20) DEFAULT NULL,`Green_pis` varchar(20) DEFAULT NULL,`Water_body` varchar(20) DEFAULT NULL,`Water_type` varchar(20) DEFAULT NULL,`Water_front` varchar(20) DEFAULT NULL,`Access_prop1` varchar(20) DEFAULT NULL,`Access_prop2` varchar(20) DEFAULT NULL,`Water_feat1` varchar(20) DEFAULT NULL,`Water_feat2` varchar(20) DEFAULT NULL,`Water_feat3` varchar(20) DEFAULT NULL,`Water_feat4` varchar(20) DEFAULT NULL,`Water_feat5` varchar(20) DEFAULT NULL,`Shoreline1` varchar(20) DEFAULT NULL,`Shoreline2` varchar(20) DEFAULT NULL,`Shore_allow` varchar(20) DEFAULT NULL,`Shoreline_exp` varchar(20) DEFAULT NULL,`Alt_power1` varchar(20) DEFAULT NULL,`Alt_power2` varchar(20) DEFAULT NULL,`Easement_rest1` varchar(20) DEFAULT NULL,`Easement_rest2` varchar(20) DEFAULT NULL,`Easement_rest3` varchar(20) DEFAULT NULL,`Easement_rest4` varchar(20) DEFAULT NULL,`Rural_svc1` varchar(20) DEFAULT NULL,`Rural_svc2` varchar(20) DEFAULT NULL,`Rural_svc3` varchar(20) DEFAULT NULL,`Rural_svc4` varchar(20) DEFAULT NULL,`Rural_svc5` varchar(20) DEFAULT NULL,`Water_acc_bldg1` varchar(20) DEFAULT NULL,`Water_acc_bldg2` varchar(20) DEFAULT NULL,`Water_del_feat1` varchar(20) DEFAULT NULL,`Water_del_feat2` varchar(20) DEFAULT NULL,`Sewage1` varchar(20) DEFAULT NULL,`Sewage2` varchar(20) DEFAULT NULL,`Potl` varchar(20) DEFAULT NULL,`Tot_park_spcs` varchar(20) DEFAULT NULL,`Link_yn` varchar(20) DEFAULT NULL,`Link_Comment` varchar(30) DEFAULT NULL,`Bldg_amen1_out` varchar(27) DEFAULT NULL,`Bldg_amen2_out` varchar(27) DEFAULT NULL,`Bldg_amen3_out` varchar(27) DEFAULT NULL,`Bldg_amen4_out` varchar(27) DEFAULT NULL,`Bldg_amen5_out` varchar(27) DEFAULT NULL,`Bldg_amen6_out` varchar(27) DEFAULT NULL,`Cond_txinc` varchar(10) DEFAULT NULL,`Condo_corp` varchar(10) DEFAULT NULL,`Condo_exp` varchar(10) DEFAULT NULL,`Corp_num` varchar(10) DEFAULT NULL,`Ens_lndry` varchar(10) DEFAULT NULL,`Gar` varchar(10) DEFAULT NULL,`Insur_bldg` varchar(10) DEFAULT NULL,`Locker` varchar(17) DEFAULT NULL,`Locker_lev_unit` varchar(3) DEFAULT NULL,`Locker_num` varchar(10) DEFAULT NULL,`Locker_unit` varchar(4) DEFAULT NULL,`Maint` varchar(10) DEFAULT NULL,`Park_desig` varchar(10) DEFAULT NULL,`Park_desig_2` varchar(10) DEFAULT NULL,`Park_fac` varchar(10) DEFAULT NULL,`Park_lgl_desc1` varchar(15) DEFAULT NULL,`Park_lgl_desc2` varchar(15) DEFAULT NULL,`Park_spc1` varchar(10) DEFAULT NULL,`Park_spc2` varchar(10) DEFAULT NULL,`Patio_ter` varchar(10) DEFAULT NULL,`Pets` varchar(10) DEFAULT NULL,`Secgrd_sys` varchar(10) DEFAULT NULL,`Share_perc` varchar(10) DEFAULT NULL,`Stories` varchar(10) DEFAULT NULL,`Unit_num` varchar(10) DEFAULT NULL,`Poss_date` varchar(27) DEFAULT NULL,`Type_own1_out` varchar(20) DEFAULT NULL,`prop_type` tinyint(4) DEFAULT NULL,`let_type` tinyint(4) DEFAULT NULL,`latitude` decimal(10,6) NOT NULL DEFAULT '0.000000',`longitude` decimal(10,6) NOT NULL DEFAULT '0.000000',`slug` varchar(255) DEFAULT NULL,`IsImageDownloaded` int NOT NULL DEFAULT 0,PRIMARY KEY (`prop_id`),UNIQUE KEY `Ml_num` (`Ml_num`),KEY `Addr_Zip` (`Addr`,`Zip`)) ENGINE=InnoDB AUTO_INCREMENT=4558195 DEFAULT CHARSET=latin1;";
                
                MySqlCommand command = new(query, Connection);

                await command.ExecuteNonQueryAsync();
                await command.DisposeAsync();

                Log_Messages("Listing Table Created", null);
                return true;
            }
            catch (Exception ex)
            {
                Log_Messages("Creating Listing Table Failed. Error:", ex.ToString() + "\n");
                return false;
            }
        }

        private static async Task Populate_Listing_Table(string ConnectionString, JArray Data)
        {
            try
            {
                using MySqlConnection connection = new(ConnectionString);

                await connection.OpenAsync();

                int totalRows = 0;

                foreach (JToken item in Data)
                {
                    JToken? listing = item["Listing"];

                    if (listing != null)
                    {
                        FieldSeperatorModel[] fields = Create_Field_Connection(listing);

                        string? columns = null, values = null, update = null;

                        foreach (FieldSeperatorModel field in fields)
                        {
                            if (!String.IsNullOrWhiteSpace(field.Value))
                            {
                                if (String.IsNullOrWhiteSpace(columns))
                                {
                                    columns = "`" + field.Database + "`";
                                }
                                else
                                {
                                    columns += ", `" + field.Database + "`";
                                }

                                if (String.IsNullOrWhiteSpace(values))
                                {
                                    values = "@" + field.Database;
                                }
                                else
                                {
                                    values += ", @" + field.Database;
                                }

                                if (String.IsNullOrWhiteSpace(update))
                                {
                                    update = "`" + field.Database + "`=@" + field.Database;
                                }
                                else
                                {
                                    update += ", `" + field.Database + "`=@" + field.Database;
                                }
                            }
                        }

                        using MySqlCommand command = new("INSERT INTO listing (" + columns + ") VALUES (" + values + ") on duplicate key update " + update, connection);

                        foreach (FieldSeperatorModel field in fields)
                        {
                            if (!String.IsNullOrWhiteSpace(field.Value))
                            {
                                command.Parameters.AddWithValue("@" + field.Database, field.Value);
                            }
                        }

                        totalRows += await command.ExecuteNonQueryAsync();

                        await command.DisposeAsync();
                    }
                }

                Log_Messages("Populating Listing Table Rows: " + totalRows, null);

                await connection.CloseAsync();
                await connection.DisposeAsync();
            }
            catch (Exception ex)
            {
                Log_Messages("Populating Listing Table Failed. Error:", ex.ToString() + "\n");
            }
        }

        private static async Task<List<string>?> Get_Pending_Image_Download_MLS(string ConnectionString)
        {
            Log_Messages("\n\n************Getting MLS Of Pending Image Downloads************", null);

            try
            {
                using MySqlConnection connection = new(ConnectionString);
                await connection.OpenAsync();

                using MySqlCommand command = new("SELECT Ml_num FROM listing WHERE IsImageDownloaded=0", connection);

                MySqlDataReader reader = command.ExecuteReader();

                List<string> mls = new();

                while (reader.Read())
                {
                    mls.Add(reader.GetString("Ml_num"));
                }

                await reader.DisposeAsync();
                await command.DisposeAsync();

                await connection.CloseAsync();
                await connection.DisposeAsync();

                Log_Messages("Pending Image Downloads Count: " + mls.Count, null);

                return mls;
            }
            catch (Exception ex)
            {
                Log_Messages("Getting MLS of Pending Image Downloads Failed. Error:", ex.ToString() + "\n");
                return null;
            }
        }

        private static async Task Update_Image_Download(string ConnectionString, string MLS)
        {
            try
            {
                using MySqlConnection connection = new(ConnectionString);

                await connection.OpenAsync();

                using MySqlCommand command = new("UPDATE listing SET IsImageDownloaded=1 WHERE Ml_num=@Ml_num", connection);
                command.Parameters.AddWithValue("@Ml_num", MLS);

                await command.ExecuteNonQueryAsync();

                await command.DisposeAsync();

                await connection.CloseAsync();
                await connection.DisposeAsync();
            }
            catch (Exception ex)
            {
                Log_Messages("Updating Image Download MLS: " + MLS + " Failed. Error:", ex.ToString() + "\n");
            }
        }

        private static async Task<string?> RETS_Login(string BaseUrl, string Username, string Password, string Version)
        {
            Log_Messages("\n\n************RETS Login************", null);

            try
            {
                RestClientOptions options = new(BaseUrl + "/rets-treb3pv/server/login")
                {
                    Authenticator = new HttpBasicAuthenticator(Username, Password),
                    ThrowOnAnyError = false,
                    MaxTimeout = -1
                };

                RestClient client = new(options);

                RestRequest request = new RestRequest()
                    .AddHeader("RETS-Version", Version);

                RestResponse response = await client.ExecuteGetAsync(request);

                if (response.StatusCode == System.Net.HttpStatusCode.OK)
                {
                    string? cookies = null;

                    if (response.Cookies != null)
                    {
                        foreach(var cookie in response.Cookies)
                        {
                            if (cookie != null)
                            {
                                if (String.IsNullOrWhiteSpace(cookies))
                                {
                                    cookies = cookie.ToString() + ";";
                                }
                                else
                                {
                                    cookies += cookie.ToString() + ";";
                                }
                            }
                        }
                    }

                    Log_Messages("Login Successfull", null);

                    return cookies;
                }
                else
                {
                    Log_Messages("Login Failed. Response:", response.Content + "\n");

                    return null;
                }
            }
            catch(Exception ex)
            {
                Log_Messages("Login Failed. Response:", ex.ToString() + "\n");

                return null;
            }
        }

        private static async Task RETS_Logout(string BaseUrl, string Username, string Password, string Version, string Cookies)
        {
            Log_Messages("\n\n************RETS Logout************", null);

            try
            {
                RestClientOptions options = new(BaseUrl + "/rets-treb3pv/server/logout")
                {
                    Authenticator = new HttpBasicAuthenticator(Username, Password),
                    ThrowOnAnyError = false,
                    MaxTimeout = -1
                };

                RestClient client = new(options);

                RestRequest request = new RestRequest()
                    .AddHeader("RETS-Version", Version)
                    .AddHeader("Cookie", Cookies);

                RestResponse response = await client.ExecuteGetAsync(request);

                if (response.StatusCode == System.Net.HttpStatusCode.OK)
                {
                    Log_Messages("Logout Successfull", null);
                }
                else
                {
                    Log_Messages("Logout Failed. Response:", response.Content + "\n");
                }
            }
            catch (Exception ex)
            {
                Log_Messages("Logout Failed. Response:", ex.ToString() + "\n");
            }
        }

        private static async Task<JArray?> RETS_Search_Residents(string BaseUrl, string Username, string Password, string Version, int Days, DateTime CurrentDateTime, string Cookies)
        {
            Log_Messages("\n\n************RETS Search Residents************", null);

            try
            {

                RestClientOptions options = new(BaseUrl + "/rets-treb3pv/server/search")
                {
                    Authenticator = new HttpBasicAuthenticator(Username, Password),
                    ThrowOnAnyError = false,
                    MaxTimeout = -1
                };

                RestClient client = new(options);

                RestRequest request = new RestRequest()
                    .AddHeader("RETS-Version", Version)
                    .AddHeader("Cookie", Cookies)
                    .AddParameter("SearchType", "Property", ParameterType.QueryString)
                    .AddParameter("Class", "ResidentialProperty", ParameterType.QueryString)
                    .AddParameter("Query", "(timestamp_sql=" + CurrentDateTime.AddDays(Days).ToString("yyyy-MM-dd") + "-" + CurrentDateTime.ToString("yyyy-MM-dd") + ")", ParameterType.QueryString)
                    //.AddParameter("Limit", limit_search, ParameterType.QueryString)
                    .AddParameter("QueryType", "DMQL2", ParameterType.QueryString);

                RestResponse response = await client.ExecuteGetAsync(request);

                if (response.StatusCode == System.Net.HttpStatusCode.OK)
                {
                    string filePath = @"C:\temp\file.xml";

                    // Write the content to the file
                    System.IO.File.WriteAllText(filePath, xmlContent);


                    JObject? json = Convert_XML_To_JSON(response.Content);

                    if (json != null)
                    {
                        JToken? rets = json["RETS"];

                        if (rets != null)
                        {
                            JToken? redata = rets["REData"];

                            if (redata != null)
                            {
                                JToken? reproperties = redata["REProperties"];

                                if (reproperties != null)
                                {
                                    Log_Messages("Search Residents Successfull", null);
                                    return reproperties.Value<JArray>("ResidentialProperty");
                                }
                            }
                        }
                    }

                    Log_Messages("Search Residents Failed. Response:", "Invalid XML\n");

                    return null;
                }
                else
                {
                    Log_Messages("Search Residents Failed. Response:", response.Content + "\n");

                    return null;
                }
            }
            catch (Exception ex)
            {
                Log_Messages("Search Residents Failed. Response:", ex.ToString() + "\n");

                return null;
            }
        }

        private static async Task<JArray?> RETS_Search_Condos(string BaseUrl, string Username, string Password, string Version, int Days, DateTime CurrentDateTime, string Cookies)
        {
            Log_Messages("\n\n************RETS Search Condos************", null);

            try
            {

                RestClientOptions options = new(BaseUrl + "/rets-treb3pv/server/search")
                {
                    Authenticator = new HttpBasicAuthenticator(Username, Password),
                    ThrowOnAnyError = false,
                    MaxTimeout = -1
                };

                RestClient client = new(options);

                RestRequest request = new RestRequest()
                    .AddHeader("RETS-Version", Version)
                    .AddHeader("Cookie", Cookies)
                    .AddParameter("SearchType", "Property", ParameterType.QueryString)
                    .AddParameter("Class", "CondoProperty", ParameterType.QueryString)
                    .AddParameter("Query", "(timestamp_sql=" + CurrentDateTime.AddDays(Days).ToString("yyyy-MM-dd") + "-" + CurrentDateTime.ToString("yyyy-MM-dd") + ")", ParameterType.QueryString)
                    //.AddParameter("Limit", limit_search, ParameterType.QueryString)
                    .AddParameter("QueryType", "DMQL2", ParameterType.QueryString);

                RestResponse response = await client.ExecuteGetAsync(request);

                if (response.StatusCode == System.Net.HttpStatusCode.OK)
                {
                    JObject? json = Convert_XML_To_JSON(response.Content);

                    if (json != null)
                    {
                        JToken? rets = json["RETS"];

                        if (rets != null)
                        {
                            JToken? redata = rets["REData"];

                            if (redata != null)
                            {
                                JToken? reproperties = redata["REProperties"];

                                if (reproperties != null)
                                {
                                    Log_Messages("Search Condos Successfull", null);
                                    return reproperties.Value<JArray>("CondoProperty");
                                }
                            }
                        }
                    }

                    Log_Messages("Search Condos Failed. Response:", "Invalid XML\n");

                    return null;
                }
                else
                {
                    Log_Messages("Search Condos Failed. Response:", response.Content + "\n");

                    return null;
                }
            }
            catch (Exception ex)
            {
                Log_Messages("Search Condos Failed. Response:", ex.ToString() + "\n");

                return null;
            }
        }

        private static async Task<JArray?> RETS_Search_Commercials(string BaseUrl, string Username, string Password, string Version, int Days, DateTime CurrentDateTime, string Cookies)
        {
            Log_Messages("\n\n************RETS Search Commercials************", null);

            try
            {

                RestClientOptions options = new(BaseUrl + "/rets-treb3pv/server/search")
                {
                    Authenticator = new HttpBasicAuthenticator(Username, Password),
                    ThrowOnAnyError = false,
                    MaxTimeout = -1
                };

                RestClient client = new(options);

                RestRequest request = new RestRequest()
                    .AddHeader("RETS-Version", Version)
                    .AddHeader("Cookie", Cookies)
                    .AddParameter("SearchType", "Property", ParameterType.QueryString)
                    .AddParameter("Class", "CommercialProperty", ParameterType.QueryString)
                    .AddParameter("Query", "(timestamp_sql=" + CurrentDateTime.AddDays(Days).ToString("yyyy-MM-dd") + "-" + CurrentDateTime.ToString("yyyy-MM-dd") + ")", ParameterType.QueryString)
                    //.AddParameter("Limit", limit_search, ParameterType.QueryString)
                    .AddParameter("QueryType", "DMQL2", ParameterType.QueryString);

                RestResponse response = await client.ExecuteGetAsync(request);

                if (response.StatusCode == System.Net.HttpStatusCode.OK)
                {
                    JObject? json = Convert_XML_To_JSON(response.Content);

                    if (json != null)
                    {
                        JToken? rets = json["RETS"];

                        if (rets != null)
                        {
                            JToken? redata = rets["REData"];

                            if (redata != null)
                            {
                                JToken? reproperties = redata["REProperties"];

                                if (reproperties != null)
                                {
                                    Log_Messages("Search Commercials Successfull", null);
                                    return reproperties.Value<JArray>("CommercialProperty");
                                }
                            }
                        }
                    }

                    Log_Messages("Search Commercials Failed. Response:", "Invalid XML\n");

                    return null;
                }
                else
                {
                    Log_Messages("Search Commercials Failed. Response:", response.Content + "\n");

                    return null;
                }
            }
            catch (Exception ex)
            {
                Log_Messages("Search Commercials Failed. Response:", ex.ToString() + "\n");

                return null;
            }
        }

        private static async Task RETS_Get_Images(string BaseUrl, string Username, string Password, string Version, string MLS, string Cookies, string ImagePath, string ConnectionString)
        {
            Log_Messages("\n\n************RETS Download Images For MLS: " + MLS + "************", null);

            try
            {
                string mls_image_path = Path.Combine(ImagePath, MLS);

                if (!Directory.Exists(mls_image_path))
                {
                    Directory.CreateDirectory(mls_image_path);
                }
                
                int count = 0;
                bool imagesExists;

                do
                {
                    imagesExists = await RETS_Download_Images(BaseUrl, Username, Password, Version, MLS + ":" + (count + 1), Cookies, mls_image_path);
                    count++;
                }
                while (imagesExists == true && count < 40);

                if (count > 0)
                {
                    await Update_Image_Download(ConnectionString, MLS);
                }
            }
            catch(Exception ex)
            {
                Log_Messages("Download Images Failed For MLS: " + MLS + ". Response:", ex.ToString() + "\n");
            }
        }

        private static async Task<bool> RETS_Download_Images(string BaseUrl, string Username, string Password, string Version, string ImageMLS, string Cookies, string ImagePath)
        {
            try
            {
                string base_auth_string = $"{Username}:{Password}";
                string base64_base_auth_string = Convert.ToBase64String(System.Text.ASCIIEncoding.ASCII.GetBytes(base_auth_string));

                HttpRequestMessage request = new(HttpMethod.Get, BaseUrl + "/rets-treb3pv/server/getobject?Type=Photo&Resource=Property&ID=" + ImageMLS);
                
                request.Headers.Authorization = new AuthenticationHeaderValue("Basic", base64_base_auth_string);
                request.Headers.Add("RETS-Version", Version);
                request.Headers.Add("Cookie", Cookies);

                HttpClient client = new();

                HttpResponseMessage response = await client.SendAsync(request);

                if (response.StatusCode == System.Net.HttpStatusCode.OK)
                {
                    foreach(var header in response.Headers)
                    {
                        if (header.Key == "Object-ID")
                        {
                            foreach (var value in header.Value)
                            {
                                if (value == "0")
                                {
                                    return false;
                                }
                            }

                            break;
                        }
                    }

                    string fileName = ImageMLS.Split(':')[1] + ".jpeg";
                    string path = Path.Combine(ImagePath, fileName);

                    Stream stream = await response.Content.ReadAsStreamAsync();

                    using (var fileStream = File.Create(path))
                    {
                        stream.Seek(0, SeekOrigin.Begin);
                        stream.CopyTo(fileStream);
                    }

                    Log_Messages("MLS: " + ImageMLS + " Saved. Path:", path + "\n");

                    return true;
                }
                else
                {
                    Log_Messages("Download Images Failed For MLS: " + ImageMLS + ". Response:", await response.Content.ReadAsStringAsync() + "\n");

                    return false;
                }
            }
            catch (Exception ex)
            {
                Log_Messages("Download Images Failed For MLS: " + ImageMLS + ". Response:", ex.ToString() + "\n");

                return false;
            }
        }

        private static FieldSeperatorModel[] Create_Field_Connection(JToken Listing)
        {
            FieldSeperatorModel[] seperators = FieldConnector.Get_Field_Seperators();

            for (int i = 0; i < seperators.Length; i++)
            {
                if (seperators[i] != null)
                {
                    string? xml = seperators[i].XML;

                    if (!String.IsNullOrWhiteSpace(xml))
                    {
                        string? value = Listing.Value<string>(xml);

                        if (!String.IsNullOrWhiteSpace(value))
                        {
                            if (value.ToLower().Trim() != "null")
                            {
                                int? length = seperators[i].Length;

                                if (length != null)
                                {
                                    if (value.Length > length)
                                    {
                                        seperators[i].Value = value[..length.Value];
                                    }
                                    else
                                    {
                                        seperators[i].Value = value;
                                    }
                                }
                                else
                                {
                                    seperators[i].Value = value;
                                }
                            }
                        }
                    }
                }
            }

            return seperators;
        }

        private static JObject? Convert_XML_To_JSON(string? XML)
        {
            try
            {
                if (String.IsNullOrWhiteSpace(XML))
                {
                    return null;
                }
                else
                {
                    XmlDocument doc = new();
                    doc.LoadXml(XML);

                    return JObject.Parse(JsonConvert.SerializeXmlNode(doc));
                }
            }
            catch (Exception)
            {
                return null;
            }
        }

        private static int? Convert_String_To_Integer(string? Value)
        {
            try
            {
                if (String.IsNullOrWhiteSpace(Value))
                {
                    return null;
                }
                else
                {
                    return Convert.ToInt32(Value);
                }
            }
            catch (Exception)
            {
                return null;
            }
        }

        private static void Log_Question(string Question)
        {
            Console.Write(Question);
        }

        private static void Log_Messages(string Title, string? Message)
        {
            Console.WriteLine(Title);

            if (!String.IsNullOrWhiteSpace(Message))
            {
                Console.WriteLine(Message);
            }
        }
    }
}