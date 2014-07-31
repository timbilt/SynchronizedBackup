using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;
using System.IO;

using Microsoft.SqlServer.Server;
using System.Data.SqlClient;

namespace CCSI.SqlServer
{
    public abstract class BackupBase
    {
        private static readonly String table = "CCSIBackupDB.dbo.backup_synchronization";
        private readonly String database_name;
        public readonly List<String> backup_paths;
        private readonly String backup_options;
        private readonly Int32 purge_in_days;

        public BackupBase(String database_name, String base_directories, String backup_options, Int32 purge_in_days, DateTime dt)
        {
            this.database_name = database_name;
            this.backup_paths = GetBackupPaths(base_directories, dt);
            this.backup_options = backup_options;
            this.purge_in_days = (purge_in_days > 0) ? purge_in_days * -1 : purge_in_days;
        }

        public abstract String category
        {
            get;
        }

        public abstract String type
        {
            get;
        }

        public String column_name
        {
            get { return "last_" + type; }
        }

        #region Initialize and Check Database

        public static void CreateSynchTable()
        {
            String command;

            command = String.Format("IF OBJECT_ID('{0}') IS NULL " +
                                    "CREATE TABLE {0} " +
                                    "(" +
                                        "database_name		NVARCHAR(256)		NOT NULL, " +
                                        "backup_running		BIT					NOT NULL, " +
                                        "fresh_day			SMALLINT			NOT NULL, " +
                                        "backup_hour		SMALLINT			NOT NULL, " +
                                        "last_full			DATETIME			NOT NULL, " +
                                        "last_differential	DATETIME			NOT NULL, " +
                                        "last_log		    DATETIME			NOT NULL  " +
                                    ")", table);

            SqlQuery.ExecuteNonQuery(command);
        }

        private static Boolean IsDatabaseInitialized(String database_name)
        {
            String command;

            command = String.Format("SELECT COUNT(*) FROM {0} WHERE database_name = '{1}'",
                                    table, database_name);

            return SqlQuery.ExecuteQueryInt32(command) == 1;
        }

        private static void UpdateDatabaseRow(String database_name, Int16 fresh_day, Int16 backup_hour)
        {
            String command;

            command = String.Format("UPDATE	{0} " +
                                    "SET	backup_running = 0, fresh_day = {2}, backup_hour = {3}, " +
                                    "       last_full = '19000101', last_diff = '19000101', last_log = '19000101' " +
                                    "WHERE	database_name = '{1}'", table, database_name, fresh_day, backup_hour);

            SqlQuery.ExecuteNonQuery(command);
        }

        private static void InsertDatabaseRow(String database_name, Int16 fresh_day, Int16 backup_hour)
        {
            String command;

            command = String.Format("INSERT INTO {0} " +
                                    "VALUES ('{1}', 0, {2}, {3}, '19000101', '19000101', '19000101')",
                                    table, database_name, fresh_day, backup_hour);

            SqlQuery.ExecuteNonQuery(command);
        }

        public static void InitializeBackupSynch(String database_name, Int16 fresh_day, Int16 backup_hour, Boolean force_overwrite)
        {
            CreateSynchTable();

            Boolean bInit = IsDatabaseInitialized(database_name);

            if (bInit && force_overwrite)
                UpdateDatabaseRow(database_name, fresh_day, backup_hour);
            else if (!bInit)
                InsertDatabaseRow(database_name, fresh_day, backup_hour);
        }

        private void SendMessage(String message)
        {
            SqlContext.Pipe.Send(message);
        }

        private Boolean GetBackupSynchState()
        {
            String command;

            command = String.Format("SELECT TOP 1 backup_running FROM {0} WHERE database_name = '{1}'",
                                    table, database_name);

            return SqlQuery.ExecuteQueryBoolean(command);
        }

        private void SetBackupSynchState(Int32 state)
        {
            String command;

            command = String.Format("UPDATE {0} SET backup_running = {1} WHERE database_name = '{2}'",
                                    table, state, database_name);

            SqlQuery.ExecuteNonQuery(command);
        }

        public DateTime LastBackupDate
        {
            get 
            {
                String command;

                command = String.Format("SELECT {0} FROM {1} WHERE database_name = '{2}'", column_name, table, database_name);

                return SqlQuery.ExecuteQueryDateTime(command);
            }            
            set
            {
                String command;

                command = String.Format("UPDATE	{0} " +
                                    "SET	{1} = '{2}' " +
                                    "WHERE	database_name = '{3}'",
                                    table, column_name, GetDateTimeString(value), database_name);

                SqlQuery.ExecuteNonQuery(command);
            }
        }

        private String GetDateTimeString(DateTime dt)
        {
            return String.Format("{0:D4}{1:D2}{2:D2} {3:D2}:{4:D2}:{5:D2}.000",
                                dt.Year, dt.Month, dt.Day,
                                dt.Hour, dt.Minute, dt.Second);
        }

        private String GetBackupFileName(DateTime dt)
        {
            return String.Format("{0}_{1}_{2}-{3}-{4}-{5}-{6}-{7}.bak", database_name, type,
                                 dt.Year, dt.Month, dt.Day, dt.Hour, dt.Minute, dt.Second);
        }

        private String CleanupPath(String path)
        {
            var clean_path = path.Trim();

            if (clean_path.Last() != '\\')
                clean_path += '\\';

            return clean_path;
        }

        private String GetDatabasePath(String path)
        {
            return path + database_name + '\\';
        }

        private void CreateDatabasePath(String path)
        {
            if (!Directory.Exists(path))
            {
                Directory.CreateDirectory(path);
            }
        }

        private String GetValidatedPath(String path, DateTime dt)
        {
            String valid_path;
            
            valid_path = CleanupPath(path);

            valid_path = GetDatabasePath(valid_path);

            CreateDatabasePath(valid_path);

            valid_path += GetBackupFileName(dt);

            return valid_path;
        }

        public List<String> GetBackupPaths(String directory_list, DateTime dt)
        {
            var retList = new List<String>();

            foreach (var dir in directory_list.Split(','))
            {
                retList.Add(GetValidatedPath(dir, dt));
            }

            return retList;
        }

        private void DeleteOldFiles(String path)
        {
            string[] files = Directory.GetFiles(path);

            foreach (string file in files)
            {
                FileInfo fi = new FileInfo(file);
                if (fi.CreationTime < DateTime.Now.AddDays(purge_in_days))
                {
                    SendMessage("Deleting old file " + fi.Name + "...");
                    fi.Delete();
                }
            }
        }

        public void PurgePaths()
        {
            foreach (var path in backup_paths)
                DeleteOldFiles(path.Substring(0, path.LastIndexOf('\\') + 1));
        }

        private String GetMirrorClause()
        {
            String mirror = String.Empty;

            if (backup_paths.Count > 1)
            {
                foreach (var path in backup_paths.Skip(1).Take(3))
                    mirror += "MIRROR TO DISK = '" + path + "' ";
            }

            return mirror;
        }

        private String GetOptionClause()
        {
            String options = String.Empty;

            if (!String.IsNullOrEmpty(backup_options))
                options += " WITH " + backup_options;

            return options;
        }

        private String GetBackupScript()
        {
            String script;

            script = String.Format("BACKUP {0} {1} TO DISK = '{2}' ", category, database_name, backup_paths.First());

            script += GetMirrorClause();

            script += GetOptionClause();

            return script;
        }

        public virtual Boolean Backup()
        {
            if (GetBackupSynchState())
            {
                SendMessage("A backup task is already running for database " + database_name + ", exiting...");
                return false;
            }            

            try
            {
                SendMessage(String.Format("Performing {0} backup of database {1}...", type, database_name));
                SetBackupSynchState(1);
                SqlQuery.ExecuteNonQuery(GetBackupScript());
            }
            catch (SqlException)
            {
                SendMessage("Backup Failed!");
                throw;
            }
            finally
            {
                SetBackupSynchState(0);
            }

            SendMessage("Backup Completed Successfully!");

            return true;
        }        

        #endregion
    }

    public class FullBackup : BackupBase
    {
        public FullBackup(String database_name, String backup_paths, String backup_options, Int32 purge_in_days, DateTime dt)
            : base(database_name, backup_paths, backup_options, purge_in_days, dt)
        {}

        public override String category
        {
            get { return "DATABASE"; }
        }

        public override String type
        {
            get { return "FULL"; }
        }
    }

    public class DifferentialBackup : BackupBase
    {
        public DifferentialBackup(String database_name, String backup_paths, String backup_options, Int32 purge_in_days, DateTime dt)
            : base(database_name, backup_paths, backup_options, purge_in_days, dt)
        {}

        public override String category
        {
            get { return "DATABASE"; }
        }

        public override String type
        {
            get { return "DIFFERENTIAL"; }
        }
    }

    public class LogBackup : BackupBase
    {
        public LogBackup(String database_name, String backup_paths, String backup_options, Int32 purge_in_days, DateTime dt)
            : base(database_name, backup_paths, backup_options, purge_in_days, dt)
        {}

        public override String category
        {
            get { return "LOG"; }
        }

        public override String type
        {
            get { return category; }
        }
    }
}
