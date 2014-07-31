using System;
using System.Collections.Generic;
using System.Linq;
using System.Text;

using System.Data.Sql;
using System.Data.SqlTypes;
using Microsoft.SqlServer.Server;

namespace CCSI.SqlServer
{
    public partial class Procedures
    {
        [SqlProcedure()]
        public static void SynchronizedBackup(String database_name, String backup_options,
                                       Int16 fresh_day, Int16 backup_hour, String base_directories, Int32 purge_in_days)
        {
            // Initialize the synchronization table
            InitializeBackupSynch(database_name, fresh_day, backup_hour, false);

            DateTime dt = DateTime.Now;
            BackupBase full, differential, log;

            full = new FullBackup(database_name, base_directories, backup_options, purge_in_days, dt);
            differential = new DifferentialBackup(database_name, base_directories, backup_options, purge_in_days, dt);
            log = new LogBackup(database_name, base_directories, backup_options, purge_in_days, dt);


            if (TryFullBackup(full, dt, fresh_day, backup_hour))
            {
                DoBackup(full, dt);
            }
            else if (TryDifferentialBackup(differential, dt, fresh_day, backup_hour))
            {
                DoBackup(differential, dt);
            }
            else
            {
                DoBackup(log, dt);
            }
        }

        [SqlProcedure()]
        public static void InitializeBackupSynch(String database_name, Int16 fresh_day, Int16 backup_hour, Boolean force_overwrite)
        {
            BackupBase.InitializeBackupSynch(database_name, fresh_day, backup_hour, force_overwrite);
        }

        private static void DoBackup<T>(T backup, DateTime now) where T: BackupBase
        {
            if (backup.Backup())
            {
                backup.LastBackupDate = now;
                backup.PurgePaths();
            }
        }

        private static Boolean TryFullBackup(BackupBase backup, DateTime dt, Int16 fresh_day, Int16 backup_hour)
        {
            return (Int32)dt.DayOfWeek + 1 == fresh_day &&
                   dt.Hour >= backup_hour &&
                   backup.LastBackupDate.Date < dt.Date;
        }

        private static Boolean TryDifferentialBackup(BackupBase backup, DateTime dt, Int16 fresh_day, Int16 backup_hour)
        {
            return (Int32)dt.DayOfWeek + 1 != fresh_day &&
                   dt.Hour >= backup_hour &&
                   backup.LastBackupDate.Date < dt.Date;
        }
    }
}
