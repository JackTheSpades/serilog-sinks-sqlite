// Copyright 2016 Serilog Contributors
// 
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
// 
//     http://www.apache.org/licenses/LICENSE-2.0
// 
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

using System;
using System.Collections.Generic;
using System.Data;
using System.Data.SQLite;
using System.IO;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;
using Serilog.Core;
using Serilog.Debugging;
using Serilog.Events;
using Serilog.Sinks.Batch;
using Serilog.Sinks.Extensions;

namespace Serilog.Sinks.SQLite
{
    internal class SQLiteSink : BatchProvider, ILogEventSink
    {
        private readonly string _databasePath;
        private readonly uint _maxDatabaseSize;
        private readonly bool _rollOver;
        private readonly string _tableName;
        private readonly SQLiteColumnCollection _columns;
        private readonly TimeSpan? _retentionPeriod;
        private readonly Timer _retentionTimer;
        private const string TimestampFormat = "yyyy-MM-ddTHH:mm:ss.fff";
        private const long BytesPerMb = 1_048_576;
        private const long MaxSupportedPages = 5_242_880;
        private const long MaxSupportedPageSize = 4096;
        private const long MaxSupportedDatabaseSize = unchecked(MaxSupportedPageSize * MaxSupportedPages) / 1048576;
        private static SemaphoreSlim semaphoreSlim = new SemaphoreSlim(1, 1);
        
        public SQLiteSink(
            string sqlLiteDbPath,
            string tableName,
            TimeSpan? retentionPeriod,
            TimeSpan? retentionCheckInterval,
            SQLiteColumnCollection columns,
            uint batchSize = 100,
            uint maxDatabaseSize = 10,
            bool rollOver = true) : base(batchSize: (int)batchSize, maxBufferSize: 100_000)
        {
            _databasePath = sqlLiteDbPath;
            _tableName = tableName;
            _maxDatabaseSize = maxDatabaseSize;
            _rollOver = rollOver;

            _columns = columns ?? throw new ArgumentNullException(nameof(columns));

            if (retentionPeriod.HasValue && !columns.HasTimestampColumn)
                throw new ArgumentException($"Usage of retention requires that columns has a {nameof(TimestampSQLiteColumn)} column.");

            if (maxDatabaseSize > MaxSupportedDatabaseSize)
            {
                throw new SQLiteException($"Database size greater than {MaxSupportedDatabaseSize} MB is not supported");
            }

            _columns.Lock();
            InitializeDatabase();

            if (retentionPeriod.HasValue)
            {
                // impose a min retention period of 15 minute
                var retentionCheckMinutes = 15;
                if (retentionCheckInterval.HasValue)
                {
                    retentionCheckMinutes = Math.Max(retentionCheckMinutes, retentionCheckInterval.Value.Minutes);
                }

                // impose multiple of 15 minute interval
                retentionCheckMinutes = (retentionCheckMinutes / 15) * 15;

                _retentionPeriod = new[] { retentionPeriod, TimeSpan.FromMinutes(30) }.Max();

                // check for retention at this interval - or use retentionPeriod if not specified
                _retentionTimer = new Timer(
                    (x) => { ApplyRetentionPolicy(); },
                    null,
                    TimeSpan.FromMinutes(0),
                    TimeSpan.FromMinutes(retentionCheckMinutes));
            }
        }

        #region ILogEvent implementation

        public void Emit(LogEvent logEvent)
        {
            PushEvent(logEvent);
        }

        #endregion

        private void InitializeDatabase()
        {
            using (var conn = GetSqLiteConnection())
            {
                CreateSqlTable(conn);
            }
        }

        private SQLiteConnection GetSqLiteConnection()
        {
            var sqlConString = new SQLiteConnectionStringBuilder
            {
                DataSource = _databasePath,
                JournalMode = SQLiteJournalModeEnum.Memory,
                SyncMode = SynchronizationModes.Normal,
                CacheSize = 500,
                PageSize = (int)MaxSupportedPageSize,
                MaxPageCount = (int)(_maxDatabaseSize * BytesPerMb / MaxSupportedPageSize)
            }.ConnectionString;

            var sqLiteConnection = new SQLiteConnection(sqlConString);
            sqLiteConnection.Open();

            return sqLiteConnection;
        }

        private void CreateSqlTable(SQLiteConnection sqlConnection)
        {
            var sqlCreateText = $"CREATE TABLE IF NOT EXISTS {_tableName} ({_columns.IdColumnName} INTEGER PRIMARY KEY AUTOINCREMENT, {string.Join(",", _columns.Select(c => c.Create()))})";
            var sqlCommand = new SQLiteCommand(sqlCreateText, sqlConnection);
            sqlCommand.ExecuteNonQuery();
        }

        private SQLiteCommand CreateSqlInsertCommand(SQLiteConnection connection)
        {
            var sqlInsertText = $"INSERT INTO {_tableName} ({string.Join(",", _columns.Select(c => c.Name))}) VALUES ({string.Join(",", _columns.Select(c => c.Parameter))})";
            var sqlCommand = connection.CreateCommand();
            sqlCommand.CommandText = sqlInsertText;
            sqlCommand.CommandType = CommandType.Text;

            foreach (SQLiteColumn col in _columns)
                sqlCommand.Parameters.Add(new SQLiteParameter(col.Parameter, col.DbType));

            return sqlCommand;
        }

        private void ApplyRetentionPolicy()
        {
            var epoch = DateTimeOffset.Now.Subtract(_retentionPeriod.Value);
            using (var sqlConnection = GetSqLiteConnection())
            {
                using (var cmd = CreateSqlDeleteCommand(sqlConnection, epoch))
                {
                    SelfLog.WriteLine("Deleting log entries older than {0}", epoch);
                    var ret = cmd.ExecuteNonQuery();
                    SelfLog.WriteLine($"{ret} records deleted");
                }
            }
        }

        private void TruncateLog(SQLiteConnection sqlConnection)
        {
            var cmd = sqlConnection.CreateCommand();
            cmd.CommandText = $"DELETE FROM {_tableName}";
            cmd.ExecuteNonQuery();

            VacuumDatabase(sqlConnection);
        }

        private void VacuumDatabase(SQLiteConnection sqlConnection)
        {
            var cmd = sqlConnection.CreateCommand();
            cmd.CommandText = $"vacuum";
            cmd.ExecuteNonQuery();
        }

        private SQLiteCommand CreateSqlDeleteCommand(SQLiteConnection sqlConnection, DateTimeOffset epoch)
        {
            var cmd = sqlConnection.CreateCommand();
            cmd.CommandText = $"DELETE FROM {_tableName} WHERE {_columns.TimeStampColumn.Name} < @epoch";
            cmd.Parameters.Add(
                new SQLiteParameter("@epoch", _columns.TimeStampColumn.DbType)
                {
                    Value = _columns.TimeStampColumn.Convert(epoch)
                });

            return cmd;
        }

        protected override async Task<bool> WriteLogEventAsync(ICollection<LogEvent> logEventsBatch)
        {
            if ((logEventsBatch == null) || (logEventsBatch.Count == 0))
                return true;
            await semaphoreSlim.WaitAsync().ConfigureAwait(false);
            try
            {
                using (var sqlConnection = GetSqLiteConnection())
                {
                    try
                    {
                        await WriteToDatabaseAsync(logEventsBatch, sqlConnection).ConfigureAwait(false);
                        return true;
                    }
                    catch (SQLiteException e)
                    {
                        SelfLog.WriteLine(e.Message);

                        if (e.ResultCode != SQLiteErrorCode.Full)
                            return false;

                        if (_rollOver == false)
                        {
                            SelfLog.WriteLine("Discarding log excessive of max database");

                            return true;
                        }

                        var dbExtension = Path.GetExtension(_databasePath);

                        var newFilePath = Path.Combine(Path.GetDirectoryName(_databasePath) ?? "Logs",
                            $"{Path.GetFileNameWithoutExtension(_databasePath)}-{DateTime.Now:yyyyMMdd_HHmmss.ff}{dbExtension}");
                         
                        File.Copy(_databasePath, newFilePath, true);

                        TruncateLog(sqlConnection);
                        await WriteToDatabaseAsync(logEventsBatch, sqlConnection).ConfigureAwait(false);

                        SelfLog.WriteLine($"Rolling database to {newFilePath}");
                        return true;
                    }
                    catch (Exception e)
                    {
                        SelfLog.WriteLine(e.Message);
                        return false;
                    }
                }
            }
            finally
            {
                semaphoreSlim.Release();
            }
        }

        private async Task WriteToDatabaseAsync(ICollection<LogEvent> logEventsBatch, SQLiteConnection sqlConnection)
        {
            using (var tr = sqlConnection.BeginTransaction())
            {
                using (var sqlCommand = CreateSqlInsertCommand(sqlConnection))
                {
                    sqlCommand.Transaction = tr;

                    foreach (var logEvent in logEventsBatch)
                    {
                        foreach (var col in _columns)
                            sqlCommand.Parameters[col.Parameter].Value = col.ValueGetter(logEvent);

                        await sqlCommand.ExecuteNonQueryAsync().ConfigureAwait(false);
                    }
                    tr.Commit();
                }
            }
        }
    }
}
