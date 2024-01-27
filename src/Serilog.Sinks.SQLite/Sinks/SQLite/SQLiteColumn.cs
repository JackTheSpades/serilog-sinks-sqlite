using Serilog.Events;
using Serilog.Sinks.Extensions;
using System;
using System.Collections;
using System.Collections.Generic;
using System.Data;
using System.Data.SQLite;
using System.Linq;
using System.Text;
using System.Xml.Linq;

namespace Serilog.Sinks.SQLite
{
    /// <summary>The default column</summary>
    public class SQLiteColumn
    {
        public readonly string Name;
        public readonly string Parameter;
        public readonly DbType DbType;
        public readonly Func<LogEvent, object> ValueGetter;

        /// <summary>Instantiates a new column for the logging table.</summary>
        /// <param name="name">The name of the column.</param>
        /// <param name="dbType">The type used in conjuncture with <see cref="SQLiteParameter"/>.</param>
        /// <param name="valueGetter">The content that will be written to the row.</param>
        /// <exception cref="ArgumentNullException"><paramref name="name"/> or <paramref name="valueGetter"/> are <see langword="null"/>.</exception>
        /// <exception cref="ArgumentException"><paramref name="name"/> is empty or starts with an '@'.</exception>
        public SQLiteColumn(string name, DbType dbType, Func<LogEvent, object> valueGetter)
        {
            if (name is null)
                throw new ArgumentNullException(nameof(name));
            if (string.IsNullOrEmpty(name))
                throw new ArgumentException("Name for the column must be set.", nameof(name));
            if (name[0] == '@')
                throw new ArgumentException("Column name must not start with an '@' character.", nameof(name));

            Name = name;
            Parameter = "@" + name;
            DbType = dbType;
            ValueGetter = valueGetter ?? throw new ArgumentNullException(nameof(valueGetter));
        }

        /// <summary>Gives the SQLite data type used in CREATE TABLE for a given <see cref="System.Data.DbType"/>.</summary>
        /// <param name="dbType"></param>
        /// <returns>"INTEGER", "REAL", "TEXT" or "BLOB" depending on <paramref name="dbType"/>.</returns>
        /// <remarks>Date/Time types are taken as "TEXT".</remarks>
        protected static string SQLiteTypeFromDbType(DbType dbType)
        {
            // https://www.sqlite.org/datatype3.html#storage_classes_and_datatypes
            /*
            INTEGER. The value is a signed integer, stored in 0, 1, 2, 3, 4, 6, or 8 bytes depending on the magnitude of the value.
            REAL.    The value is a floating point value, stored as an 8-byte IEEE floating point number.
            TEXT.    The value is a text string, stored using the database encoding (UTF-8, UTF-16BE or UTF-16LE).
            BLOB.    The value is a blob of data, stored exactly as it was input.
            */
            switch (dbType)
            {
                case DbType.Boolean:
                case DbType.VarNumeric:
                case DbType.Byte:
                case DbType.SByte:
                case DbType.Int16:
                case DbType.Int32:
                case DbType.Int64:
                case DbType.UInt16:
                case DbType.UInt32:
                case DbType.UInt64:
                    return "INTEGER";

                case DbType.Currency:
                case DbType.Decimal:
                case DbType.Single:
                case DbType.Double:
                    return "REAL";

                case DbType.Binary:
                    return "BLOB";

                // https://www.sqlite.org/datatype3.html#date_and_time_datatype
                /* SQLite does not have a storage class set aside for storing dates and/or times. 
                   Instead, the built-in Date And Time Functions of SQLite are capable of storing dates and times as TEXT, REAL, or INTEGER values:
                        TEXT as ISO8601 strings ("YYYY-MM-DD HH:MM:SS.SSS").
                        REAL as Julian day numbers, the number of days since noon in Greenwich on November 24, 4714 B.C. according to the proleptic Gregorian calendar.
                        INTEGER as Unix Time, the number of seconds since 1970-01-01 00:00:00 UTC. 
                 */
                case DbType.Date:
                case DbType.DateTime:
                case DbType.DateTime2:
                case DbType.DateTimeOffset:
                case DbType.Time:

                case DbType.Guid:
                case DbType.Object:
                case DbType.String:
                case DbType.StringFixedLength:
                case DbType.AnsiString:
                case DbType.AnsiStringFixedLength:
                case DbType.Xml:
                default:
                    return "TEXT";
            }
        }

        /// <summary>The content that will be used during the CREATE TABLE command.</summary>
        /// <returns>The column name and SQLite column type without trailing comma.</returns>
        public virtual string Create() => $"{Name} {SQLiteTypeFromDbType(DbType)}";
    }

    /// <summary>
    /// The primary column for handling the time stamp of a log entry. This column will also be used to enforce the retention policy (if enabled).
    /// </summary>
    public sealed class TimestampSQLiteColumn : SQLiteColumn
    {
        private readonly bool _useUtc;
        private readonly string _format;

        /// <summary>Gives a text representation of the time stamp. Used for both the log entries and the retention policy.</summary>
        /// <param name="timestamp">Time stamp to be converted.</param>
        /// <returns></returns>
        public string Convert(DateTimeOffset timestamp)
            => Convert(timestamp, _useUtc, _format);

        /// <summary>Gives a text representation of the time stamp. Used for both the log entries and the retention policy.</summary>
        /// <param name="timestamp">Time stamp to be converted.</param>
        /// <param name="useUtc">If time stamp should be converted to UTC time first.</param>
        /// <param name="format">The format used in <see cref="DateTimeOffset.ToString(string)"/>.</param>
        /// <returns></returns>
        public static string Convert(DateTimeOffset timestamp, bool useUtc, string format)
            => (useUtc ? timestamp.ToUniversalTime() : timestamp).ToString(format);

        /// <summary>Instantiates a new column for the logging table and applying the retention policy.</summary>
        /// <param name="name">The name of the column.</param>
        /// <param name="useUtc">Store timestamp in UTC format.</param>
        /// <param name="format">
        /// The format used for the time stamps.
        /// If retention policy is used the format has to support the '&lt;' operator in SQLite where-clause.</param>
        public TimestampSQLiteColumn(string name = "Timestamp", bool useUtc = false, string format = "yyyy-MM-ddTHH:mm:ss.fff")
            : base(name, DbType.DateTime2, e => Convert(e.Timestamp, useUtc, format))
        {
            if (StringComparer.OrdinalIgnoreCase.Equals("epoch", name))
                throw new ArgumentException($"epoch is not valid as name for {nameof(TimestampSQLiteColumn)}", nameof(name));

            _useUtc = useUtc;
            _format = format;
        }
    }

    /// <summary></summary>
    public class PropertiesJsonSQLiteColumn : SQLiteColumn
    {
        public static string LogPropertiesToJson(IReadOnlyDictionary<string, LogEventPropertyValue> properties, string emptyProperties = "")
            => properties.Count > 0 ? properties.Json() : emptyProperties;

        /// <summary>Instantiates a new column for the logging table.</summary>
        /// <param name="name">The name of the column.</param>
        /// <param name="emptyProperties">Value used if <see cref="LogEvent.Properties"/> is empty.</param>
        public PropertiesJsonSQLiteColumn(string name = "Properties", string emptyProperties = "")
            : base(name, DbType.String, e => LogPropertiesToJson(e.Properties, emptyProperties))
        {
        }
    }

    public class SQLiteColumnCollection : ICollection<SQLiteColumn>
    {
        public readonly string IdColumnName;

        private readonly List<SQLiteColumn> _columns;
        private int _timestampIndex;
        private bool _locked;

        /// <summary></summary>
        /// <param name="idColumnName"></param>
        /// <exception cref="ArgumentNullException"><paramref name="idColumnName"/> is <see langword="null"/>.</exception>
        /// <exception cref="ArgumentException"><paramref name="idColumnName"/> is empty or starts with an '@'.</exception>
        public SQLiteColumnCollection(string idColumnName = "id")
        {
            if (idColumnName is null)
                throw new ArgumentNullException(nameof(idColumnName));
            if (string.IsNullOrEmpty(idColumnName))
                throw new ArgumentException("Id column name must be set.", nameof(idColumnName));
            if (idColumnName[0] == '@')
                throw new ArgumentException("Id column name must not start with an '@' character.", nameof(idColumnName));

            IdColumnName = idColumnName;
            _columns = new List<SQLiteColumn>();
            _timestampIndex = -1;
            _locked = false;
        }

        /// <summary>Locks the collection from further changes.</summary>
        public void Lock() => _locked = true;

        public int Count => _columns.Count;
        public bool IsReadOnly => _locked;

        /// <summary><see langword="true"/> if the collection has registered a column of type <see cref="TimestampSQLiteColumn"/>.</summary>
        public bool HasTimestampColumn => _timestampIndex >= 0;
        /// <summary>The registered <see cref="TimestampSQLiteColumn"/> instance or <see langword="null"/> if none is registered.</summary>
        public TimestampSQLiteColumn TimeStampColumn => HasTimestampColumn ? _columns[_timestampIndex] as TimestampSQLiteColumn : null;

        public void Add(SQLiteColumn column)
        {
            if (IsReadOnly)
                throw new ReadOnlyException();
            if (column is null)
                throw new ArgumentNullException(nameof(column));
            if (_columns.Any(c => StringComparer.InvariantCultureIgnoreCase.Equals(column.Name, c.Name)))
                throw new ArgumentException($"Column with name '{column.Name}' already exists.", nameof(column));
            if (StringComparer.InvariantCultureIgnoreCase.Equals(IdColumnName, column.Name))
                throw new ArgumentException("Column name cannot not be same as Id column.", nameof(column));
            if (column is TimestampSQLiteColumn && HasTimestampColumn)
                throw new ArgumentException($"Columns must have only one column of type {nameof(TimestampSQLiteColumn)}");

            if (column is TimestampSQLiteColumn)
                _timestampIndex = _columns.Count;

            _columns.Add(column);
        }

        public void Clear()
        {
            if (IsReadOnly)
                throw new ReadOnlyException();
            _columns.Clear();
            _timestampIndex = -1;
        }
        /// <summary>Checks if the collection has a column with the given name, ignoring case sensitivity.</summary>
        /// <param name="name">The name to be checked for.</param>
        /// <returns><see langword="true"/> if a column exists.</returns>
        public bool Contains(string name)
        {
            var comp = StringComparer.InvariantCultureIgnoreCase;
            if (comp.Equals(name, IdColumnName))
                return true;
            return _columns.Any(c => comp.Equals(c.Name, name));
        }
        public bool Contains(SQLiteColumn item) => _columns.Contains(item);

        public void CopyTo(SQLiteColumn[] array, int arrayIndex) => _columns.CopyTo(array, arrayIndex);

        /// <summary>Try to remove a column with the given name, ignoring case sensitivity.</summary>
        /// <param name="name">The name of the column to be removed.</param>
        /// <returns><see langword="true"/> if a column was removed.</returns>
        /// <remarks><see cref="IdColumnName"/> cannot be removed.</remarks>
        public bool Remove(string name) => Remove(_columns.FirstOrDefault(c => StringComparer.InvariantCultureIgnoreCase.Equals(c.Name, name)));
        public bool Remove(SQLiteColumn item)
        {
            if (IsReadOnly)
                throw new ReadOnlyException();

            bool removed = _columns.Remove(item);
            if (removed && item is TimestampSQLiteColumn)
                _timestampIndex = -1;
            return removed;
        }

        public IEnumerator<SQLiteColumn> GetEnumerator() => _columns.GetEnumerator();
        IEnumerator IEnumerable.GetEnumerator() => GetEnumerator();

        /// <summary>Creates the default collection of columns containing Timestamp, Level, Exception, RenderedMessage and Properties.</summary>
        /// <param name="useUtc">Store timestamp in UTC format</param>
        /// <param name="dateTimeFormat">
        /// The format used for the time stamps.
        /// If retention policy is used the format has to support the '&lt;' operator in SQLite where-clause.</param>
        /// <param name="formatProvider">Supplies culture-specific formatting information, or null.</param>
        /// <returns></returns>
        public static SQLiteColumnCollection DefaultCollection(bool useUtc = false, string dateTimeFormat = "yyyy-MM-ddTHH:mm:ss.fff", IFormatProvider formatProvider = null)
            => new SQLiteColumnCollection()
            {
                new TimestampSQLiteColumn(useUtc: useUtc, format: dateTimeFormat),
                new SQLiteColumn("Level", DbType.String, e => e.Level.ToString()),
                new SQLiteColumn("Exception", DbType.String, e => e.Exception?.ToString() ?? string.Empty),
                new SQLiteColumn("RenderedMessage", DbType.String, e => e.MessageTemplate.Render(e.Properties, formatProvider)),
                new PropertiesJsonSQLiteColumn()
            };
    }

}
