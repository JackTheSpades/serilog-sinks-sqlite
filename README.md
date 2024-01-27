# Serilog.Sinks.SQLite
A lightweight high performance Serilog sink that writes to SQLite database.

## Getting started
Install [Serilog.Sinks.SQLite](https://www.nuget.org/packages/Serilog.Sinks.SQLite) from NuGet

```PowerShell
Install-Package Serilog.Sinks.SQLite
```

Configure logger by calling `WriteTo.SQLite()`

```C#
var logger = new LoggerConfiguration()
    .WriteTo.SQLite(@"Logs\log.db")
    .CreateLogger();
    
logger.Information("This informational message will be written to SQLite database");
```

## Configure Columns
The `WriteTo.SQLite(...)` extension provides a `columns` parameter which can be used to configure the columns and content of the SQLite table.

By default, if `null` is passed, `SQLiteColumnCollection.DefaultCollection()` will be invoked containing the columns `id`, `Timestamp`, `Level`, `Exception`, `RenderedMessage` and `Properties`.

### UTC and DateTime format
The method also takes parameters to configure the time stamp column's UTC usage and string format as well as the `IFormatProvider` for the rendered message.
Note that the time stamp format is used for the retention policy and as such needs to be usable in conjunction with an SQLite `<` operator in a where-clause.

```C#
var myColumns = SQLiteColumnCollection.DefaultCollection(useUtc: false, dateTimeFormat: "yyyy-MM-ddTHH:mm:ss.fff", formatProvider: null);

var logger = new LoggerConfiguration()
    .WriteTo.SQLite(@"Logs\log.db", columns: myColumns)
    .CreateLogger();
```

### More Control
The columns can also be fully assambled by hand.

```C#
var myColumns = new SQLiteColumnCollection(idColumnName: "id")
{
    new TimestampSQLiteColumn(useUtc: false, format: "yyyy-MM-ddTHH:mm:ss.fff"),
    new SQLiteColumn("Level", DbType.String, e => e.Level.ToString()),
    new SQLiteColumn("Exception", DbType.String, e => e.Exception?.ToString() ?? string.Empty),
    new SQLiteColumn("RenderedMessage", DbType.String, e => e.MessageTemplate.Render(e.Properties, formatProvider: null)),
    new PropertiesJsonSQLiteColumn()
};
```

The `TimestampSQLiteColumn` is special as it is used in conjunction with the retention policy. A column collection may only hold one instance of `TimestampSQLiteColumn`.
If a `retentionPeriod` is set on `WriteTo.SQLite` and no time stamp column is added yet, the configuration will try to add the default one (`new TimestampSQLiteColumn()`).

The name of the primary key column can be configured but the column type and attributes of `INTEGER PRIMARY KEY AUTOINCREMENT` are fixed.

Even more options are available when deriving from the default `SQLiteColumn` class.
For example, if more constraints are desired the `Create()` method can be overridden:

```C#
class SQLiteUniqueColumn : SQLiteColumn
{
    // ...

    public override string Create() => $"{Name} {SQLiteTypeFromDbType(DbType)} UNIQUE";
}
```


## XML &lt;appSettings&gt; configuration

To use the SQLite sink with the [Serilog.Settings.AppSettings](https://www.nuget.org/packages/Serilog.Settings.AppSettings) package, first install that package if you haven't already done so:

```PowerShell
Install-Package Serilog.Settings.AppSettings
```
In your code, call `ReadFrom.AppSettings()`

```C#
var logger = new LoggerConfiguration()
    .ReadFrom.AppSettings()
    .CreateLogger();
```
In your application's App.config or Web.config file, specify the SQLite sink assembly and required **sqliteDbPath** under the `<appSettings>` node:

```XML
<appSettings>
    <add key="serilog:using:SQLite" value="Serilog.Sinks.SQLite"/>
    <add key="serilog:write-to:SQLite.sqliteDbPath" value="Logs\log.db"/>
    <add key="serilog:write-to:SQLite.tableName" value="Logs"/>
    <add key="serilog:write-to:SQLite.storeTimestampInUtc" value="true"/>
</appSettings>    
```

## Performance
SQLite sink automatically buffers log internally and flush to SQLite database in batches on dedicated thread.

[![Build status](https://ci.appveyor.com/api/projects/status/sqjvxji4w84iyqa0?svg=true)](https://ci.appveyor.com/project/SaleemMirza/serilog-sinks-sqlite)
