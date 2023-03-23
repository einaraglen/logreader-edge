using System.Globalization;
using Microsoft.Data.Sqlite;

namespace CDP.Variants;

public class CDPBasic : ICDPReader
{

    private SqliteConnection connection;
    private CDPDataStore type;
    public CDPBasic(string file, CDPDataStore type)
    {
        this.type = type;
        this.connection = new SqliteConnection($"Data Source={file};mode=ReadOnly");
        this.connection.Open();
    }

    private string GetTable()
    {
        return this.type == CDPDataStore.Basic ? "SQLSignalLogger" : "SQLSignalLogger2";
    }

    public Dictionary<string, Dictionary<long, double>> GetChanges(List<string> signals, long changes)
    {

        SqliteCommand command = connection.CreateCommand();

        command.CommandText = $"SELECT timestamp, {this.GetSignalColumns(signals)} FROM {this.GetTable()} ORDER BY timestamp DESC LIMIT {changes}";

        SqliteDataReader reader = command.ExecuteReader();

        Dictionary<string, Dictionary<long, double>> collection = new Dictionary<string, Dictionary<long, double>>();

        while (reader.Read())
        {
            long timestamp = (long)(reader.GetDouble(0) * 1000);

            for (int i = 0; i < signals.Count; i++)
            {
                string name = signals[i];
                Dictionary<long, double> values = collection.ContainsKey(name) ? collection[name] : new Dictionary<long, double>();
                double value = reader.GetDouble(i + 1);
                values.Add(timestamp, value);
                collection[name] = values;
            }
        }

        return collection;
    }

    public Dictionary<string, Dictionary<long, double>> GetRange(List<string> signals, long from, long to)
    {
        SqliteCommand command = connection.CreateCommand();

        string min = Utils.DoubleToString(from / 1000.0);
        string max = Utils.DoubleToString(to / 1000.0);

        command.CommandText = $"SELECT timestamp, {this.GetSignalColumns(signals)} FROM {this.GetTable()} WHERE timestamp BETWEEN {min} AND {max}";

        SqliteDataReader reader = command.ExecuteReader();

        Dictionary<string, Dictionary<long, double>> collection = new Dictionary<string, Dictionary<long, double>>();

        while (reader.Read())
        {
            long timestamp = (long)(reader.GetDouble(0) * 1000);

            for (int i = 0; i < signals.Count; i++)
            {
                string name = signals[i];
                Dictionary<long, double> values = collection.ContainsKey(name) ? collection[name] : new Dictionary<long, double>();
                double value = reader.GetDouble(i + 1);
                values.Add(timestamp, value);
                collection[name] = values;
            }
        }

        return collection;
    }

    public List<SignalMetadata> GetSignals()
    {
        SqliteCommand command = connection.CreateCommand();
        command.CommandText = $"SELECT name FROM pragma_table_info('{this.GetTable()}') ORDER BY cid";

        SqliteDataReader reader = command.ExecuteReader();

        List<SignalMetadata> signals = new List<SignalMetadata>();

        int id = 1;

        while (reader.Read())
        {
            string column = reader.GetString(0);

            if (!column.Equals("id") && !column.Equals("timestamp"))
            {
                signals.Add(new SignalMetadata(id, reader.GetString(0), null, null));
                id++;
            }
        }

        return signals;
    }

    private string GetSignalColumns(List<string> signals)
    {
        return String.Join(", ", signals);
    }

    private double GetBound(string variant)
    {
        SqliteCommand command = connection.CreateCommand();
        command.CommandText = $"SELECT {variant}(timestamp) FROM {this.GetTable()}";

        SqliteDataReader reader = command.ExecuteReader();

        double value = 0.0;

        while (reader.Read())
        {
            value = reader.GetDouble(0);
        }

        return value * 1000;
    }

    public Dictionary<string, long> GetCount(List<string> signals, long from, long to)
    {
        SqliteCommand command = connection.CreateCommand();

        string min = Utils.DoubleToString(from / 1000.0);
        string max = Utils.DoubleToString(to / 1000.0);

        command.CommandText = $"SELECT COUNT(*) FROM {this.GetTable()} WHERE timestamp BETWEEN {min} AND {max}";

        SqliteDataReader reader = command.ExecuteReader();

        long value = 0;

        while (reader.Read())
        {
            value = reader.GetInt64(0);
        }

        return signals.ToDictionary(x => x, v => value);
    }

    public Range GetBounds()
    {
        long min = (long)this.GetBound("MIN");
        long max = (long)this.GetBound("MAX");

        return new Range(min, max);
    }

    public void Open()
    {
        this.connection.Open();
    }

    public void Close()
    {
        this.connection.Close();
    }
}