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

    public Dictionary<string, Dictionary<double, double>> GetChanges(List<string> signals, long changes)
    {

        SqliteCommand command = connection.CreateCommand();

        command.CommandText = $"SELECT timestamp, {this.GetSignalColumns(signals)} FROM {this.GetTable()} ORDER BY timestamp DESC LIMIT {changes}";

        SqliteDataReader reader = command.ExecuteReader();

        Dictionary<string, Dictionary<double, double>> collection = new Dictionary<string, Dictionary<double, double>>();

        while (reader.Read())
        {
            double timestamp = reader.GetDouble(0) * 1000;

            for (int i = 0; i < signals.Count; i++)
            {
                string name = signals[i];
                Dictionary<double, double> values = collection.ContainsKey(name) ? collection[name] : new Dictionary<double, double>();
                double value = reader.GetDouble(i + 1);
                values.Add(timestamp, value);
                collection[name] = values;
            }
        }

        return collection;
    }

    public Dictionary<string, Dictionary<double, double>> GetRange(List<string> signals, long from, long to)
    {
        SqliteCommand command = connection.CreateCommand();

        string min = Utils.DoubleToString(from / 1000.0);
        string max = Utils.DoubleToString(to / 1000.0);

        command.CommandText = $"SELECT timestamp, {this.GetSignalColumns(signals)} FROM {this.GetTable()} WHERE timestamp BETWEEN {min} AND {max}";

        SqliteDataReader reader = command.ExecuteReader();

        Dictionary<string, Dictionary<double, double>> collection = new Dictionary<string, Dictionary<double, double>>();

        while (reader.Read())
        {
            double timestamp = reader.GetDouble(0) * 1000;

            for (int i = 0; i < signals.Count; i++)
            {
                string name = signals[i];
                Dictionary<double, double> values = collection.ContainsKey(name) ? collection[name] : new Dictionary<double, double>();
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

    private double GetBound(string variant) {
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

    public Range GetBounds()
    {
        double min = this.GetBound("MIN");
        double max = this.GetBound("MAX");

        return new Range(min, max);
    }
}