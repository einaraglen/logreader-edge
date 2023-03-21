using System.Globalization;
using Microsoft.Data.Sqlite;

namespace Database.Readers.Variants;

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

    public Dictionary<string, Dictionary<double, double>> GetChanges(List<string> signals, int changes)
    {

        SqliteCommand command = connection.CreateCommand();

        command.CommandText = $"SELECT timestamp, {this.GetSignalColumns(signals)} FROM {this.GetTable()} ORDER BY timestamp DESC LIMIT {changes}";

        SqliteDataReader reader = command.ExecuteReader();

        Dictionary<string, Dictionary<double, double>> collection = new Dictionary<string, Dictionary<double, double>>();

        while (reader.Read())
        {
            double timestamp = Convert.ToDouble(reader.GetString(0), CultureInfo.InvariantCulture) * 1000;

            for (int i = 0; i < signals.Count; i++)
            {
                string name = signals[i];
                Dictionary<double, double> values = collection.ContainsKey(name) ? collection[name] : new Dictionary<double, double>();
                double value = Convert.ToDouble(reader.GetString(i + 1), CultureInfo.InvariantCulture);
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
            double timestamp = Convert.ToDouble(reader.GetString(0), CultureInfo.InvariantCulture) * 1000;

            for (int i = 0; i < signals.Count; i++)
            {
                string name = signals[i];
                Dictionary<double, double> values = collection.ContainsKey(name) ? collection[name] : new Dictionary<double, double>();
                double value = Convert.ToDouble(reader.GetString(i + 1), CultureInfo.InvariantCulture);
                values.Add(timestamp, value);
                collection[name] = values;
            }
        }

        return collection;
    }

    public List<string> GetSignals()
    {
        SqliteCommand command = connection.CreateCommand();
        command.CommandText = $"SELECT name FROM pragma_table_info('{this.GetTable()}') ORDER BY cid";

        SqliteDataReader reader = command.ExecuteReader();

        List<string> signals = new List<string>();

        while (reader.Read())
        {
            string column = reader.GetString(0);

            if (!column.Equals("id") && !column.Equals("timestamp"))
            {
                signals.Add(reader.GetString(0));
            }
        }

        return signals;
    }

    private string GetSignalColumns(List<string> signals)
    {
        return String.Join(", ", signals);
    }
}