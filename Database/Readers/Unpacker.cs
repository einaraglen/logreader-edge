using System.Globalization;
using Database.Readers;
using Microsoft.Data.Sqlite;
using System.IO;

public class Unpacker
{
    private SqliteConnection connection;
    private CDPDataStore type;

    private Dictionary<int, SignalMetadata> signals = new Dictionary<int, SignalMetadata>();

    public Unpacker(string file, CDPDataStore type)
    {
        this.type = type;
        this.connection = new SqliteConnection($"Data Source={file};mode=ReadOnly");
        this.connection.Open();

        this.GetSignals();
    }

    public void GetSignals()
    {
        SqliteCommand command = connection.CreateCommand(); 

        command.CommandText = $"SELECT * FROM {this.GetSignalTable()}";

        SqliteDataReader reader = command.ExecuteReader();

        while (reader.Read())
        {
            SignalMetadata metadata = new SignalMetadata(reader.GetInt16(0), reader.GetString(1), reader.GetString(2), null);
            this.signals[metadata.id] = metadata;
        }
    }

    public Dictionary<string, Dictionary<double, double>> GetKeyframes(List<string> signals, int frames)
    {
        SqliteCommand command = connection.CreateCommand();

        command.CommandText = $"SELECT x_axis, {this.GetKeyframesColumns(signals)} FROM KeyFrames1 ORDER BY x_axis DESC LIMIT {frames}";

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

        command.CommandText = $"SELECT * FROM {this.GetBlobTable()} WHERE x_axis BETWEEN {min} AND {max}";

        SqliteDataReader reader = command.ExecuteReader();

        Dictionary<string, Dictionary<double, double>> collection = new Dictionary<string, Dictionary<double, double>>();

        while (reader.Read())
        {
            double timestamp = Convert.ToDouble(reader.GetString(0), CultureInfo.InvariantCulture) * 1000;
            Stream stream = reader.GetStream(1);
            this.UnpackCompact(stream);
        }

        return collection;
    }

    private void UnpackSplit(Stream stream)
    {
        using (BinaryReader reader = new BinaryReader(stream))
        {
            int id = Blob.GetSignal(reader);
            SignalMetadata signal = this.signals[id];
            Console.WriteLine(signal.name);
        }
    }

    private void UnpackCompact(Stream stream)
    {
        using (BinaryReader reader = new BinaryReader(stream))
        {
            int id = Blob.GetSignal(reader);
            SignalMetadata signal = this.signals[id];
            Console.WriteLine(signal.name);
        }
    }

    private string GetKeyframesColumns(List<string> signals)
    {
        return String.Join("Last, ", signals);
    }

    private string GetBlobTable()
    {
        return this.type == CDPDataStore.Split ? "NodeValues" : "SignalValues";
    }

    private string GetSignalTable()
    {
        return this.type == CDPDataStore.Split ? "Node" : "SignalMap";
    }

}