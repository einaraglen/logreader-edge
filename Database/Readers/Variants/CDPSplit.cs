using Microsoft.Data.Sqlite;

namespace Database.Readers.Variants;

public class CDPSplit : ICDPReader {

    private SqliteConnection connection;
    private Dictionary<string, int> dictionary = new Dictionary<string, int>();

    private Dictionary<int, Unpacker> partitions = new Dictionary<int, Unpacker>();

    public CDPSplit(string file) {
        this.connection = new SqliteConnection($"Data Source={file};mode=ReadOnly");

        this.connection.Open();

        this.GetDictionary();
        this.GetPartitions(file);

        this.connection.Close();
    }

    private void GetPartitions(string root) {
        List<int> unique = this.dictionary.Select(x => x.Value).Distinct().ToList();

        foreach (int index in unique) {
            string file = this.GetPartitionFile(root, index);
            this.partitions[index] = new Unpacker(file, CDPDataStore.Split);
        }
    }

    private void GetDictionary() {

        SqliteCommand command = connection.CreateCommand();

        command.CommandText = "SELECT * FROM ConnectionNodeMap";

        SqliteDataReader reader = command.ExecuteReader();

        while (reader.Read())
        {
           string name = reader.GetString(1);
           int partition = reader.GetInt16(3);

            this.dictionary[name] = partition;
        }
    }

    private string GetPartitionFile(string root, int partition) {
        string[] filename = Path.GetFileName(root).Split(".");
        string dir = Path.GetDirectoryName(root)!;
        return Path.Join(dir, $"{filename[0]}{partition}.{filename[1]}");
    }

    public Dictionary<string, Dictionary<double, double>> GetChanges(List<string> signals, int changes)
    {
        Dictionary<string, Dictionary<double, double>> collection = new Dictionary<string, Dictionary<double, double>>();
        Dictionary<int, List<string>> groups = new Dictionary<int, List<string>>();

        foreach (string signal in signals) {
            int partition = this.dictionary[signal];
            List<string> group = groups.ContainsKey(partition) ? groups[partition] : new List<string>();
            group.Add(signal);
            groups[partition] = group;
        }


        foreach (KeyValuePair<int, List<string>> entry in groups) {
            Dictionary<string, Dictionary<double, double>> values = this.partitions[entry.Key].GetLastKeyframes(entry.Value, changes);
            values.ToList().ForEach(x => collection.Add(x.Key, x.Value));
        }


        return collection;
    }

    public Dictionary<string, Dictionary<double, double>> GetRange(List<string> signals, long from, long to)
    {
        Dictionary<string, Dictionary<double, double>> collection = new Dictionary<string, Dictionary<double, double>>();
        Dictionary<int, List<string>> groups = new Dictionary<int, List<string>>();

        foreach (string signal in signals) {
            int partition = this.dictionary[signal];
            List<string> group = groups.ContainsKey(partition) ? groups[partition] : new List<string>();
            group.Add(signal);
            groups[partition] = group;
        }


        foreach (KeyValuePair<int, List<string>> entry in groups) {
            Dictionary<string, Dictionary<double, double>> values = this.partitions[entry.Key].GetRange(entry.Value, from, to);
            values.ToList().ForEach(x => collection.Add(x.Key, x.Value));
        }


        return collection;
    }

    public List<string> GetSignals()
    {
        return this.dictionary.Select(x => x.Key).ToList();
    }
}