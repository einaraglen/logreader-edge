using Microsoft.Data.Sqlite;

namespace Database.Readers.Variants;

public class CDPSplit : ICDPReader {

    private SqliteConnection connection;

    public CDPSplit(string file) {
        this.connection = new SqliteConnection($"Data Source={file};mode=ReadOnly");
    }

    public Dictionary<string, Dictionary<double, double>> GetChanges(List<string> signals, int changes)
    {
        throw new NotImplementedException();
    }

    public Dictionary<string, Dictionary<double, double>> GetRange(List<string> signals, long from, long to)
    {
        throw new NotImplementedException();
    }

    public List<string> GetSignals()
    {
        throw new NotImplementedException();
    }

    private string GetSignalColumns(List<string> signals) {
        return signals.ToString() ?? "";
    }
}