namespace Database.Readers.Variants;

public class CDPCompact : ICDPReader
{

    private Unpacker unpacker;
    public CDPCompact(string file)
    {
        this.unpacker = new Unpacker(file, CDPDataStore.Compact);
    }

    public Dictionary<string, Dictionary<double, double>> GetChanges(List<string> signals, int changes)
    {
        this.unpacker.GetKeyframes(signals, changes);
        throw new NotImplementedException();
    }

    public Dictionary<string, Dictionary<double, double>> GetRange(List<string> signals, long from, long to)
    {
        this.unpacker.GetRange(signals, from, to);
        throw new NotImplementedException();
    }

    public List<string> GetSignals()
    {
        throw new NotImplementedException();
    }
}