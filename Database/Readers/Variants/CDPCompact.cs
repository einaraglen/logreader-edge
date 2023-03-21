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
        return this.unpacker.GetLastKeyframes(signals, changes);
    }

    public Dictionary<string, Dictionary<double, double>> GetRange(List<string> signals, long from, long to)
    {
        return this.unpacker.GetRange(signals, from, to);
    }

    public List<string> GetSignals()
    {
        return this.unpacker.signals.Select(x => x.Value.name).ToList();
    }
}