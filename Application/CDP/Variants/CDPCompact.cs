namespace CDP.Variants;

public class CDPCompact : ICDPReader
{

    private Unpacker unpacker;
    public CDPCompact(string file)
    {
        this.unpacker = new Unpacker(file, CDPDataStore.Compact);
    }

    public Dictionary<string, Dictionary<double, double>> GetChanges(List<string> signals, long changes)
    {
        return this.unpacker.GetLastKeyframes(signals, changes);
    }

    public Dictionary<string, Dictionary<double, double>> GetRange(List<string> signals, long from, long to)
    {
        return this.unpacker.GetRange(signals, from, to);
    }

    public List<SignalMetadata> GetSignals()
    {
        return this.unpacker.signals.Select(x => x.Value).ToList();
    }

    public Range GetBounds()
    {
        return this.unpacker.GetBounds();
    }
}