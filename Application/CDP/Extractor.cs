using CDP.Variants;

namespace CDP;

public class Extractor : ICDPReader
{

    private ICDPReader reader;


    public Extractor(string dir)
    {
        Discovery discovery = new Discovery(dir);

        switch (discovery.type)
        {
            case CDPDataStore.Basic:
                this.reader = new CDPBasic(discovery.file, discovery.type);
                break;
            case CDPDataStore.Basic2:
                this.reader = new CDPBasic(discovery.file, discovery.type);
                break;
            case CDPDataStore.Compact:
                this.reader = new CDPCompact(discovery.file);
                break;
            case CDPDataStore.Split:
                this.reader = new CDPSplit(discovery.file);
                break;
            default:
                throw new InvalidDataException("No supported datase format discovered");
        }

    }

    public Dictionary<string, Dictionary<double, double>> GetChanges(List<string> signals, long changes)
    {
        return this.reader.GetChanges(signals, changes);
    }

    public Dictionary<string, Dictionary<double, double>> GetRange(List<string> signals, long from, long to)
    {
        return this.reader.GetRange(signals, from, to);
    }

    public List<SignalMetadata> GetSignals()
    {
        return this.reader.GetSignals();
    }

    public Range GetBounds()
    {
        return this.reader.GetBounds();
    }
}