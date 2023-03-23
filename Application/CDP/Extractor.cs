using CDP.Variants;

namespace CDP;

public class Extractor : ICDPReader
{

    private ICDPReader? reader;
    private string dir;


    public Extractor(string dir)
    {
        this.dir = dir;

    }

    public void Load()
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

    public Dictionary<string, Dictionary<long, double>> GetChanges(List<string> signals, long changes)
    {
        if (this.reader == null)
        {
            throw new InvalidOperationException("Cannot use Extractor before loading database");
        }

        return this.reader.GetChanges(signals, changes);
    }

    public Dictionary<string, Dictionary<long, double>> GetRange(List<string> signals, long from, long to)
    {
        if (this.reader == null)
        {
            throw new InvalidOperationException("Cannot use Extractor before loading database");
        }

        return this.reader.GetRange(signals, from, to);
    }

    public List<SignalMetadata> GetSignals()
    {
        if (this.reader == null)
        {
            throw new InvalidOperationException("Cannot use Extractor before loading database");
        }

        return this.reader.GetSignals();
    }

    public Range GetBounds()
    {
        if (this.reader == null)
        {
            throw new InvalidOperationException("Cannot use Extractor before loading database");
        }

        return this.reader.GetBounds();
    }

    public void Open()
    {
        if (this.reader == null)
        {
            throw new InvalidOperationException("Cannot use Extractor before loading database");
        }

        this.reader.Open();
    }

    public void Close()
    {
        if (this.reader == null)
        {
            throw new InvalidOperationException("Cannot use Extractor before loading database");
        }

        this.reader.Close();
    }
}