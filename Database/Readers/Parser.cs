using Database.Readers;
using Database.Readers.Variants;

public class Parser
{

    public ICDPReader reader;


    public Parser(string dir)
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
}