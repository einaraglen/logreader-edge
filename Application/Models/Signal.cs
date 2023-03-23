using CDP;
using LogReaderLibrary.Models.Proto;


public class SignalMetadata
{
    public int id;
    public string name;
    public string? type;
    public string? path;

    public SignalMetadata(int id, string name, string? type, string? path)
    {
        this.id = id;
        this.name = name;
        this.type = type;
        this.path = path;
    }

    public Signal GetFormatted()
    {
        if (this.path == null)
        {
            return new Signal
            {
                Id = this.id,
                Name = this.name
            };
        }
        else
        {
            return new Signal
            {
                Id = this.id,
                Name = this.name,
                Path = this.path
            };
        }
    }

    public CDPDataType Type()
    {
        return (CDPDataType)Enum.Parse(typeof(CDPDataType), this.type!.ToUpper());
    }
}