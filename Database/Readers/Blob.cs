public enum CDPDataTypes
{
    UNDEFINED,
    DOUBLE,
    UINT64,
    INT64,
    FLOAT,
    UINT,
    INT,
    USHORT,
    SHORT,
    UCHAR,
    CHAR,
    BOOL,
    STRING,
}

public class Blob
{
    public static int GetSignal(BinaryReader reader)
    {
        return reader.ReadUInt16();
    }

    public static CDPDataTypes GetType(BinaryReader reader)
    {
        return (CDPDataTypes)reader.ReadByte();
    }

    public static double GetValue(BinaryReader reader, CDPDataTypes type)
    {
        switch (type)
        {
            case CDPDataTypes.DOUBLE:
                return (double)reader.ReadDouble();
            case CDPDataTypes.UINT64:
                return (double)reader.ReadUInt64();
            case CDPDataTypes.INT64:
                return (double)reader.ReadInt64();
            case CDPDataTypes.FLOAT:
                return (double)reader.ReadDecimal();
            case CDPDataTypes.UINT:
                return (double)reader.ReadUInt16();
            case CDPDataTypes.INT:
                return (double)reader.ReadUInt16();
            case CDPDataTypes.USHORT:
                return (double)reader.ReadUInt16();
            case CDPDataTypes.SHORT:
                return (double)reader.ReadUInt16();
            case CDPDataTypes.UCHAR:
                return (double)reader.ReadUInt16();
            case CDPDataTypes.CHAR:
                return (double)reader.ReadUInt16();
            case CDPDataTypes.BOOL:
                return (double)reader.ReadInt16();
            case CDPDataTypes.STRING:
                throw new InvalidDataException("String not supported when reading blob");
            default:
                throw new InvalidDataException("No matching types when reading blob");
        }
    }

}