namespace CDP;

public enum CDPDataType
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

    public static CDPDataType GetType(BinaryReader reader)
    {
        return (CDPDataType)reader.ReadByte();
    }

    public static double GetValue(BinaryReader reader, CDPDataType type)
    {
        switch (type)
        {
            case CDPDataType.DOUBLE:
                return (double)reader.ReadDouble();
            case CDPDataType.UINT64:
                return (double)reader.ReadUInt64();
            case CDPDataType.INT64:
                return (double)reader.ReadInt64();
            case CDPDataType.FLOAT:
                return (double)reader.ReadSingle();
            case CDPDataType.UINT:
                return (double)reader.ReadUInt16();
            case CDPDataType.INT:
                return (double)reader.ReadUInt16();
            case CDPDataType.USHORT:
                return (double)reader.ReadUInt16();
            case CDPDataType.SHORT:
                return (double)reader.ReadUInt16();
            case CDPDataType.BOOL:
                return (double)(reader.ReadBoolean() ? 1 : 0);
            default:
                throw new InvalidDataException("No matching types when reading blob");
        }
    }

}