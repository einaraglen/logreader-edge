using Microsoft.Data.Sqlite;
namespace CDP;

public enum CDPDataStore
{
    Compact,
    Split,
    Basic,
    Basic2,
    Unknown
}

public class Discovery
{

    public string file = "";
    public CDPDataStore type = CDPDataStore.Unknown;

    public Discovery(string dir)
    {
        List<string> files = GetDatabaseFiles(dir);
        GetDatabaseType(files);
    }

    private void GetDatabaseType(List<string> files)
    {
        foreach (string file in files)
        {
            List<string> tables = GetDatabaseTables(file);

            if (tables.Contains("ConnectionNodeMap"))
            {
                Console.WriteLine($"Discovered CDPDatastore [SPLIT]: {file}");
                this.type = CDPDataStore.Split;
                this.file = file;
                break;
            }

            if (tables.Contains("SignalMap"))
            {
                Console.WriteLine($"Discovered CDPDatastore [COMPACT]: {file}");
                this.type = CDPDataStore.Compact;
                this.file = file;
                break;
            }

            if (tables.Contains("SQLSignalLogger") || tables.Contains("SQLSignalLogger2"))
            {
                Console.WriteLine($"Discovered CDPDatastore [BASIC]: {file}");
                this.type = CDPDataStore.Basic;
                this.file = file;
                break;
            }

            if ( tables.Contains("SQLSignalLogger2"))
            {
                Console.WriteLine($"Discovered CDPDatastore [BASIC2]: {file}");
                this.type = CDPDataStore.Basic2;
                this.file = file;
                break;
            }
        }
    }

    private List<string> GetDatabaseTables(string path)
    {
        SqliteConnection connection = new SqliteConnection($"Data Source={path};mode=ReadOnly");

        connection.Open();

        SqliteCommand command = connection.CreateCommand();
        command.CommandText = "SELECT name FROM sqlite_master WHERE type='table'";

        SqliteDataReader reader = command.ExecuteReader();

        List<string> tables = new List<string>();

        while (reader.Read())
        {
            tables.Add(reader.GetString(0));
        }

        connection.Close();

        return tables;
    }

    private List<string> GetDatabaseFiles(string dir)
    {
        return Directory.EnumerateFiles(dir, "*.db").ToList();
    }
}