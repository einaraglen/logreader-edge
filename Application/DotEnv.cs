public static class DotEnv
{
    public static void Load(string[]? required = null)
    {
        var root = Directory.GetCurrentDirectory();
        var file = Path.Combine(root, ".env");

        if (!File.Exists(file)) {
            throw new FileLoadException($"Could not locate environment variables at '{file}'");
        }


        foreach (var line in File.ReadAllLines(file))
        {
            string[] parts = line.Split('=', StringSplitOptions.RemoveEmptyEntries);

            if (parts.Length != 2) {
                continue;
            }

            if (required != null && required.Contains(parts[0])) {
                required = required.Where(x => x != parts[0]).ToArray();
            }

            Environment.SetEnvironmentVariable(parts[0], CleanVariable(parts[1]));
        }

        if (required != null && required.Length != 0) {
            foreach (string missing in required) {
                Console.WriteLine($"Missing Environment Variable '{missing}'");
            }

            throw new InvalidDataException("Failed to Load Environment: Missing Variables");
        }

        Console.WriteLine($"Loaded Environment Variables from: {file}");
    }

    private static string CleanVariable(string variable) {
        return variable.Replace("\"", "");
    }
}