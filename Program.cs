using Database.Readers;

Parser parser = new Parser("./assets/split");

List<string> test = new List<string>{
    "MemUsedLoggerApp",
};

// long from = 1538636400562;
// long to = 1538636404362;
// int changes = 5;

long from = 1665584962017;
long to = from + 100000;


// parser.reader.GetSignals().ForEach(x => Console.WriteLine(x));

Dictionary<string, Dictionary<double, double>> values = parser.reader.GetRange(test, from, to);
// Dictionary<string, Dictionary<double, double>> values = parser.reader.GetChanges(test, changes);

foreach (var signal in values)
    {
        Console.WriteLine($"Name: {signal.Key}, size: {signal.Value.Count}");
    }
