namespace Database.Readers.Variants;

public interface ICDPReader {
    List<string> GetSignals();
    Dictionary<string, Dictionary<double, double>> GetRange(List<string> signals, long from, long to);
    Dictionary<string, Dictionary<double, double>> GetChanges(List<string> signals, int changes);

}