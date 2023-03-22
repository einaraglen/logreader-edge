namespace CDP.Variants;

public interface ICDPReader {
    List<SignalMetadata> GetSignals();
    Range GetBounds();
    Dictionary<string, Dictionary<double, double>> GetRange(List<string> signals, long from, long to);
    Dictionary<string, Dictionary<double, double>> GetChanges(List<string> signals, long changes);
}