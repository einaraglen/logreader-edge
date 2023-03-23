namespace CDP.Variants;

public interface ICDPReader {
    List<SignalMetadata> GetSignals();
    Range GetBounds();
    Dictionary<string, Dictionary<long, double>> GetRange(List<string> signals, long from, long to);
    Dictionary<string, Dictionary<long, double>> GetChanges(List<string> signals, long changes);
    void Open();
    void Close();
}