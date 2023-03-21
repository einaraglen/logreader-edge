public class SignalMetadata {
    public int id;
    public string name;
    public string? type;
    public string? path;

    public SignalMetadata(int id, string name, string? type, string? path) {
        this.id = id;
        this.name = name;
        this.type = type;
        this.path = path;
    }
}