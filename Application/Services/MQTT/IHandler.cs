public interface IHandler {
    void OnMessage(string id, byte[] bytes);
}