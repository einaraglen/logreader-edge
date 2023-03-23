namespace MQTT.Handler;

public interface IHandler {
    Task OnMessage(string id, byte[] bytes);
}