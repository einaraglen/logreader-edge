using MQTTnet;

namespace MQTT.Message;

public class MessageBuilder {
    private string? topic;
    private byte[]? bytes;

    public MessageBuilder WithTopic(String topic) {
        this.topic = topic;
        return this;
    }

    public MessageBuilder WithPayload(byte[] bytes) {
        this.bytes = bytes;
        return this;
    }

    public async Task Publish() {
        MqttApplicationMessage message = new MqttApplicationMessage();
        message.Topic = topic;
        await MQTTClientSingleton.Instance.Client.PublishAsync(message);
    }
}