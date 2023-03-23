using Models.Proto;
using CDP;
using MQTTnet;
using Google.Protobuf;

public class SignalsHandler : IHandler
{
    public void OnMessage(string id, byte[] bytes)
    {
        try
        {
            Console.WriteLine("Handling Signals Request");
            List<SignalMetadata> signals = ExtractorSingleton.Instance.Extractor.GetSignals();

            SignalsPayload payload = new SignalsPayload();

            payload.Signals.AddRange(signals.Select(x => x.GetFormatted()));

            using (var stream = new MemoryStream())
            {
                payload.WriteTo(stream);
                var serialized = stream.ToArray();

                MqttApplicationMessage message = message = new MqttApplicationMessageBuilder()
                        .WithTopic($"Edge/Response/{Environment.GetEnvironmentVariable("MQTT_CLIENT_ID")!}/GetSignals")
                        .WithPayload(serialized)
                        .Build();

                MQTTClientSingleton.Instance.Client.PublishAsync(message).Wait();
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Failed to Handle Signals request: \n{ex}");
        }

    }
}