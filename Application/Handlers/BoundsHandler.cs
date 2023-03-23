using Models.Proto;
using CDP;
using MQTTnet;
using Google.Protobuf;

public class BoundsHandler : IHandler
{
    public void OnMessage(string id, byte[] bytes)
    {
        try
        {
            Console.WriteLine("Handling Bounds Request");
            Range bounds = ExtractorSingleton.Instance.Extractor.GetBounds();

            BoundsPayload payload = new BoundsPayload
            {
                From = bounds.from,
                To = bounds.to,
            };

            using (var stream = new MemoryStream())
            {
                payload.WriteTo(stream);
                var serialized = stream.ToArray();

                MqttApplicationMessage message = message = new MqttApplicationMessageBuilder()
                        .WithTopic($"Edge/Response/{Environment.GetEnvironmentVariable("MQTT_CLIENT_ID")!}/GetBounds")
                        .WithPayload(serialized)
                        .Build();

                MQTTClientSingleton.Instance.Client.PublishAsync(message).Wait();
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Failed to Handle Bounds request: \n{ex}");
        }

    }
}