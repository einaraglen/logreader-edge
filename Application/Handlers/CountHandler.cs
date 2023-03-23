using Models.Proto;
using CDP;
using MQTTnet;
using Google.Protobuf;

public class CountHandler : IHandler
{
    public void OnMessage(string id, byte[] bytes)
    {
        try
        {
            Console.WriteLine("Handling Count Request");
            RangeRequest data = RangeRequest.Parser.ParseFrom(bytes);

            var collection = ExtractorSingleton.Instance.Extractor.GetCount(data.Signals.ToList(), (long)data.From, (long)data.To);

            CountPayload payload = new CountPayload();

            payload.Counts.AddRange(collection.Select(x => new Count
            {
                Signal = x.Key,
                Value = x.Value
            }));

            using (var stream = new MemoryStream())
            {
                payload.WriteTo(stream);
                var serialized = stream.ToArray();

                MqttApplicationMessage message = message = new MqttApplicationMessageBuilder()
                        .WithTopic($"Edge/Response/{Environment.GetEnvironmentVariable("MQTT_CLIENT_ID")!}/GetCount")
                        .WithPayload(serialized)
                        .Build();

                MQTTClientSingleton.Instance.Client.PublishAsync(message).Wait();
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Failed to Handle Count request: \n{ex}");
        }

    }
}