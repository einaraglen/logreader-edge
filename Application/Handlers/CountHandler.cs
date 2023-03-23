using Models.Proto;
using CDP;
using Google.Protobuf;
using LogReaderLibrary.MQTT.Handler;
using LogReaderLibrary.MQTT.Message;

public class CountHandler : IHandler
{
    public async Task OnMessage(string id, byte[] bytes)
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

                await new MessageBuilder()
                    .WithTopic($"Edge/Response/{Environment.GetEnvironmentVariable("MQTT_CLIENT_ID")!}/GetCount")
                    .WithPayload(serialized)
                    .Publish();
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Failed to Handle Count request: \n{ex}");
        }
    }
}