using CDP;
using Google.Protobuf;
using LogReaderLibrary.Models.Proto;
using LogReaderLibrary.MQTT.Handler;
using LogReaderLibrary.MQTT.Message;

public class BoundsHandler : IHandler
{
    public async Task OnMessage(string id, byte[] bytes, string? correlation)
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

                await new MessageBuilder()
                .WithTopic($"Edge/Response/{Environment.GetEnvironmentVariable("MQTT_CLIENT_ID")!}/GetBounds")
                .WithPayload(serialized)
                .WithCorrelation(correlation)
                .Publish();
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Failed to Handle Bounds request: \n{ex}");
        }
    }
}