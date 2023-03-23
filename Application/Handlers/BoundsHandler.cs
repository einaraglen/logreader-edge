using Models.Proto;
using CDP;
using Google.Protobuf;
using LogReaderLibrary.MQTT.Handler;
using LogReaderLibrary.MQTT.Message;

public class BoundsHandler : IHandler
{
    public async Task OnMessage(string id, byte[] bytes)
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
                .Publish();
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Failed to Handle Bounds request: \n{ex}");
        }
    }
}