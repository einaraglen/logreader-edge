using Models.Proto;
using CDP;
using Google.Protobuf;
using LogReaderLibrary.MQTT.Handler;
using LogReaderLibrary.MQTT.Message;

public class SignalsHandler : IHandler
{
    public async Task OnMessage(string id, byte[] bytes)
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

                await new MessageBuilder()
                    .WithTopic($"Edge/Response/{Environment.GetEnvironmentVariable("MQTT_CLIENT_ID")!}/GetSignals")
                    .WithPayload(serialized)
                    .Publish();
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Failed to Handle Signals request: \n{ex}");
        }

    }
}