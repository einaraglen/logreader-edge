using CDP;
using Google.Protobuf;
using LogReaderLibrary.Models.Proto;
using LogReaderLibrary.MQTT.Handler;
using LogReaderLibrary.MQTT.Message;

public class SignalsHandler : IHandler
{
    public async Task OnMessage(string id, byte[] bytes, string? correlation)
    {
        try
        {
            Console.WriteLine($"Handling Signals Request with correlation: {correlation}");
            List<SignalMetadata> signals = ExtractorSingleton.Instance.Extractor.GetSignals();

            SignalsPayload payload = new SignalsPayload();

            payload.Signals.AddRange(signals.Select(x => x.GetFormatted()));

            using (var stream = new MemoryStream())
            {
                payload.WriteTo(stream);
                var serialized = stream.ToArray();

                var message = new MessageBuilder()
                    .WithTopic($"Edge/Response/{Environment.GetEnvironmentVariable("MQTT_CLIENT_ID")!}/GetSignals")
                    .WithPayload(serialized)
                    .WithCorrelation(correlation);

                if (correlation != null)
                {
                    message.WithCorrelation(correlation);
                }

                await message.Publish();
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Failed to Handle Signals request: \n{ex}");
        }

    }
}