using CDP;
using Google.Protobuf;
using LogReaderLibrary.Models.Proto.Timeseries;
using LogReaderLibrary.MQTT;
using LogReaderLibrary.MQTT.Message;
using MQTTnet.Client;

public class BoundsHandler : IMessageReceiver
{
    private string TOPIC;

    public BoundsHandler() {
        this.TOPIC = $"Edge/Request/{Environment.GetEnvironmentVariable("MQTT_CLIENT_ID")!}/GetBounds";
    }
    public async Task OnMessage(MqttApplicationMessageReceivedEventArgs args)
    {
        try
        {
            if (!args.ApplicationMessage.Topic.Equals(this.TOPIC)) {
                return;
            }
            
            var correlation = MQTTUtils.GetCorrelation(args);

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
                    .Publish()
                    .ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Failed to Handle Bounds request: \n{ex}");
        }
    }
}