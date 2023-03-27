using CDP;
using Google.Protobuf;
using SeaBrief.Models.Proto.Timeseries;
using SeaBrief.MQTT;
using SeaBrief.MQTT.Message;
using MQTTnet.Client;

public class BoundsHandler : IMessageReceiver
{
    private string TOPIC;

    public BoundsHandler()
    {
        var client = Environment.GetEnvironmentVariable("MQTT_CLIENT_ID")!;
        var service = Environment.GetEnvironmentVariable("MQTT_SERVICE_NAME")!;
        this.TOPIC = $"Edge/{client}/{service}/GetBounds/Request";
    }
    public async Task OnMessage(MqttApplicationMessageReceivedEventArgs args)
    {
        try
        {
            if (!args.ApplicationMessage.Topic.Equals(this.TOPIC))
            {
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
                    .WithTopic(MQTTUtils.GetResponseTopic(this.TOPIC))
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