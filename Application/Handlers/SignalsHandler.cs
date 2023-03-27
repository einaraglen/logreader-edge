using CDP;
using Google.Protobuf;
using SeaBrief.Models.Proto.Metadata;
using SeaBrief.MQTT;
using SeaBrief.MQTT.Message;
using MQTTnet.Client;

public class SignalsHandler : IMessageReceiver
{
    private string TOPIC;
    public SignalsHandler()
    {
        var client = Environment.GetEnvironmentVariable("MQTT_CLIENT_ID")!;
        var service = Environment.GetEnvironmentVariable("MQTT_SERVICE_NAME")!;
        this.TOPIC = $"Edge/{client}/{service}/GetSignals/Request";
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

            Console.WriteLine("Handling Signals Request");
            List<SignalMetadata> signals = ExtractorSingleton.Instance.Extractor.GetSignals();

            SignalsPayload payload = new SignalsPayload();

            payload.Signals.AddRange(signals.Select(x => x.GetFormatted()));

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
            Console.WriteLine($"Failed to Handle Signals request: \n{ex}");
        }

    }
}