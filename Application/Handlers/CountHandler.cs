using CDP;
using Google.Protobuf;
using SeaBrief.Models.Proto.Metadata;
using SeaBrief.Models.Proto.Timeseries;
using SeaBrief.MQTT;
using SeaBrief.MQTT.Message;
using MQTTnet.Client;

public class CountHandler : IMessageReceiver
{
    private string TOPIC;
    public CountHandler() {
        var client = Environment.GetEnvironmentVariable("MQTT_CLIENT_ID")!;
        var service = Environment.GetEnvironmentVariable("MQTT_SERVICE_NAME")!;
        this.TOPIC = $"Edge/{client}/{service}/GetCount/Request";
    }
    public async Task OnMessage(MqttApplicationMessageReceivedEventArgs args)
    {
        try
        {
            if (!args.ApplicationMessage.Topic.Equals(this.TOPIC)) {
                return;
            }
            
            var correlation = MQTTUtils.GetCorrelation(args);

            Console.WriteLine("Handling Count Request");
            RangeRequest data = RangeRequest.Parser.ParseFrom(MQTTUtils.GetPayload(args));

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
                    .WithTopic(MQTTUtils.GetResponseTopic(this.TOPIC))
                    .WithPayload(serialized)
                    .WithCorrelation(correlation)
                    .Publish()
                    .ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Failed to Handle Count request: \n{ex}");
        }
    }
}