using CDP;
using Google.Protobuf;
using LogReaderLibrary.Models.Proto.Metadata;
using LogReaderLibrary.Models.Proto.Timeseries;
using LogReaderLibrary.MQTT;
using LogReaderLibrary.MQTT.Message;
using MQTTnet.Client;

public class CountHandler : IMessageReceiver
{
    private string TOPIC;
    public CountHandler() {
        this.TOPIC = $"Edge/Request/{Environment.GetEnvironmentVariable("MQTT_CLIENT_ID")!}/GetCount";
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
                    .WithTopic($"Edge/Response/{Environment.GetEnvironmentVariable("MQTT_CLIENT_ID")!}/GetCount")
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