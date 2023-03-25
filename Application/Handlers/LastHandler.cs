using CDP;
using Google.Protobuf;
using LogReaderLibrary.Models.Proto.Timeseries;
using LogReaderLibrary.Compression;
using LogReaderLibrary.MQTT.Message;
using LogReaderLibrary.MQTT;
using MQTTnet.Client;

public class LastHandler : IMessageReceiver
{
    private string TOPIC;
    public LastHandler()
    {
        this.TOPIC = $"Edge/Request/{Environment.GetEnvironmentVariable("MQTT_CLIENT_ID")!}/GetLast";
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

            Console.WriteLine("Handling Last Request");
            ChangesRequest data = ChangesRequest.Parser.ParseFrom(MQTTUtils.GetPayload(args));

            var collection = ExtractorSingleton.Instance.Extractor.GetLast(data.Signals.ToList(), data.Changes);

            TimeseriesPayload payload = new TimeseriesPayload();

            foreach (var entry in collection) {
                TimeseriesValues values = new TimeseriesValues();
                values.Values.Add(entry.Value);
                payload.Timeseries.Add(entry.Key, values);
            }

            using (var stream = new MemoryStream())
            {
                payload.WriteTo(stream);
                var serialized = stream.ToArray();

                await new MessageBuilder()
                    .WithTopic($"Edge/Response/{Environment.GetEnvironmentVariable("MQTT_CLIENT_ID")!}/GetLast")
                    .WithPayload(serialized)
                    .WithCorrelation(correlation)
                    .Publish()
                    .ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Failed to Handle Changes request: \n{ex}");
        }
    }
}