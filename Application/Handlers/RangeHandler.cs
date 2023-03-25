using CDP;
using Google.Protobuf;
using LogReaderLibrary.Compression;
using LogReaderLibrary.Models.Proto.Timeseries;
using LogReaderLibrary.MQTT;
using LogReaderLibrary.MQTT.Message;
using MQTTnet.Client;

public class RangeHandler : IMessageReceiver
{
    private string TOPIC;
    public RangeHandler()
    {
        this.TOPIC = $"Edge/Request/{Environment.GetEnvironmentVariable("MQTT_CLIENT_ID")!}/GetRange";
    }
    public async Task OnMessage(MqttApplicationMessageReceivedEventArgs args)
    {
        try
        {
            if (!args.ApplicationMessage.Topic.Equals(this.TOPIC)) {
                return;
            }
            
            var correlation = MQTTUtils.GetCorrelation(args);

            Console.WriteLine("Handling Range Request");
            RangeRequest data = RangeRequest.Parser.ParseFrom(MQTTUtils.GetPayload(args));

            var collection = ExtractorSingleton.Instance.Extractor.GetRange(data.Signals.ToList(), (long)data.From, (long)data.To);

            CompressedTimeseriesPayload payload = new CompressedTimeseriesPayload();

            string[] names = collection.Select(x => x.Key).ToArray();

            foreach (string name in names)
            {
                var logs = collection[name]!;
                var timestamps = logs.Select(x => x.Key).ToArray();
                var encoded = Delta2.Encode(timestamps);
                var values = logs.Select(x => x.Value).ToArray();

                Values protoValues = new Values();
                protoValues.Entries.AddRange(values);

                Timestamps protoTimestamps = new Timestamps();
                protoTimestamps.Entries.AddRange(encoded);

                payload.Signals.Add(name);
                payload.Timestamps.Add(protoTimestamps);
                payload.Values.Add(protoValues);
            }

            using (var stream = new MemoryStream())
            {
                payload.WriteTo(stream);
                var serialized = stream.ToArray();

                await new MessageBuilder()
                    .WithTopic($"Edge/Response/{Environment.GetEnvironmentVariable("MQTT_CLIENT_ID")!}/GetRange")
                    .WithPayload(serialized)
                    .WithCorrelation(correlation)
                    .Publish()
                    .ConfigureAwait(false);
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Failed to Handle Range request: \n{ex}");
        }

    }
}