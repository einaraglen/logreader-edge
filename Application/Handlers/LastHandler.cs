using CDP;
using Google.Protobuf;
using SeaBrief.Models.Proto.Timeseries;
using SeaBrief.MQTT.Message;
using SeaBrief.MQTT;
using MQTTnet.Client;

public class LastHandler : IMessageReceiver
{
    private string TOPIC;
    public LastHandler()
    {
        var client = Environment.GetEnvironmentVariable("MQTT_CLIENT_ID")!;
        var service = Environment.GetEnvironmentVariable("MQTT_SERVICE_NAME")!;
        this.TOPIC = $"Edge/{client}/{service}/GetLast/Request";
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

            foreach (var entry in collection)
            {
                TimeseriesValues timeseriesValues = new TimeseriesValues();
                foreach (var values in entry.Value)
                {
                    Timeseries row = new Timeseries();
                    row.XAxis = values.Key;
                    row.YAxis = values.Value;
                    timeseriesValues.Values.Add(row);
                }
                payload.Timeseries.Add(entry.Key, timeseriesValues);
            }

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
            Console.WriteLine($"Failed to Handle Changes request: \n{ex}");
        }
    }
}