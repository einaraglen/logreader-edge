using CDP;
using Models.Proto;
using MQTTnet.Extensions.ManagedClient;
using Google.Protobuf;
using MQTTnet;

public class MQTTResponder
{

    private IManagedMqttClient client;
    private Extractor extractor;

    public MQTTResponder(IManagedMqttClient client, string dir)
    {
        this.extractor = new Extractor(dir);
        this.client = client;
    }

    public async Task OnRangeRequest(byte[] bytes)
    {
        Console.WriteLine("Handling Range Request");
        RangeRequest data = RangeRequest.Parser.ParseFrom(bytes);

        var collection = this.extractor.GetRange(data.Signals.ToList(), (long)data.From, (long)data.To);

        DataPayload payload = new DataPayload();

        foreach (var entry in collection)
        {
            Entry current = new Entry
            {
                Name = entry.Key,
            };

            foreach (var value in entry.Value)
            {
                Value row = new Value
                {
                    Timestamp = value.Key,
                    Value_ = value.Value
                };
                current.Data.Add(row);
            }

            payload.Signals.Add(current);
        }

        using (var stream = new MemoryStream())
        {
            payload.WriteTo(stream);
            var serialized = stream.ToArray();

            MqttApplicationMessage message = message = new MqttApplicationMessageBuilder()
                    .WithTopic($"Edge/Response/test_vessel/GetRange")
                    .WithPayload(serialized)
                    .Build();

            await this.client.InternalClient.PublishAsync(message);

        }
    }

    public async Task OnChangesRequest(byte[] bytes)
    {

        Console.WriteLine("Handling Changes Request");
        ChangesRequest data = ChangesRequest.Parser.ParseFrom(bytes);

        Console.WriteLine($"Trying to find {data.Changes} changes for each signal");

        var collection = this.extractor.GetChanges(data.Signals.ToList(), data.Changes);

        DataPayload payload = new DataPayload();

        foreach (var entry in collection)
        {
            Entry current = new Entry
            {
                Name = entry.Key,
            };

            foreach (var value in entry.Value)
            {
                Value row = new Value
                {
                    Timestamp = value.Key,
                    Value_ = value.Value
                };
                current.Data.Add(row);
            }

            payload.Signals.Add(current);
        }

        using (var stream = new MemoryStream())
        {
            payload.WriteTo(stream);
            var serialized = stream.ToArray();

            MqttApplicationMessage message = message = new MqttApplicationMessageBuilder()
                    .WithTopic($"Edge/Response/test_vessel/GetChanges")
                    .WithPayload(serialized)
                    .Build();

            await this.client.InternalClient.PublishAsync(message);

        }
    }

    public async Task OnBoundsRequest(byte[] bytes)
    {
        Console.WriteLine("Handling Bounds Request");

        Range bounds = this.extractor.GetBounds();
        BoundsPayload payload = new BoundsPayload
        {
            From = bounds.from,
            To = bounds.to,
        };

        using (var stream = new MemoryStream())
        {
            payload.WriteTo(stream);
            var serialized = stream.ToArray();

            MqttApplicationMessage message = message = new MqttApplicationMessageBuilder()
                    .WithTopic($"Edge/Response/test_vessel/GetBounds")
                    .WithPayload(serialized)
                    .Build();

            await this.client.InternalClient.PublishAsync(message);
        }
    }

    public async Task OnSignalsRequest(byte[] bytes)
    {
        Console.WriteLine("Handling Signals Request");
        var collection = this.extractor.GetSignals();
        var payload = new SignalsPayload();

        foreach (SignalMetadata entry in collection)
        {
            var signal = new Signal
            {
                Id = entry.id,
                Name = entry.name,
            };
            if (entry.path != null)
            {
                signal.Path = entry.path;
            }
            payload.Signals.Add(signal);
        }

        using (var stream = new MemoryStream())
        {
            payload.WriteTo(stream);
            var serializedData = stream.ToArray();

            MqttApplicationMessage message = message = new MqttApplicationMessageBuilder()
                    .WithTopic($"Edge/Response/test_vessel/GetSignals")
                    .WithPayload(serializedData)
                    .Build();

            await this.client.InternalClient.PublishAsync(message);
        }
    }


}