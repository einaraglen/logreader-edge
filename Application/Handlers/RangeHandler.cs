using Services.Compression;
using Models.Proto;
using CDP;
using Google.Protobuf;
using MQTT.Handler;
using MQTT.Message;

public class RangeHandler : IHandler
{
    public async Task OnMessage(string id, byte[] bytes)
    {
        try
        {
            Console.WriteLine("Handling Range Request");
            RangeRequest data = RangeRequest.Parser.ParseFrom(bytes);

            var collection = ExtractorSingleton.Instance.Extractor.GetRange(data.Signals.ToList(), (long)data.From, (long)data.To);

            DataPayload payload = new DataPayload();

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
                    .Publish();
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Failed to Handle Range request: \n{ex}");
        }

    }
}