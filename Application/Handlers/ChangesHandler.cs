using Services.Compression;
using Models.Proto;
using CDP;
using MQTTnet;
using Google.Protobuf;

public class ChangesHandler : IHandler
{
    public void OnMessage(string id, byte[] bytes)
    {
        try
        {
            Console.WriteLine("Handling Changes Request");
            ChangesRequest data = ChangesRequest.Parser.ParseFrom(bytes);

            var collection = ExtractorSingleton.Instance.Extractor.GetChanges(data.Signals.ToList(), data.Changes);

            DataPayload payload = new DataPayload();

            string[] names = collection.Select(x => x.Key).ToArray();

            foreach (string name in names)
            {
                var logs = collection[name]!;
                var timestamps = logs.Select(x => x.Key).ToArray();
                var encoded = Delta2.Encode(timestamps);
                var values = logs.Select(x => x.Value);

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

                MqttApplicationMessage message = message = new MqttApplicationMessageBuilder()
                        .WithTopic($"Edge/Response/{Environment.GetEnvironmentVariable("MQTT_CLIENT_ID")!}/GetChanges")
                        .WithPayload(serialized)
                        .Build();

                MQTTClientSingleton.Instance.Client.PublishAsync(message).Wait();
            }
        }
        catch (Exception ex)
        {
            Console.WriteLine($"Failed to Handle Changes request: \n{ex}");
        }
    }
}