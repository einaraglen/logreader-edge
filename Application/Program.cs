using CDP;
using LogReaderLibrary.DotEnv;
using LogReaderLibrary.MQTT;

class Application
{
    static void Main(string[] args)
    {
        try
        {
            DotEnv.Load(new string[] { "LOG_DIRECTORY", "MQTT_CLIENT_ID", "MQTT_ADDRESS", "MQTT_PORT" });

            var client = Environment.GetEnvironmentVariable("MQTT_CLIENT_ID")!;
            var address = Environment.GetEnvironmentVariable("MQTT_ADDRESS")!;
            var port = Environment.GetEnvironmentVariable("MQTT_PORT")!;

            ExtractorSingleton.Instance.Extractor.Load();

            MQTTClientSingleton.Instance.Connect($"RemoteEdge@{client}", address, port);

            MQTTClientSingleton.Instance
            .AddTopic($"Edge/Request/{client}/GetRange")
            .AddTopic($"Edge/Request/{client}/GetLast")
            .AddTopic($"Edge/Request/{client}/GetBounds")
            .AddTopic($"Edge/Request/{client}/GetCount")
            .AddTopic($"Edge/Request/{client}/GetSignals");

            MQTTClientSingleton.Instance
            .AddMessageReceiver(new BoundsHandler())
            .AddMessageReceiver(new CountHandler())
            .AddMessageReceiver(new RangeHandler())
            .AddMessageReceiver(new SignalsHandler())
            .AddMessageReceiver(new LastHandler());


            Console.Read();
        }
        catch (Exception ex)
        {
            Console.WriteLine(ex);
        }
        finally
        {
            MQTTClientSingleton.Instance.Disconnect();
            ExtractorSingleton.Instance.Extractor.Close();
        }
    }
}