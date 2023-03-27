using CDP;
using SeaBrief.DotEnv;
using SeaBrief.MQTT;

class Application
{
    static void Main(string[] args)
    {
        try
        {
            DotEnv.Load(new string[] { "LOG_DIRECTORY", "MQTT_CLIENT_ID", "MQTT_ADDRESS", "MQTT_PORT", "MQTT_SERVICE_NAME" });

            var client = Environment.GetEnvironmentVariable("MQTT_CLIENT_ID")!;
            var service = Environment.GetEnvironmentVariable("MQTT_SERVICE_NAME")!;
            var address = Environment.GetEnvironmentVariable("MQTT_ADDRESS")!;
            var port = Environment.GetEnvironmentVariable("MQTT_PORT")!;

            ExtractorSingleton.Instance.Extractor.Load();

            MQTTClientSingleton.Instance.Connect($"RemoteEdge@{client}", address, port);

            MQTTClientSingleton.Instance
            .AddTopic($"Edge/{client}/{service}/+/Request");

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