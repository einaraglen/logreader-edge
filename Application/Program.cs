using CDP;

class Application
{
    static void Main(string[] args)
    {
        try
        {
            DotEnv.Load(new string[] { "LOG_DIRECTORY", "MQTT_CLIENT_ID", "MQTT_ADDRESS", "MQTT_PORT" });

            ExtractorSingleton.Instance.Extractor.Load();

            var requests = new MessageReceiver("^Edge/Request/(?<id>[^/]+)/(?<endpoint>[^/]+)?$");

            requests
            .AddHandler("GetRange", new RangeHandler())
            .AddHandler("GetChanges", new ChangesHandler())
            .AddHandler("GetBounds", new BoundsHandler())
            .AddHandler("GetCount", new CountHandler())
            .AddHandler("GetSignals", new SignalsHandler());

            MQTTClientSingleton.Instance.AddMessageReceiver(requests.OnMessageReceived);

            MQTTClientSingleton.Instance
            .Subscribe($"Edge/Request/{Environment.GetEnvironmentVariable("MQTT_CLIENT_ID")!}/GetRange")
            .Subscribe($"Edge/Request/{Environment.GetEnvironmentVariable("MQTT_CLIENT_ID")!}/GetChanges")
            .Subscribe($"Edge/Request/{Environment.GetEnvironmentVariable("MQTT_CLIENT_ID")!}/GetBounds")
            .Subscribe($"Edge/Request/{Environment.GetEnvironmentVariable("MQTT_CLIENT_ID")!}/GetCount")
            .Subscribe($"Edge/Request/{Environment.GetEnvironmentVariable("MQTT_CLIENT_ID")!}/GetSignals")
            .Complete();

            Console.ReadLine();
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