﻿using CDP;
using MQTT;
using MQTT.Message;

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

            MQTTClientSingleton.Instance.Connect(client, address, port);


            var requests = new MessageReceiver("^Edge/Request/(?<id>[^/]+)/(?<endpoint>[^/]+)?$")
            .WithHandler("GetRange", new RangeHandler())
            .WithHandler("GetChanges", new ChangesHandler())
            .WithHandler("GetBounds", new BoundsHandler())
            .WithHandler("GetCount", new CountHandler())
            .WithHandler("GetSignals", new SignalsHandler());

            MQTTClientSingleton.Instance.AddMessageReceiver(requests.OnMessageReceived);

            MQTTClientSingleton.Instance
            .WithListener($"Edge/Request/{client}/GetRange")
            .WithListener($"Edge/Request/{client}/GetChanges")
            .WithListener($"Edge/Request/{client}/GetBounds")
            .WithListener($"Edge/Request/{client}/GetCount")
            .WithListener($"Edge/Request/{client}/GetSignals")
            .Subscribe();

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