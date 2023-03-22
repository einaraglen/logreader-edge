using MQTTnet.Client;
using MQTTnet.Extensions.ManagedClient;
using MQTTnet;
using MQTTnet.Packets;
using System.Text.RegularExpressions;
using CDP;

namespace Services;

public class MQTT
{
    const string VESSEL = "test_vessel";

    const string RANGE = $"Edge/Request/{VESSEL}/GetRange";
    const string SIGNALS = $"Edge/Request/{VESSEL}/GetSignals";
    const string CHANGES = $"Edge/Request/{VESSEL}/GetChanges";
    const string BOUNDS = $"Edge/Request/{VESSEL}/GetBounds";
    private IManagedMqttClient client;
    private MQTTResponder responder;
    public MQTT()
    {

        this.client = new MqttFactory().CreateManagedMqttClient();
        this.responder = new MQTTResponder(this.client, "./assets/split");
        this.Handlers();
    }

    public async Task Connect()
    {
        MqttClientOptionsBuilder builder = new MqttClientOptionsBuilder()
                                                .WithClientId("EDGE")
                                                .WithTcpServer("localhost", 1883);
        ManagedMqttClientOptions options = new ManagedMqttClientOptionsBuilder()
                                .WithAutoReconnectDelay(TimeSpan.FromSeconds(60))
                                .WithClientOptions(builder.Build())
                                .Build();
        await this.client.StartAsync(options);
        await this.Subscribe();
    }

    public async Task Disconnect()
    {
        await this.client.StopAsync();
    }

    private async Task Subscribe()
    {
        ICollection<MqttTopicFilter> topics = new List<MqttTopicFilter>();
        topics.Add(new MqttTopicFilterBuilder().WithTopic(RANGE).Build());
        topics.Add(new MqttTopicFilterBuilder().WithTopic(SIGNALS).Build());
        topics.Add(new MqttTopicFilterBuilder().WithTopic(CHANGES).Build());
        topics.Add(new MqttTopicFilterBuilder().WithTopic(BOUNDS).Build());
        await this.client.SubscribeAsync(topics);

    }

    private void Handlers()
    {
        this.client.ConnectedAsync += this.OnConnected;
        this.client.DisconnectedAsync += this.OnDisconected;
        this.client.ConnectingFailedAsync += this.OnFailure;
        this.client.ApplicationMessageReceivedAsync += this.OnMessageReceived;
    }

    private static Match DeconstructTopic(string topic)
    {

        string regex = $"^Edge/Request/(?<id>[^/]+)/(?<endpoint>[^/]+)?$";
        return Regex.Match(topic, regex);
    }

    private async Task OnMessageReceived(MqttApplicationMessageReceivedEventArgs arg)
    {
        byte[] bytes = arg.ApplicationMessage.Payload;
        Match topic = DeconstructTopic(arg.ApplicationMessage.Topic);

        if (topic.Success == false)
        {
            return;
        }

        string endpoint = topic.Groups["endpoint"].Value;


        switch (endpoint)
        {
            case "GetRange":
                await this.responder.OnRangeRequest(bytes);
                break;
            case "GetSignals":
                await this.responder.OnSignalsRequest(bytes);
                break;
            case "GetChanges":
                await this.responder.OnChangesRequest(bytes);
                break;
            case "GetBounds":
                await this.responder.OnBoundsRequest(bytes);
                break;
            default:
                Console.WriteLine("Unknown topic", topic);
                break;
        }
    }

    private Task OnConnected(MqttClientConnectedEventArgs arg)
    {
        Console.WriteLine("Connected");
        return Task.CompletedTask;
    }

    private Task OnDisconected(MqttClientDisconnectedEventArgs arg)
    {
        Console.WriteLine("Disconnected");
        return Task.CompletedTask;
    }

    private Task OnFailure(ConnectingFailedEventArgs arg)
    {
        Console.WriteLine("Connection failed check network or broker!");
        return Task.CompletedTask;
    }
}