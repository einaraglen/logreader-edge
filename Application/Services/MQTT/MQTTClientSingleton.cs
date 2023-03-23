using MQTTnet;
using MQTTnet.Client;
using MQTTnet.Extensions.ManagedClient;

public class MQTTClientSingleton
{
    private static readonly Lazy<MQTTClientSingleton> lazy =
        new Lazy<MQTTClientSingleton>(() => new MQTTClientSingleton());

    private IManagedMqttClient client;
    private List<string> topics = new List<string>();
    private bool connected = false;

    private MQTTClientSingleton()
    {
        string clientId = Environment.GetEnvironmentVariable("MQTT_CLIENT_ID")!;
        string address = Environment.GetEnvironmentVariable("MQTT_ADDRESS")!;
        string port = Environment.GetEnvironmentVariable("MQTT_PORT")!;

        MqttClientOptionsBuilder builder = new MqttClientOptionsBuilder()
                                                .WithClientId($"edge@{clientId}")
                                                .WithTcpServer(address, Convert.ToInt32(port));
        ManagedMqttClientOptions options = new ManagedMqttClientOptionsBuilder()
                                .WithAutoReconnectDelay(TimeSpan.FromSeconds(60))
                                .WithClientOptions(builder.Build())
                                .Build();
        this.client = new MqttFactory().CreateManagedMqttClient();
        this.LoadHandlers();
        this.client.StartAsync(options).Wait();
    }

    public MQTTClientSingleton Subscribe(string topic) {
        var builder = new MqttTopicFilterBuilder().WithTopic(topic).Build();
        this.client.SubscribeAsync(builder.Topic).Wait();
        this.topics.Add(topic);
        return this;
    }

    public void Complete() {
        Console.WriteLine($"Subscribed to {this.topics.Count} topics");
    }

    public MQTTClientSingleton AddMessageReceiver(Func<MQTTnet.Client.MqttApplicationMessageReceivedEventArgs, System.Threading.Tasks.Task> OnMessageReceived) {
        this.client.ApplicationMessageReceivedAsync += OnMessageReceived;
        return this;
    }

    public List<String> GetTopics() {
        return this.topics;
    }

    public bool IsConnected() {
        return this.connected;
    }

    public void Disconnect() {
       this.client.StopAsync().Wait();
    }

    public static MQTTClientSingleton Instance { get { return lazy.Value; } }

    public IMqttClient Client { get { return this.client.InternalClient; } }

     private void LoadHandlers() {
        this.client.ConnectedAsync += this.OnConnected;
        this.client.DisconnectedAsync += this.OnDisconected;
        this.client.ConnectingFailedAsync += this.OnFailure;
    }

    private Task OnConnected(MqttClientConnectedEventArgs arg)
    {
        Console.WriteLine("MQTT Connected");
        this.connected = true;
        return Task.CompletedTask;
    }

    private Task OnDisconected(MqttClientDisconnectedEventArgs arg)
    {
        Console.WriteLine("MQTT Disconnected");
        this.connected = false;
        return Task.CompletedTask;
    }

    private Task OnFailure(ConnectingFailedEventArgs arg)
    {
        Console.WriteLine("MQTT Connection failed check network or broker!");
        this.connected = false;
        return Task.CompletedTask;
    }
}