using Services;

MQTT client = new MQTT();

await client.Connect();

Console.ReadLine();

await client.Disconnect();