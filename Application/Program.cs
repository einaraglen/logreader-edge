// using Services;

// MQTT client = new MQTT();

// await client.Connect();

// Console.ReadLine();

// await client.Disconnect();
using System.IO.Compression;
using CDP;
using Google.Protobuf;
using Models.Proto;
using Services.Compression;

Extractor extraction = new Extractor("./assets/basic");

var signal = "PT_REDUCED_LOAD_ACCUMULATOR";

var data = extraction.GetChanges(new List<string> { signal }, 10000);


var uncompressed = data[signal].Select(x => Convert.ToInt64(x.Key)).ToList();

var compressed = Delta2.Encode(data[signal]);
var compressedHalf = Delta2.Encode(data[signal].Skip(data[signal].Count / 2).ToDictionary(x => x.Key, x => x.Value));

TestPayload test = new TestPayload();

var blob = new Models.Proto.Blob();
var _blob = new Models.Proto.Blob2();

blob.Timestamp.AddRange(compressed);
_blob.Value.AddRange(data[signal].Select(x => x.Value).ToList());
// blob.Value.AddRange(data[signal].Select(x => x.Value).ToList());
test.Blobs.Add(blob);
test.Values.Add(_blob);

var blob2 = new Models.Proto.Blob();
var _blob2 = new Models.Proto.Blob2();


blob2.Timestamp.AddRange(compressedHalf);
_blob2.Value.AddRange(data[signal].Skip(data[signal].Count / 2).Select(x => x.Value).ToList());
// blob2.Value.AddRange(data[signal].Skip(data[signal].Count / 2).Select(x => x.Value).ToList());
test.Blobs.Add(blob2);
test.Values.Add(_blob2);

// Stream stream = new MemoryStream(uncompressed.SelectMany(x => BitConverter.GetBytes(x)).ToArray());
Stream output2 = new MemoryStream();

Stream output1 = new MemoryStream(test.ToByteArray());

var compressor = new DeflateStream(output2, CompressionMode.Compress);

output1.CopyTo(compressor);
compressor.Flush();

Console.WriteLine($"SIZE: {output2.Length}");

Stream output20 = new MemoryStream(); 

Stream stream2 = new MemoryStream(compressed.SelectMany(x => BitConverter.GetBytes(x)).ToArray());
Stream stream3 = new MemoryStream(compressedHalf.SelectMany(x => BitConverter.GetBytes(x)).ToArray());

var compressor2 = new DeflateStream(output20, CompressionMode.Compress);

stream2.CopyTo(compressor2);
stream3.CopyTo(compressor2);
compressor2.Flush();

Console.WriteLine($"Uncompress: {output1.Length} Compressed: {output20.Length}");


