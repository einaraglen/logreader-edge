// using Services;

// MQTT client = new MQTT();

// await client.Connect();

// Console.ReadLine();

// await client.Disconnect();
using System.IO.Compression;
using CDP;
using Google.Protobuf;
using Models.Proto;

Int64[] Delta2Encode(Dictionary <double, double> input) {
    List<Int64> values = input.Select(x => Convert.ToInt64(x.Key)).ToList();
    Int64[] compressed = new Int64[values.Count];

   compressed[0] = values[0];
    Int64 curDelta = values[1] - values[0];
    compressed[1] = curDelta;
    
    int i = 2;
    while (i < values.Count) {
      Int64 delta = values[i] - values[i-1];
      Int64 delta2 = delta - curDelta;
      compressed[i] = delta2;
      curDelta = delta;
      i++;
    }

    return compressed;
}

Int64[] Delta2Decode(Dictionary <double, double> input) {
    List<Int64> values = input.Select(x => Convert.ToInt64(x.Key)).ToList();
    Int64[] decompressed = new Int64[values.Count];

    decompressed[0] = values[0];

    Int64 curDelta = 0;
    for (int i = 1; i < values.Count; i++) {
      curDelta += values[i];
      decompressed[i] = decompressed[i-1] + curDelta;
    }

    return decompressed;
}

Extractor extraction = new Extractor("./assets/basic");

var signal = "PT_REDUCED_LOAD_ACCUMULATOR";

var data = extraction.GetChanges(new List<string> { signal }, 10000);


var uncompressed = data[signal].Select(x => Convert.ToInt64(x.Key)).ToList();

var compressed = Delta2Encode(data[signal]);
var compressedHalf = Delta2Encode(data[signal].Skip(data[signal].Count / 2).ToDictionary(x => x.Key, x => x.Value));

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


