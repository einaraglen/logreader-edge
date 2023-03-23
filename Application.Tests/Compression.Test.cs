using System.IO.Compression;
using CDP;
using Google.Protobuf;
using Models.Proto;
using Services.Compression;

namespace Application.Tests;

public class CompressionTest
{
    private string root = "C:/repos/logreader-edge/assets";

    [Fact]
    public void CompressDelta2EncodeDecode()
    {
        Extractor extraction = new Extractor($"{root}/basic");

        var signal = "PT_REDUCED_LOAD_ACCUMULATOR";

        var data = extraction.GetChanges(new List<string> { signal }, 10000);

        var encoded = Delta2.Encode(data[signal].Select(x => x.Key).ToArray());

        var decoded = Delta2.Decode(encoded);

        Assert.True(encoded.Length == decoded.Length, $"Encoded: {encoded.Length}, Decoded: {decoded.Length}");
    }

    [Fact]
    public void CompressDelta2AndProtobuffDecodeEncode()
    {
        Extractor extraction = new Extractor($"{root}/basic");

        var signal = "PT_REDUCED_LOAD_ACCUMULATOR";

        var data = extraction.GetChanges(new List<string> { signal }, 10000);

        var encoded = Delta2.Encode(data[signal].Select(x => x.Key).ToArray());

        var protobuf = new DataPayload();

        var timestamps = new Timestamps();

        var values = new Values();

        timestamps.Entries.AddRange(encoded);

        values.Entries.AddRange(data[signal].Select(x => x.Value).ToList());

        protobuf.Signals.Add(signal);
        protobuf.Timestamps.Add(timestamps);
        protobuf.Values.Add(values);

        byte[] bytes = GetByteArrayFromProtobuf(protobuf);

        var fromBytes = DataPayload.Parser.ParseFrom(bytes);

        var reproduction = new Dictionary<string, Dictionary<double, double>>();

        for (int i = 0; i < fromBytes.Signals.Count(); i++)
        {
            var name = fromBytes.Signals[i];
            var logs = fromBytes.Values[i].Entries;
            var encodedTimestamps = fromBytes.Timestamps[i];
            var decoded = Delta2.Decode(encodedTimestamps.Entries.ToArray());

            var log = new Dictionary<double, double>();

            for (int j = 0; j < decoded.Length; j++)
            {
                log.Add(decoded[j], logs[j]);
            }

            reproduction.Add(name, log);
        }

        var before = data[signal].ToList();

        var after = reproduction[signal].ToList();

        for (int i = 0; i < before.Count; i++)
        {
            var currBefore = before[i];
            var currAfter = after[i];

            Assert.True(currBefore.Key == currAfter.Key, $"Keys at index {i} are not matching after reprodution ({currBefore.Key}, {currAfter.Key})");
            Assert.True(currBefore.Value == currAfter.Value, $"Values at index {i} are not matching after reprodution ({currBefore.Value}, {currAfter.Value})");
        }

    }

    [Fact]
    public void CompressDelta2SizeComparison()
    {
        Extractor extraction = new Extractor($"{root}/basic");

        var signal = "PT_REDUCED_LOAD_ACCUMULATOR";

        var data = extraction.GetChanges(new List<string> { signal }, 10000);

        var encoded = Delta2.Encode(data[signal].Select(x => x.Key).ToArray());

        var decoded = Delta2.Decode(encoded);

        var encodedSize = GetSizeOf(encoded.SelectMany(x => BitConverter.GetBytes(x)).ToArray());
        var decodedSize = GetSizeOf(decoded.SelectMany(x => BitConverter.GetBytes(x)).ToArray());

        Assert.True(encodedSize < decodedSize, "Compression failed to reduce size");
    }

    private byte[] GetByteArrayFromProtobuf(DataPayload protobuf)
    {
        using (var stream = new MemoryStream())
        {
            protobuf.WriteTo(stream);
            return stream.ToArray();

        }
    }


    private long GetSizeOf(byte[] bytes)
    {
        Stream output = new MemoryStream();

        Stream input = new MemoryStream(bytes);

        var compressor2 = new DeflateStream(output, CompressionMode.Compress);

        input.CopyTo(compressor2);
        compressor2.Flush();

        return output.Length;
    }
}