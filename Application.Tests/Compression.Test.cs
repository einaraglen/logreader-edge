using System.IO.Compression;
using CDP;
using Services.Compression;

namespace Application.Tests;

public class CompressionTest
{
    [Fact]
    public void CompressDelta2EncodeDecode()
    {
        Extractor extraction = new Extractor("C:/Users/monga/Documents/projects/logreader-edge/assets/basic");

        var signal = "PT_REDUCED_LOAD_ACCUMULATOR";

        var data = extraction.GetChanges(new List<string> { signal }, 10000);

        var encoded = Delta2.Encode(data[signal]);

        var decoded = Delta2.Decode(encoded);

        Assert.True(encoded.Length == decoded.Length, $"Encoded: {encoded.Length}, Decoded: {decoded.Length}");
    }

    [Fact]
    public void CompressDelta2SizeComparison()
    {
        Extractor extraction = new Extractor("C:/Users/monga/Documents/projects/logreader-edge/assets/basic");

        var signal = "PT_REDUCED_LOAD_ACCUMULATOR";

        var data = extraction.GetChanges(new List<string> { signal }, 10000);

        var encoded = Delta2.Encode(data[signal]);

        var decoded = Delta2.Decode(encoded);

        var encodedSize = GetSizeOf(encoded.SelectMany(x => BitConverter.GetBytes(x)).ToArray());
        var decodedSize = GetSizeOf(decoded.SelectMany(x => BitConverter.GetBytes(x)).ToArray());

        Assert.True(encodedSize < decodedSize, "Compression failed to reduce size");
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