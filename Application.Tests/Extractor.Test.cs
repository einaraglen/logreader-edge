using CDP;

namespace Application.Tests;

public class Utils {
    public static string DoubleToString(double number) {
        return number.ToString("0.00", System.Globalization.CultureInfo.InvariantCulture);
    }
}

public class ExtractorTest
{
    [Fact]
    public void BasicSupport()
    {
        Extractor extractor = new Extractor("C:/Users/monga/Documents/projects/logreader-edge/assets/basic");
        var signals = extractor.GetSignals();
        Assert.True(signals.Count > 0, "Extractor failed to read Basic");
    }

    [Fact]
    public void CompacSupport()
    {
        Extractor extractor = new Extractor("C:/Users/monga/Documents/projects/logreader-edge/assets/compact");
        var signals = extractor.GetSignals();
        Assert.True(signals.Count > 0, "Extractor failed to read Compact");
    }

    [Fact]
    public void SplitSupport()
    {
        Extractor extractor = new Extractor("C:/Users/monga/Documents/projects/logreader-edge/assets/split");
        var signals = extractor.GetSignals();
        Assert.True(signals.Count > 0, "Extractor failed to read Split");
    }

    [Fact]
    public void BasicChanges()
    {
        Extractor extractor = new Extractor("C:/Users/monga/Documents/projects/logreader-edge/assets/basic");
        Random rnd = new Random();

        var signals = extractor.GetSignals();

        var signal = signals[rnd.Next(signals.Count)];

        var values = extractor.GetChanges(new List<string>{ signal.name }, 10);

        Assert.Equal(10, values[signal.name].Values.Count);
    }

    [Fact]
    public void CompactChanges()
    {
        Extractor extractor = new Extractor("C:/Users/monga/Documents/projects/logreader-edge/assets/compact");
        Random rnd = new Random();

        var signals = extractor.GetSignals();

        var signal = signals[rnd.Next(signals.Count)];

        var values = extractor.GetChanges(new List<string>{ signal.name }, 10);

        Assert.Equal(10, values[signal.name].Values.Count);
    }

    [Fact]
    public void SplitChanges()
    {
        Extractor extractor = new Extractor("C:/Users/monga/Documents/projects/logreader-edge/assets/split");
        Random rnd = new Random();

        var signals = extractor.GetSignals();

        var signal = signals[rnd.Next(signals.Count)];


        var values = extractor.GetChanges(new List<string>{ "CombiPRT_FC2_FBSpeedRef" }, 5);

        Assert.Equal(5, values["CombiPRT_FC2_FBSpeedRef"].Values.Count);
    }
}