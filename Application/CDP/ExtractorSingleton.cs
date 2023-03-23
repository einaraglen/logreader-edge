using System;
using System.IO;

namespace CDP;

public class ExtractorSingleton
{
    private static readonly Lazy<ExtractorSingleton> lazy =
            new Lazy<ExtractorSingleton>(() => new ExtractorSingleton());

    private Extractor extracor;
    private ExtractorSingleton()
    {
        this.extracor = new Extractor(Environment.GetEnvironmentVariable("LOG_DIRECTORY")!);
    }

    public static ExtractorSingleton Instance { get { return lazy.Value; } }

    public Extractor Extractor { get { return this.extracor; } }
}