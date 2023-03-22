namespace Services.Compression;

public class Delta2 {
    public static long[] Encode(Dictionary <double, double> input) {
    long[] values = input.Select(x => Convert.ToInt64(x.Key)).ToArray();
    long[] compressed = new long[values.Length];

   compressed[0] = values[0];
    long last = values[1] - values[0];
    compressed[1] = last;
    
    int i = 2;
    while (i < values.Length) {
      long delta = values[i] - values[i-1];
      long delta2 = delta - last;
      compressed[i] = delta2;
      last = delta;
      i++;
    }

    return compressed;
}

public static long[] Decode(Dictionary <double, double> input) {
    long[] values = input.Select(x => Convert.ToInt64(x.Key)).ToArray();
    long[] decompressed = new long[values.Length];

    decompressed[0] = values[0];

    long last = 0;
    for (int i = 1; i < values.Length; i++) {
      last += values[i];
      decompressed[i] = decompressed[i-1] + last;
    }

    return decompressed;
}
}