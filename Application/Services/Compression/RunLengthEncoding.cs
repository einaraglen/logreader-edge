namespace Services.Compression;

public class RunLengthEncoding
{
    public static Dictionary<double, double> Encode(Dictionary<double, double> input)
    {
        Dictionary<double, double> output = new Dictionary<double, double>();
        double currentKey = input.Keys.First();
        double currentValue = input[currentKey];
        int count = 1;

        foreach (double key in input.Keys.Skip(1))
        {
            double value = input[key];
            if (value == currentValue)
            {
                count++;
            }
            else
            {
                output.Add(currentKey, currentValue);
                output.Add(currentKey + count, currentValue + 0);
                currentKey = key;
                currentValue = value;
                count = 1;
            }
        }
        output.Add(currentKey, currentValue);
        output.Add(currentKey + count, currentValue + 0);

        return output;
    }

    public static Dictionary<double, double> Decode(Dictionary<double, double> input)
    {
        Dictionary<double, double> output = new Dictionary<double, double>();

        foreach (double key in input.Keys)
        {
            double value = input[key];
            if (key == input.Keys.Last())
            {
                output.Add(key, value);
            }
            else
            {
                double countKey = key + 1;
                double count = input[countKey];
                for (int i = 0; i < count; i++)
                {
                    output.Add(key + i, value);
                }
            }
        }

        return output;
    }
}