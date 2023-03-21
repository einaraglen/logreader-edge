public class Utils {
    public static string DoubleToString(double number) {
        return number.ToString("0.00", System.Globalization.CultureInfo.InvariantCulture);
    }
}