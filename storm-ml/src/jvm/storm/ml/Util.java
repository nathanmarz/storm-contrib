package storm.ml;

import java.util.ArrayList;
import java.util.List;
import java.util.Scanner;

public class Util {
    public static List<Double> parse_str_vector(String str_vector) {
        List<Double> vector = new ArrayList<Double>();

        Scanner scanner = new Scanner(str_vector.substring(1, str_vector.length()-1));
        scanner.useDelimiter(", ");

        while (scanner.hasNextDouble())
            vector.add(scanner.nextDouble());

        return vector;
    }
}
