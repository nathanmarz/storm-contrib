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

    public static Double dot_product(List<Double> vector_a, List<Double> vector_b) {
        Double result = 0.0;

        for (int i=0; i<vector_a.size(); i++)
            result += vector_a.get(i) * vector_b.get(i);

        return result;
    }
}
