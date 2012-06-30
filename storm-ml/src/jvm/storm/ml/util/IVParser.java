package storm.ml.util;

import java.util.ArrayList;
import java.util.List;

public class IVParser {
    public static List<List<Double>> parse(String filename) {
        Double n=1.0;

        List<List<Double>> result = new ArrayList<List<Double>>();
        while (n<10) {
            List<Double> result_item = new ArrayList<Double>();
            result_item.add(n++);
            result_item.add(n*n);

            result.add(result_item);
        }

        return result;
    }
}
