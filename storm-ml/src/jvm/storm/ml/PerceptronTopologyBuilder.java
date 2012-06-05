package storm.ml;

import java.lang.Integer;
import java.lang.Double;
import java.lang.Boolean;
import java.math.BigDecimal;
import java.util.List;
import java.util.ArrayList;

import org.javatuples.Pair;

public class PerceptronTopologyBuilder {
    public final Integer size;
    public final Double threshold;
    public final Double learning_rate;

    private List<BigDecimal> weights;

    public PerceptronTopologyBuilder(Integer size, Double threshold, Double learning_rate) {
        this.size = size;                   // size of the weight array and input
        this.threshold = threshold;         // margin to determine positive results
        this.learning_rate = learning_rate; // adaptation factor for the weights

        this.weights = new ArrayList<BigDecimal>(size);
        int i; for (i=0; i<size; i++)
            this.weights.add(new BigDecimal(0));
    }

    private BigDecimal dot_product(List<BigDecimal> vector1, List<BigDecimal> vector2) {
        BigDecimal result = new BigDecimal(0);
        int i; for (i=0; i<this.size; i++)
            result.add(vector1.get(i).multiply(vector2.get(i)));

        return result;
    }

    public void train(List<Pair<List<BigDecimal>, Boolean>> training_set) {
        while (true) {
            int error_count = 0;
            for (Pair<List<BigDecimal>, Boolean> training_pair : training_set) {
                List<BigDecimal> input_vector = training_pair.getValue0();
                Integer desired_output        = training_pair.getValue1() ? 1 : 0;
                System.out.println(String.format("%s", this.weights));

                int result = dot_product(input_vector, this.weights).compareTo(new BigDecimal(threshold)) > 0 ? 1 : 0;

                int error = desired_output - result;
                if (error != 0) {
                    error_count += 1;
                    int i; for (i=0; i<this.size; i++)
                        this.weights.set(i, this.weights.get(i).add(input_vector.get(i).multiply(new BigDecimal(this.learning_rate * error))));
                }
            }
            if (error_count == 0)
                break;
            System.out.println();
        }
    }
}
