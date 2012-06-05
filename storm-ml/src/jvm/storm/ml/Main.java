package storm.ml;

import java.lang.Boolean;
import java.math.BigDecimal;
import java.util.List;
import java.util.ArrayList;

import org.javatuples.Pair;

import storm.ml.PerceptronTopologyBuilder;

public class Main {
  public static void main(String[] keywords) {
      List<Pair<List<BigDecimal>, Boolean>> training_set = new ArrayList<Pair<List<BigDecimal>, Boolean>>(4);
      List<BigDecimal> input_vector = new ArrayList<BigDecimal>(3);

      input_vector.add(new BigDecimal(1));
      input_vector.add(new BigDecimal(0));
      input_vector.add(new BigDecimal(0));
      training_set.add(new Pair<List<BigDecimal>, Boolean>(input_vector, true));

      input_vector.add(new BigDecimal(1));
      input_vector.add(new BigDecimal(0));
      input_vector.add(new BigDecimal(1));
      training_set.add(new Pair<List<BigDecimal>, Boolean>(input_vector, true));

      input_vector.add(new BigDecimal(1));
      input_vector.add(new BigDecimal(1));
      input_vector.add(new BigDecimal(0));
      training_set.add(new Pair<List<BigDecimal>, Boolean>(input_vector, true));

      input_vector.add(new BigDecimal(1));
      input_vector.add(new BigDecimal(1));
      input_vector.add(new BigDecimal(1));
      training_set.add(new Pair<List<BigDecimal>, Boolean>(input_vector, false));

      PerceptronTopologyBuilder ptb = new PerceptronTopologyBuilder(3, 0.5, 0.1);
      ptb.train(training_set);
  }
}
