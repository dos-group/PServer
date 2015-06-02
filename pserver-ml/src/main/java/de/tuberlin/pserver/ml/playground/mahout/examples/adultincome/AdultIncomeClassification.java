package de.tuberlin.pserver.ml.playground.mahout.examples.adultincome;

import com.google.common.collect.Lists;
import de.tuberlin.pserver.ml.playground.mahout.L1;
import de.tuberlin.pserver.ml.playground.mahout.OnlineLogisticRegression;
import de.tuberlin.pserver.ml.playground.mahout.evaluation.Auc;

import java.util.Collections;
import java.util.List;

/** * Uses the SGD classifier on the 'Adult' dataset from UCI.
 *
 * See http://archive.ics.uci.edu/ml/datasets/Adult
 *
 * Learn the income of people based on their information.
 */
public class AdultIncomeClassification {

  public static final int NUM_CATEGORIES = 2;

  public static void main(String[] args) throws Exception {
    List<AdultData> adults = Lists.newArrayList(new AdultDataParser("/home/therb/Development/Projects/pserver/datasets/adult.data.txt"));

    double heldOutPercentage = 0.10;

    for (int run = 0; run < 20; run++) {
      Collections.shuffle(adults);
      int cutoff = (int) (heldOutPercentage * adults.size());
      List<AdultData> test = adults.subList(0, cutoff);
      List<AdultData> train = adults.subList(cutoff, adults.size());

      OnlineLogisticRegression lr = new OnlineLogisticRegression(NUM_CATEGORIES, AdultData.FEATURES, new L1())
        .learningRate(1)
        .alpha(1)
        .lambda(0.000001)
        .stepOffset(10000)
        .decayExponent(0.2);

      for (int pass = 0; pass < 20; pass++) {
        for (AdultData adultData : train) {
          lr.train(adultData.getTarget(), adultData.asVector());
        }
        if (pass % 5 == 0) {
          Auc eval = new Auc(0.5);
          for (AdultData testCall : test) {
            eval.add(testCall.getTarget(), lr.classifyScalar(testCall.asVector()));
          }
          System.out.printf("%d, %.4f, %.4f\n", pass, lr.currentLearningRate(), eval.auc());
        }
      }
    }
  }
}
