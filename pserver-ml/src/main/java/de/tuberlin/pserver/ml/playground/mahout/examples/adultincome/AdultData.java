package de.tuberlin.pserver.ml.playground.mahout.examples.adultincome;

import de.tuberlin.pserver.math.DVector;
import de.tuberlin.pserver.math.Vector;
import de.tuberlin.pserver.ml.playground.mahout.encoders.ConstantValueEncoder;
import de.tuberlin.pserver.ml.playground.mahout.encoders.FeatureVectorEncoder;
import de.tuberlin.pserver.ml.playground.mahout.encoders.StaticWordValueEncoder;

import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.Map;

public class AdultData {
  public static final int FEATURES = 100;
  private static final ConstantValueEncoder interceptEncoder = new ConstantValueEncoder("intercept");
  private static final FeatureVectorEncoder featureEncoder = new StaticWordValueEncoder("feature");

  private DVector vector;

  private Map<String, String> fields = new LinkedHashMap<>();

  public AdultData(Iterable<String> fieldNames, Iterable<String> values) {
    vector = new DVector(FEATURES, Vector.VectorType.COLUMN_VECTOR);
    Iterator<String> value = values.iterator();
    interceptEncoder.addToVector("1", vector);
    for (String name : fieldNames) {
      String fieldValue = value.next();
      fields.put(name, fieldValue);

      switch (name) {
        case "age": {
          if(fieldValue != null && fieldValue.length() > 0 ) {
            double v = Double.parseDouble(fieldValue);
            if(v == 0){
              featureEncoder.addToVector(name, 1, vector);
            }
            else
              featureEncoder.addToVector(name, Math.log(v), vector);
            break;
          }
          else
           featureEncoder.addToVector(name,1,vector);
          break;
        }
        case "education-num": {
          double v = Double.parseDouble(fieldValue);
          if(v == 0){
            featureEncoder.addToVector(name, 1, vector);
          }
          else
            featureEncoder.addToVector(name, Math.log(v), vector);
          break;
        }
        case "capital-gain": {
          double v = Double.parseDouble(fieldValue);
          if(v == 0){
            featureEncoder.addToVector(name, 1, vector);
          }
          else
            featureEncoder.addToVector(name, Math.log(v), vector);
          break;
        }
        case "capital-loss": {
          double v = Double.parseDouble(fieldValue);
          if(v == 0){
            featureEncoder.addToVector(name, 1, vector);
          }
          else
            featureEncoder.addToVector(name, Math.log(v), vector);
          break;
        }
        case "hours-per-week": {
          double v = Double.parseDouble(fieldValue);
          if(v == 0){
            featureEncoder.addToVector(name, 1, vector);
          }
          else
            featureEncoder.addToVector(name, Math.log(v), vector);
          break;
        }
        case "workclass":
        case "fnlwgt":
        case "education":
        case "marital-status":
        case "occupation":
        case "relationship":
        case "race":
        case "sex":
        case "native-country":
          featureEncoder.addToVector(name + ":" + fieldValue, 1, vector);
          break;
        case "label":
          // ignore these for vectorizing
          break;
        default:
          throw new IllegalArgumentException(String.format("Bad field name: %s", name));
      }
    }
  }

  public Vector asVector() {
    return vector;
  }

  public int getTarget() {
    return fields.get("label").equals("<=50K") ? 0 : 1;
  }
}
