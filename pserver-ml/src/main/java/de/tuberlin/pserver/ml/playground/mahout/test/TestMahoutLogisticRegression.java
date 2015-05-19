package de.tuberlin.pserver.ml.playground.mahout.test;

import de.tuberlin.pserver.ml.playground.mahout.Adultincome.AdultIncomeClassificationMain;

public class TestMahoutLogisticRegression {

    public static void main(String[] args) {

        try {
            AdultIncomeClassificationMain.main(args);
        } catch (Exception e) {
            e.printStackTrace();
        }

    }
}
