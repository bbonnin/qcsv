package io.onurb.tools.qcsv;

import com.github.javafaker.Faker;

import java.io.FileWriter;
import java.io.IOException;
import java.util.Random;

public class DataGenerator {

    private static Faker faker = new Faker();

    private static Random r = new Random();

    public static void main(String[] args) throws IOException {

        FileWriter writer = new FileWriter("gen-data.csv");
        StringBuilder row = new StringBuilder();

        for (int i = 0; i < 100; i++) {
            int nbCols = r.nextInt(9) + 1;
            row.delete(0, row.length());

            for (int j = 0; j < nbCols; j++) {
                if (r.nextBoolean()) {
                    row.append(r.nextFloat());
                }
                else {
                    row.append(faker.food().ingredient());
                }

                row.append("|");
            }

            row.replace(row.length() - 1, row.length(), "\n");

            writer.append(row);
            writer.flush();
        }

        writer.close();
    }
}