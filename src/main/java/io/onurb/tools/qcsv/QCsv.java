package io.onurb.tools.qcsv;

import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;

public class QCsv {

    @Parameter(names = "-q", required = true)
    private String query;

    @Parameter(names = "-d")
    private String delimiter = QueryRunner.DEFAULT_DELIMITER;

    @Parameter(names = "-i", required = true)
    private String input;

    @Parameter(names = "-l")
    private int maxAnalyzedLines = QueryRunner.DEFAULT_MAX_ANALYZED_LINES;


    public static void main(String[] args) throws QException {

        //String delimiter = "\t";
        //String input = "/Users/bruno/Downloads/11";
        //String query = "select * from t0 where c2 = 690";

        //String delimiter = "\\|";
        //String input = "gen-data.csv";
        //int analyzeMaxLines = 20;
        //String query = "select * from t0 where c1 = 'Beans'";

        QCsv q = new QCsv();

        JCommander.newBuilder()
                .addObject(q)
                .build()
                .parse(args);

        q.run();
    }

    private void run() {
        try {
            System.out.println("Paramters:");
            System.out.println("\tInput file: " + input);
            System.out.println("\tQuery: " + query);
            System.out.println("\tDelimiter: " + delimiter);
            System.out.println("\tMax analyzed lines: " + maxAnalyzedLines);
            final QueryRunner runner = new QueryRunner(input, delimiter, maxAnalyzedLines, query);
            runner.run();
        }
        catch (QException e) {
            e.printStackTrace();
        }
    }
}
