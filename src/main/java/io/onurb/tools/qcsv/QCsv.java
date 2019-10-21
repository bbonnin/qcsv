package io.onurb.tools.qcsv;

import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.StringConverter;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class QCsv {

    @Parameter(names = "-q", required = true)
    private String query;

    @Parameter(names = "-d")
    private String delimiter = String.valueOf(QueryRunner.DEFAULT_DELIMITER);

    @Parameter(names = "-e")
    private String enclosureChar = QueryRunner.DEFAULT_ENCLOSURE_CHAR;

    @Parameter(names = "-i", required = true)
    private String input;

    @Parameter(names = "-l")
    private int maxAnalyzedLines = QueryRunner.DEFAULT_MAX_ANALYZED_LINES;

    @Parameter(names = "-T", converter = StringConverter.class)
    private List<String> allColTypes;

    @DynamicParameter(names = "-t")
    private Map<String, String> colTypes = new HashMap<>();

    @Parameter(names = "-c")
    private boolean allTypesAreVarchar = false;

    @Parameter(names = "-s")
    private boolean skipHeader = false;


    public static void main(String[] args) throws QException {

        final QCsv q = new QCsv();

        JCommander.newBuilder()
                .addObject(q)
                .build()
                .parse(args);

        q.run();
    }

    private void run() {
        try {
            System.out.println("Parameters:");
            System.out.println("\tInput file: " + input);
            System.out.println("\tQuery: " + query);
            System.out.println("\tDelimiter: " + delimiter);
            System.out.println("\tEnclosure char: " + enclosureChar);
            System.out.println("\tMax analyzed lines: " + maxAnalyzedLines);
            System.out.println("\tColumn types: " + colTypes);
            System.out.println("\tAll column types: " + allColTypes);
            System.out.println("\tConsider all types as varchar: " + allTypesAreVarchar);
            System.out.println("\tSkip header: " + skipHeader);

            final QueryRunner runner = new QueryRunner(input, delimiter.charAt(0), enclosureChar, maxAnalyzedLines,
                    query, allColTypes, colTypes, allTypesAreVarchar, skipHeader);
            runner.run();
        }
        catch (QException e) {
            e.printStackTrace();
        }
    }
}
