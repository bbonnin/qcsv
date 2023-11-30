package io.onurb.tools.qcsv;

import com.beust.jcommander.DynamicParameter;
import com.beust.jcommander.JCommander;
import com.beust.jcommander.Parameter;
import com.beust.jcommander.converters.StringConverter;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.hsqldb.jdbc.JDBCDriver;

public class QCsv {

    @Parameter(names = "-q", required = true)
    private String query;

    @Parameter(names = "-d")
    private String delimiter = String.valueOf(QueryRunner.DEFAULT_DELIMITER);

    @Parameter(names = "-e")
    private String enclosureChar = QueryRunner.DEFAULT_ENCLOSURE_CHAR;

    @Parameter(names = "-i", required = true, description = "Input CSV file")
    private String input;

    @Parameter(names = "-n", description = "File encoding")
    private String encoding = QueryRunner.DEFAULT_ENCODING;

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


    public static void main(String[] args) {

        final QCsv q = new QCsv();

        JCommander.newBuilder()
                .addObject(q)
                .build()
                .parse(args);

        q.run();
    }

    private void run() {
        QueryRunner runner;

        try {
            System.out.println("Parameters:");
            System.out.println("\tInput file: " + input);
            System.out.println("\tQuery: " + query);
            System.out.println("\tDelimiter: " + delimiter);
            System.out.println("\tEncoding: " + encoding);
            System.out.println("\tEnclosure char: " + enclosureChar);
            System.out.println("\tMax analyzed lines: " + maxAnalyzedLines);
            System.out.println("\tColumn types: " + colTypes);
            System.out.println("\tAll column types: " + allColTypes);
            System.out.println("\tConsider all types as varchar: " + allTypesAreVarchar);
            System.out.println("\tSkip header: " + skipHeader);
            System.out.println();

            runner = new QueryRunner(input, encoding, delimiter.charAt(0), enclosureChar, maxAnalyzedLines,
                                     query, allColTypes, colTypes, allTypesAreVarchar, skipHeader);

            int count = runner.count();

            System.out.println("COUNT=" + count);

/*            final ResultSet resultSet = runner.run();

            if (resultSet != null) {
                final ResultSetMetaData metadata = resultSet.getMetaData();

                System.out.println("\nResult:");

                int l = 1;

                while (resultSet.next()) {
                    final StringBuilder result = new StringBuilder();
                    result.append(l++).append(" | ");

                    for (int i = 1; i <= metadata.getColumnCount(); i++) {
                        result.append(resultSet.getObject(i)).append(" | ");
                    }

                    System.out.println(result.delete(result.length() - 3, result.length()));
                }
            }
            else {
                System.err.println("No result");
            }
        }
        catch (QException | SQLException e){
            e.printStackTrace();
        }
        finally {
            if (runner != null) runner.close();
        }*/
        }
        catch (QException e) {
            e.printStackTrace();
        }
    }
}
