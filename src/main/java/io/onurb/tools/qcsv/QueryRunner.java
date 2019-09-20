package io.onurb.tools.qcsv;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.FileUtils;
import org.apache.commons.io.LineIterator;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.*;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.time.format.DateTimeParseException;
import java.util.*;

@Slf4j
public class QueryRunner {

    public static final String DEFAULT_DELIMITER = ",";

    public static final int DEFAULT_MAX_ANALYZED_LINES = 50;

    private static final int VARCHAR_SIZE_RATIO = 4;

    private final String input;

    private final int maxAnalyzedLines;

    private final String query;

    private final String delimiter;

    private final Connection db;

    /** Maximum number of columns found in the file.. */
    private int nbCols = 0;

    /** Max length of the value found for each column. */
    private Map<String, Integer> strLenCols = new HashMap<>();

    /** Type of each column. */
    private Map<String, String> colTypes = new HashMap<>();

    /**
     * Constructor.
     * @param input CSV file name
     * @param query SQL query to execute
     */
    public QueryRunner(String input, String query) throws QException {
        this(input, DEFAULT_DELIMITER, DEFAULT_MAX_ANALYZED_LINES, query);
    }

    /**
     * Constructor.
     * @param input CSV file name
     * @param delimiter Delimiter used in CSV file
     * @param maxAnalyzedLines Max number of lines used to analyze the csv (to detect types, ...)
     * @param query SQL query to execute
     */
    public QueryRunner(String input, String delimiter, int maxAnalyzedLines, String query) throws QException {
        this.input = input;
        this.delimiter = delimiter;
        this.maxAnalyzedLines = maxAnalyzedLines;
        this.query = query;

        try {
            this.db = DriverManager.getConnection("jdbc:hsqldb:mem:qcsv", "SA", "");
        }
        catch (SQLException e) {
            throw new QException("Database connection", e);
        }
    }

    /**
     * Start the execution of the query.
     *
     * @throws QException
     */
    public void run() throws QException {
        prepareExecution();
        executeQuery();
    }

    /**
     * Prepare the context before executing the query.
     */
    private void prepareExecution() throws QException {
        try {
            analyzeCsv();
            importCsv();
        }
        catch (IOException | SQLException e) {
            throw new QException("Error when preparing the execution", e);
        }
    }

    /**
     * Before running this method must be invoked to create a table in HSQLDB.
     *
     * @throws FileNotFoundException
     * @throws SQLException
     */
    private void analyzeCsv() throws FileNotFoundException, SQLException {

        int analyzedLines = 0;


        // First, read the first n lines for detecting number of columns, types of the columns
        //
        try (final Scanner scanner = new Scanner(new File(input))) {
            while (scanner.hasNextLine() && analyzedLines < maxAnalyzedLines) {
                analyzedLines++;

                final List<String> cols = getRecordFromLine(scanner.nextLine(), delimiter);

                nbCols = Math.max(nbCols, cols.size());

                for (int i = 0; i < cols.size(); i++) {
                    final String colName = "c" + i;
                    final String value = cols.get(i);

                    final String type = getType(value);

                    if (type != null) {
                        if ("varchar".equals(type)) {
                            colTypes.put(colName, "varchar");
                        } else {
                            colTypes.putIfAbsent(colName, type);
                        }
                    }

                    strLenCols.put(colName, Math.max(strLenCols.getOrDefault(colName, 0), value.length()));
                }
            }
        }

        log.debug("nbCols = {}", nbCols);
        log.debug("strLenCols = {}", strLenCols);
        log.debug("colTypes = {}", colTypes);


        // Then, build the "create table..." query
        //
        final StringBuilder createQuery = new StringBuilder("create table t0 (");

        for (int i = 0; i < nbCols; i++) {
            final String colName = "c" + i;
            createQuery.append(colName).append(' ');
            createQuery.append(getSqlType(colTypes.get(colName), strLenCols.get(colName))).append(',');
        }

        createQuery.replace(createQuery.length() - 1, createQuery.length(), ")");

        log.debug("CREATE TABLE QUERY: {}", createQuery);

        final Statement stmt = db.createStatement();
        stmt.executeQuery(createQuery.toString());
    }


    /**
     * Import the CSV in the database.
     *
     * @throws FileNotFoundException The input has not been found
     * @throws SQLException Any errors with the database
     */
    private void importCsv() throws IOException, SQLException {

        final File inputFile = new File(input);

        try (LineIterator scanner = FileUtils.lineIterator(inputFile, "UTF-8")) {

            while (scanner.hasNext()) {
                final List<String> cols = getRecordFromLine(scanner.nextLine(), delimiter);

                if (cols.size() > nbCols) {
                    // EXPERIMENTAL: when the number of columns is exceeded, create a ALTER TABLE for adding column

                    for (int i = nbCols; i < cols.size(); i++) {
                        final StringBuilder alterSql = new StringBuilder("alter table t0 add ");
                        final String type = getType(cols.get(i));
                        final String colName = "c" + i;

                        colTypes.put(colName, type);

                        alterSql.append(colName).append(" ").append(getSqlType(type, cols.get(i).length()));

                        final Statement stmt = db.createStatement();
                        stmt.executeQuery(alterSql.toString());
                    }
                }

                final StringBuilder query = new StringBuilder();
                query.append("insert into t0 (");

                for (int i = 0; i < cols.size(); i++) {
                    query.append("c" + i + ",");
                }

                query.replace(query.length() - 1, query.length(), ") values (");

                for (int i = 0; i < cols.size(); i++) {
                    if (StringUtils.isEmpty(cols.get(i))) {
                        query.append("NULL,");
                    }
                    else if ("timestamp".equals(colTypes.get("c" + i))) {
                        query.append("'").append(cols.get(i).replaceAll("T", " ")).append("',");
                    }
                    else if (!"numeric".equals(colTypes.get("c" + i))) {
                        query.append("'").append(cols.get(i)).append("',");
                    }
                    else {
                        query.append(cols.get(i)).append(",");
                    }
                }

                query.replace(query.length() - 1, query.length(), ")");

                log.debug("INSERT QUERY: {}", query);

                final Statement stmt = db.createStatement();
                stmt.executeQuery(query.toString());
            }
        }
    }

    /**
     * Run the SQL query on the csv file.
     *
     * @throws QException Any error with the processing
     */
    private void executeQuery() throws QException { //TODO: what kind of output ? Map ? ResultSet ?
        try {
            final Statement stmt = db.createStatement();
            final ResultSet resultSet = stmt.executeQuery(query);

            while (resultSet.next()) {
                log.debug(resultSet.getString("c0"));
            }
        }
        catch (SQLException e) {
            throw new QException("Query execution", e);
        }
    }


    /**
     * Return the sql type according to the content of the column.
     *
     * @param value Value of the column
     * @return Type
     */
    private static String getType(String value) {
        if (StringUtils.isEmpty(value)) {
            return null;
        }

        if (NumberUtils.isParsable(value)) {
            return "numeric";
        }

        // Detect date patterns
        try {
            LocalDate.parse(value, DateTimeFormatter.ISO_DATE);
            return "date";
        }
        catch (DateTimeParseException e) {
            // ignore
        }

        try {
            LocalDateTime.parse(value, DateTimeFormatter.ISO_DATE_TIME);
            return "timestamp";
        }
        catch (DateTimeParseException e) {
            // ignore
        }

        return "varchar";
    }

    /**
     * Return the SQL for building create/alter table queries.
     *
     * @param type Type of the column
     * @param size Max size found for a column (used for varchar)
     * @return SQL type
     */
    private static String getSqlType(String type, Integer size) {
        if ("varchar".equals(type)) {
            return "varchar(" + (size * VARCHAR_SIZE_RATIO) + ")";
        }

        return type;
    }

    /**
     * Read a line and return all elements.
     *
     * @param line A line in the csv file
     * @param delimiter Delimiter string to use
     * @return A list of elements
     */
    private List<String> getRecordFromLine(String line, String delimiter) {
        final List<String> values = new ArrayList<>();

        try (Scanner rowScanner = new Scanner(line)) {
            rowScanner.useDelimiter(delimiter);
            while (rowScanner.hasNext()) {
                values.add(rowScanner.next());
            }
        }

        return values;
    }
}
