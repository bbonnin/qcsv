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

    public static final String DEFAULT_ENCLOSURE_CHAR = "";

    public static final String DEFAULT_DELIMITER = ",";

    public static final int DEFAULT_MAX_ANALYZED_LINES = 50;

    private static final int VARCHAR_SIZE_RATIO = 4;

    private static final DateTimeFormatter SIMPLE_DATE = DateTimeFormatter.ofPattern("dd/MM/yyyy");

    private static final DateTimeFormatter HSQLDB_DATE = DateTimeFormatter.ofPattern("yyyy-MM-dd");

    private final String input;

    private final int maxAnalyzedLines;

    private final String query;

    private final String delimiter;

    private final String enclosureChar;

    private final Connection db;

    /** Maximum number of columns found in the file.. */
    private int nbCols = 0;

    /** Max length of the value found for each column. */
    private Map<String, Integer> strLenCols = new HashMap<>();

    /** Type of each column. */
    private Map<String, String> colTypes = new HashMap<>();

    /** Types of the columns provided on the command line. */
    private List<String> providedColTypes;

    /**
     * Constructor.
     * @param input CSV file name
     * @param query SQL query to execute
     */
    public QueryRunner(String input, String query) throws QException {
        this(input, DEFAULT_DELIMITER, DEFAULT_ENCLOSURE_CHAR, DEFAULT_MAX_ANALYZED_LINES, query, null);
    }

    /**
     * Constructor.
     * @param input CSV file name
     * @param delimiter Delimiter used in CSV file
     * @param enclosureChar Enclosure character
     * @param maxAnalyzedLines Max number of lines used to analyze the csv (to detect types, ...)
     * @param query SQL query to execute
     */
    public QueryRunner(String input, String delimiter, String enclosureChar, int maxAnalyzedLines, String query, List<String> providedColTypes) throws QException {
        this.input = input;
        this.delimiter = delimiter;
        this.enclosureChar = enclosureChar;
        this.maxAnalyzedLines = maxAnalyzedLines;
        this.query = query;
        this.providedColTypes = providedColTypes;

        if (providedColTypes != null) {
            for (int i = 0; i < providedColTypes.size(); i++) {
                final String colType = providedColTypes.get(i);
                colTypes.put("c" + i, colType);
            }
        }

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

        System.out.println("\nAnalyze CSV...");

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
                    final String value = getValue(cols.get(i));
                    final String type = getType(value);
                    log.debug("Type for {}: {}", value, type);

                    if (type != null) {
                        if ("varchar".equals(type)) {
                            colTypes.put(colName, "varchar");
                        }
                        else {
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
            createQuery.append(getSqlType(colName, colTypes.get(colName), strLenCols.get(colName))).append(',');
        }

        createQuery.replace(createQuery.length() - 1, createQuery.length(), ")");

        log.debug("CREATE TABLE QUERY: {}", createQuery);

        final Statement stmt = db.createStatement();
        stmt.executeQuery(createQuery.toString());
    }

    /**
     * Returns the value (without enclosure chars).
     *
     * @param rawValue The raw value
     * @return Value without enclouse chars or empty string
     */
    private String getValue(String rawValue) {

        if (StringUtils.isNoneEmpty(enclosureChar) &&
            rawValue.startsWith(enclosureChar) &&
            rawValue.endsWith(enclosureChar)) {

            if (rawValue.length() == enclosureChar.length() * 2) {
                return "";
            }

            try {
                return rawValue.substring(1, rawValue.length() - 1);
            } catch (StringIndexOutOfBoundsException e) {
                System.err.println("|" + rawValue + "|");
                return rawValue;
            }
        }

        return rawValue;
    }

    /**
     * Import the CSV in the database.
     *
     * @throws FileNotFoundException The input has not been found
     * @throws SQLException Any errors with the database
     */
    private void importCsv() throws IOException, SQLException {

        System.out.println("\nLoad CSV...");

        final File inputFile = new File(input);

        try (LineIterator scanner = FileUtils.lineIterator(inputFile, "UTF-8")) {

            while (scanner.hasNext()) {
                final List<String> cols = getRecordFromLine(scanner.nextLine(), delimiter);

                if (cols.size() > nbCols) {
                    // EXPERIMENTAL: when the number of columns is exceeded, create a ALTER TABLE for adding column

                    for (int i = nbCols; i < cols.size(); i++) {
                        final StringBuilder alterSql = new StringBuilder("alter table t0 add ");
                        final String value = getValue(cols.get(i));
                        final String type = getType(value); //TODO: date type
                        final String colName = "c" + i;

                        colTypes.put(colName, type);

                        alterSql.append(colName).append(" ").append(getSqlType(colName, type, value.length()));

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
                    String value = getValue(cols.get(i));

                    if (StringUtils.isEmpty(value)) {
                        query.append("NULL,");
                    }
                    else if ("timestamp".equals(colTypes.get("c" + i))) {
                        query.append("'").append(value.replaceAll("T", " ")).append("',");
                    }
                    else if (colTypes.get("c" + i).endsWith("date")) {
                        query.append("'").append(convertDateFormat(value, colTypes.get("c" + i))).append("',");
                    }
                    else if (!"numeric".equals(colTypes.get("c" + i))) {
                        query.append("'").append(value.replaceAll("'", "''")).append("',");
                    }
                    else {
                        query.append(value).append(",");
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
            System.out.println("\nQuery execution...");
            final Statement stmt = db.createStatement();
            final ResultSet resultSet = stmt.executeQuery(query);
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
        catch (SQLException e) {
            throw new QException("Query execution", e);
        }
    }

    private String convertDateFormat(String value, String type) {
        try {
            if ("isodate".equals(type)) {
                return HSQLDB_DATE.format(DateTimeFormatter.ISO_DATE.parse(value));
            }
            if ("basicdate".equals(type)) {
                return HSQLDB_DATE.format(DateTimeFormatter.BASIC_ISO_DATE.parse(value));
            }
            if ("simpledate".equals(type)) {
                return HSQLDB_DATE.format(SIMPLE_DATE.parse(value));
            }
        }
        catch (DateTimeParseException e) {
            log.error("Convert " + value, e);
        }

        return value;
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
            return "isodate";
        }
        catch (DateTimeParseException e) {
            // ignore
        }

        try {
            LocalDate.parse(value, DateTimeFormatter.BASIC_ISO_DATE);
            return "basicdate";
        }
        catch (DateTimeParseException e) {
            // ignore
        }

        try {
            LocalDate.parse(value, SIMPLE_DATE);
            return "simpledate";
        }
        catch (DateTimeParseException e) {
            // ignore
        }

        try {
            LocalDate.parse(value, HSQLDB_DATE);
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
     * @param colName Name of the column used to update global informations about types
     * @param type Type of the column
     * @param size Max size found for a column (used for varchar)
     * @return SQL type
     */
    private String getSqlType(String colName, String type, Integer size) {
        if (type == null) {
            // Empty field
            type = "varchar";
            size = 50;

            strLenCols.putIfAbsent(colName, size);
            colTypes.putIfAbsent(colName, type);
        }

        if ("varchar".equals(type)) {
            return "varchar(" + (size * VARCHAR_SIZE_RATIO) + ")";
        }

        if (type.startsWith("varchar")) {
            return type;
        }

        if (type.endsWith("date")) {
            return "date";
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
            rowScanner.useDelimiter(delimiter + "(?=([^\\\"]*\\\"[^\\\"]*\\\")*[^\\\"]*$)");
            while (rowScanner.hasNext()) {
                values.add(rowScanner.next());
            }
        }

        //System.out.println(values);

        return values;
    }
}
