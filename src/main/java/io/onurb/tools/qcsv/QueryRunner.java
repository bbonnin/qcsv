package io.onurb.tools.qcsv;

import lombok.extern.slf4j.Slf4j;

import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.commons.lang3.math.NumberUtils;
import org.joda.time.LocalDate;
import org.joda.time.LocalDateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.Reader;
import java.sql.*;
import java.util.*;

import com.opencsv.CSVParserBuilder;
import com.opencsv.CSVReader;
import com.opencsv.CSVReaderBuilder;


@Slf4j
public class QueryRunner {

    public static final String DEFAULT_ENCODING = "UTF-8";

    public static final String DEFAULT_ENCLOSURE_CHAR = "";

    public static final char DEFAULT_DELIMITER = ',';

    public static final int DEFAULT_MAX_ANALYZED_LINES = 50;

    private static final int VARCHAR_SIZE_RATIO = 4;

    private static final DateTimeFormatter SIMPLE_DATE = DateTimeFormat.forPattern("dd/MM/yyyy");

    private static final DateTimeFormatter HSQLDB_DATE = DateTimeFormat.forPattern("yyyy-MM-dd");

    private static final String DEFAULT_VARCHAR = "varchar(200)";

    private final String input;

    private final String encoding;

    private final int maxAnalyzedLines;

    private final String query;

    private final char delimiter;

    private final String enclosureChar;

    private final Connection db;

    /** Maximum number of columns found in the file.. */
    private int nbCols = -1;

    /** Max length of the value found for each column. */
    private final Map<String, Integer> strLenCols = new HashMap<>();

    /** Type of each column. */
    private final Map<String, String> colTypes = new HashMap<>();

    /** Consider all types as varchar. */
    private boolean allTypesAreVarchar;

    /** Skip header. */
    private boolean skipHeader;

    private String database;

    /**
     * Constructor.
     * @param input CSV file name
     * @param query SQL query to execute
     */
    public QueryRunner(String input, String query) throws QException {
        this(input, DEFAULT_ENCODING, DEFAULT_DELIMITER, DEFAULT_ENCLOSURE_CHAR, DEFAULT_MAX_ANALYZED_LINES, query, null, null, false, false);
    }

    /**
     * Constructor.
     * @param input CSV file name
     * @param query SQL query to execute
     */
    public QueryRunner(String input, String encoding, char delimiter, String query) throws QException {
        this(input, encoding, delimiter, DEFAULT_ENCLOSURE_CHAR, DEFAULT_MAX_ANALYZED_LINES, query, null, null, true, false);
    }

    /**
     * Constructor.
     * @param input CSV file name
     * @param delimiter Delimiter used in CSV file
     * @param enclosureChar Enclosure character
     * @param maxAnalyzedLines Max number of lines used to analyze the csv (to detect types, ...)
     * @param query SQL query to execute
     */
    public QueryRunner(String input, String encoding, char delimiter, String enclosureChar, int maxAnalyzedLines,
                       String query, List<String> allColTypes, Map<String, String> providedColTypes,
                       boolean allTypesAreVarchar, boolean skipHeader) throws QException {

        if (allColTypes != null && allColTypes.size() > 0
                && providedColTypes != null && providedColTypes.size() > 0) {
            throw new QException("You cannot provide all column types and specific types at the same time.");
        }

        this.input = input;

        // Some queries can contain the csv file name in the query (old queries for q.py)
        //
        if (query.contains(this.input)) {
            this.query = query.replace(input, "csv");
        }
        else {
            this.query = query;
        }

        this.encoding = encoding == null ? DEFAULT_ENCODING : encoding;
        this.delimiter = delimiter;
        this.enclosureChar = enclosureChar;
        this.maxAnalyzedLines = maxAnalyzedLines;
        this.allTypesAreVarchar = allTypesAreVarchar;
        this.skipHeader = skipHeader;

        if (allColTypes != null) {
            for (int i = 0; i < allColTypes.size(); i++) {
                final String colType = allColTypes.get(i);
                colTypes.put("c" + (i+1), colType);
            }
        }

        if (allTypesAreVarchar) {
            // Just add the first column type, the other ones will be added during insertion
            colTypes.put("c1", DEFAULT_VARCHAR);
        }

        try {
            this.database = System.getProperty("java.io.tmpdir") + "/qcsv_db_" + System.currentTimeMillis();
            this.db = DriverManager.getConnection("jdbc:hsqldb:file:" + database + "/qcsv","SA", "");
        }
        catch (SQLException e) {
            throw new QException("Database connection", e);
        }
    }

    public void clean() {
        try {

            this.db.createStatement().execute("shutdown");
            this.db.close();
            log.info("Delete " + this.database);




            File dir = new File(this.database);
            if (dir.exists()) {
                FileUtils.deleteDirectory(dir);
                boolean res = dir.delete();
                log.info("Delete " + this.database + ": " + res);
            }
        } catch (SQLException | IOException e) {
            log.error("Close the DB", e);
        }
    }

    /**
     * Start the execution of the query.
     *
     * @return Result set of the query
     * @throws QException
     */
    public ResultSet run() throws QException {
        prepareExecution();

        try {
            Statement stmt = db.createStatement();
            //String fct = "CREATE FUNCTION s2f(s " + DEFAULT_VARCHAR + ") returns float no sql language java parameter style java external name 'CLASSPATH:" + Convert.class.getName() + ".toFloat'";
            String fct = "create function tof(s " + DEFAULT_VARCHAR + ") returns float return to_number(replace(s, ',', '.'))";
            log.info(fct);
            stmt.execute(fct);

            fct = "CREATE FUNCTION toyyyymmdd(s " + DEFAULT_VARCHAR + ") RETURNS " + DEFAULT_VARCHAR +
                    " RETURN substr(s, 7, 4) || '-' || substr(s, 4, 2) || '-' || substr(s, 1, 2)";
            log.info(fct);
            stmt.execute(fct); // 06/07/2016 => 2016-07-06
        } catch (SQLException e) {
            e.printStackTrace();
        }

        final ResultSet resultSet = executeQuery();

        return resultSet;
    }

    /**
     * Start the execution of the query and get the result as it is a count.
     *
     * @return Count result
     * @throws QException
     */
    public Integer count() throws QException {
        Integer count = null;

        try {
            final ResultSet resultSet = run();

            if (resultSet != null && resultSet.next()) {
                count = resultSet.getInt(1);
            }
        }
        catch (SQLException e) {
            log.error("Get result of a count", e);
        }
        finally {
            clean();
        }

        return count;
    }

    /**
     * Start the execution of the query and get the result as a list of rows (using the provided delimiter).
     *
     * @return String with the rows
     * @throws QException
     */
    public String getRows() throws QException {
        final ResultSet resultSet = run();
        final StringBuilder buffer = new StringBuilder();

        try {
            if (resultSet != null) {
                final ResultSetMetaData metadata = resultSet.getMetaData();

                while (resultSet.next()) {
                    for (int i = 1; i <= metadata.getColumnCount(); i++) {
                        final Object val = resultSet.getObject(i);
                        buffer.append(val == null ? "" : val).append(this.delimiter);
                    }

                    buffer.delete(buffer.length() - 1, buffer.length()).append('\n');
                }

                buffer.delete(buffer.length() - 1, buffer.length());
            }
            else {
                System.err.println("No result");
            }
        }
        catch (SQLException e) {
            log.error("Get result as a string", e);
        }

        return buffer.toString();
    }

    /**
     * Prepare the context before executing the query.
     */
    private void prepareExecution() throws QException {
        try {
            if (colTypes.size() > 0) {
                log.info("No need to analyze: types have been provided");
            } else {
                analyzeCsv();
            }

            createTable();
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
    private void analyzeCsv() throws IOException, SQLException {

        log.info("Analyze the CSV file: {}", input);

        // First, read the first n lines for detecting number of columns, types of the columns
        //
        try (final Reader inputFile = new InputStreamReader(new FileInputStream(input), encoding)) {

            final CSVReader csvReader = getCsvReader(inputFile);
            String[] record;

            while ((record = csvReader.readNext()) != null) {
                if (csvReader.getLinesRead() > maxAnalyzedLines) break;

                nbCols = Math.max(nbCols, record.length);

                for (int i = 0; i < record.length; i++) {
                    final String colName = "c" + (i + 1);
                    final String value = getValue(record[i]);
                    final String type = getType(value);
                    log.debug("Type for {}: {}", value, type);

                    if (type != null) {
                        if ("varchar".equals(type)) {
                            colTypes.put(colName, "varchar");
                        } else {
                            String currentType = colTypes.get(colName);

                            if (currentType != null && !currentType.equals(type)) {
                                colTypes.put(colName, "varchar"); // On force à varchar
                            } else {
                                putIfAbsent(colTypes, colName, type);
                                //colTypes.putIfAbsent(colName, type); //TODO: Java 8
                            }
                        }
                    }

                    //strLenCols.put(colName, Math.max(strLenCols.getOrDefault(colName, 0), value.length())); //TODO: Java 8
                    strLenCols.put(colName, Math.max(strLenCols.containsKey(colName) ? strLenCols.get(colName) : 0, value.length()));
                }
            }
        }

        log.info("nbCols = {}", nbCols);
        log.info("strLenCols = {}", strLenCols);
        log.info("colTypes = {}", colTypes);
    }

    private void createTable() throws SQLException {

        log.info("Create the table");

        if (nbCols == -1) {
            nbCols = colTypes.size();
        }

        // Build the "create table..." query
        //
        final StringBuilder createQuery = new StringBuilder("create cached table csv (");

        for (int i = 0; i < nbCols; i++) {
            final String colName = "c" + (i+1);
            createQuery.append(colName).append(' ');
            createQuery.append(getSqlType(colName, colTypes.get(colName), strLenCols.get(colName))).append(',');
        }

        createQuery.replace(createQuery.length() - 1, createQuery.length(), ")");

        log.debug("CREATE TABLE QUERY: {}", createQuery);

        final Statement stmt = db.createStatement();
        stmt.executeQuery("drop table csv if exists");
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

        log.info("Load the CSV");

        int nbInserts = 0;
        int nbErrors = 0;
        long start = System.currentTimeMillis();

        try (final Reader inputFile = new InputStreamReader(new FileInputStream(input), encoding)) {

            final CSVReader csvReader = getCsvReader(inputFile);
            String[] record;

            final Statement stmt = db.createStatement();
            stmt.execute("SET FILES LOG FALSE;");

            while ((record = csvReader.readNext()) != null) {
                if (csvReader.getLinesRead() % 1000 == 0) {
                    log.debug("Load " + csvReader.getLinesRead() + " lines");
                }

                if (record.length > nbCols) {
                    // EXPERIMENTAL: when the number of columns is exceeded, create a ALTER TABLE for adding column

                    for (int i = nbCols; i < record.length; i++) {
                        final StringBuilder alterSql = new StringBuilder("alter table csv add ");
                        final String value = getValue(record[i]);
                        final String type = getType(value);
                        final String colName = "c" + (i+1);

                        colTypes.put(colName, type);

                        alterSql.append(colName).append(" ").append(getSqlType(colName, type, value.length()));

                        final Statement updateStmt = db.createStatement();
                        log.debug("UPDATE TABLE QUERY: " + alterSql);
                        updateStmt.executeQuery(alterSql.toString());
                    }

                    nbCols = record.length;
                }

                final StringBuilder query = new StringBuilder();
                query.append("insert into csv (");

                for (int i = 0; i < record.length; i++) {
                    query.append("c" + (i+1) + ",");
                }

                query.replace(query.length() - 1, query.length(), ") values (");

                for (int i = 0; i < record.length; i++) {
                    String value = getValue(record[i]);
                    String colName = "c" + (i+1);

                    if (StringUtils.isEmpty(value)) {
                        query.append("NULL,");
                    }
                    else if ("timestamp".equals(colTypes.get(colName))) {
                        query.append("'").append(value.replaceAll("T", " ")).append("',");
                    }
                    else if (colTypes.get(colName).endsWith("date")) {
                        query.append("'").append(convertDateFormat(value, colTypes.get(colName))).append("',");
                    }
                    else if (!"numeric".equals(colTypes.get(colName))) {
                        query.append("'").append(value.replaceAll("'", "''")).append("',");
                    }
                    else {
                        query.append(value).append(",");
                    }
                }

                query.replace(query.length() - 1, query.length(), ")");

                log.debug("INSERT QUERY: {}", query);

                try {
                    stmt.executeQuery(query.toString()); //TODO: batch mode
                    nbInserts++;
                }
                catch (Exception e) {
                    nbErrors++;
                    log.error("Bulk insert", e);
                }
            }
        }


        final long time = System.currentTimeMillis() - start;
        log.info("End of load ! Time: {} s ({} ms), nb inserts: {}, nb errors: {}", (time/1000), time, nbInserts, nbErrors);
    }

    /**
     * Run the SQL query on the csv file.
     *
     * @throws QException Any error with the processing
     */
    private ResultSet executeQuery() throws QException {
        log.info("Query execution: " + query);

        ResultSet resultSet;

        try {

            final Statement stmt = db.createStatement();
            resultSet = stmt.executeQuery(query);
        }
        catch (SQLException e) {
            throw new QException("Query execution", e);
        }

        log.info("End of the query execution");

        return resultSet;
    }

    /**
     * Convert a date as string to a date formatted for HSQLDB.
     *
     * TODO: utilisation des nouvelles classes DateXXX lors du passage à Java 8
     *
     * @param value Value of the date
     * @param type Type of date
     * @return String with date
     */
    private static String convertDateFormat(String value, String type) {
        try {
            if ("isodate".equals(type)) {
                return HSQLDB_DATE.print(ISODateTimeFormat.date().parseLocalDate(value)); //.format(DateTimeFormatter.ISO_DATE.parse(value));
            }
            if ("basicdate".equals(type)) {
                return HSQLDB_DATE.print(ISODateTimeFormat.basicDate().parseLocalDate(value)); //.format(DateTimeFormatter.BASIC_ISO_DATE.parse(value));
            }
            if ("simpledate".equals(type)) {
                return HSQLDB_DATE.print(SIMPLE_DATE.parseLocalDate(value)); //.format(SIMPLE_DATE.parse(value));
            }
        }
        catch (IllegalArgumentException e) {
            log.error("Convert " + value, e);
        }

        return value;
    }

    /**
     * Return the sql type according to the content of the column.
     *
     * TODO: utilisation des nouvelles classes DateXXX lors du passage à Java 8
     *
     * @param value Value of the column
     * @return Type
     */
    private String getType(String value) {
        if (allTypesAreVarchar) {
            return "varchar";
        }

        if (StringUtils.isEmpty(value)) {
            return null;
        }

        if (NumberUtils.isParsable(value)) {
            return "numeric";
        }

        // Detect date patterns
        try {
            LocalDate.parse(value, ISODateTimeFormat.date());
            return "isodate";
        }
        catch (IllegalArgumentException e) {
            // ignore
        }

        try {
            LocalDate.parse(value, ISODateTimeFormat.basicDate());
            return "basicdate";
        }
        catch (IllegalArgumentException e) {
            // ignore
        }

        try {
            LocalDate.parse(value, SIMPLE_DATE);
            return "simpledate";
        }
        catch (IllegalArgumentException e) {
            // ignore
        }

        try {
            LocalDate.parse(value, HSQLDB_DATE);
            return "date";
        }
        catch (IllegalArgumentException e) {
            // ignore
        }


        try {
            LocalDateTime.parse(value, ISODateTimeFormat.dateHourMinuteSecond());
            return "timestamp";
        }
        catch (IllegalArgumentException e) {
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
        if (allTypesAreVarchar) {
            return DEFAULT_VARCHAR;
        }

        if (type == null) {
            // Empty field
            type = "varchar";
            size = 50;

            putIfAbsent(strLenCols, colName, size);
            putIfAbsent(colTypes, colName, type);
            //strLenCols.putIfAbsent(colName, size); //TODO: Java 8
            //colTypes.putIfAbsent(colName, type); //TODO: Java 8
        }

        if ("varchar".equals(type)) {
            if (size == null || size == 0) {
                size = 50;
            }
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

    private CSVReader getCsvReader(Reader inputFile) {
        final CSVParserBuilder parserBuilder = new CSVParserBuilder()
                .withSeparator(delimiter)
                .withIgnoreQuotations(true);

        if (enclosureChar.length() > 0) {
            parserBuilder.withQuoteChar(enclosureChar.charAt(0));
        }

        final CSVReader csvReader = new CSVReaderBuilder(inputFile)
                .withCSVParser(parserBuilder.build())
                .withMultilineLimit(1)
                .build();

        return csvReader;
    }

    /**
     * Method to remove when this code will be compiled with Java 8.
     */
    private void putIfAbsent(Map map, Object key, Object value) {
        if (!map.containsKey(key)) {
            map.put(key, value);
        }
    }
}
