package io.onurb.tools.qcsv;

import org.apache.commons.lang3.math.NumberUtils;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.*;
import java.util.*;

public class QCsv {

    private static final int VARCHAR_SIZE_RATIO = 4;

    private String input;

    private String delimiter;

    private int analyzeMaxLines;

    private int nbCols = 0;

    private Map<String, Integer> strLenCols = new HashMap<>();

    private Map<String, String> colTypes = new HashMap<>();

    private Connection db;


    public static void main(String[] args) throws SQLException, IOException {


        //String delimiter = "\t";
        //String input = "/Users/bruno/Downloads/11";
        //String query = "select * from t0 where c2 = 690";

        String delimiter = "\\|";
        String input = "gen-data.csv";
        int analyzeMaxLines = 20;
        String query = "select * from t0 where c1 = 'Beans'";

        QCsv q = new QCsv(input, delimiter, analyzeMaxLines);
        q.analyzeCsv();
        q.importCsv();
        q.executeQuery(query);
    }

    /**
     * Constructor.
     * @param input CSV file name
     * @param delimiter Delimiter used in CSV file
     * @param analyzeMaxLines Max number of lines used to analyze the csv (to detect types, ...)
     */
    public QCsv(String input, String delimiter, int analyzeMaxLines) throws SQLException, FileNotFoundException {
        this.input = input;
        this.delimiter = delimiter;
        this.analyzeMaxLines = analyzeMaxLines;

        this.db = DriverManager.getConnection("jdbc:hsqldb:mem:qcsv", "SA", "");
    }

    private void importCsv() throws FileNotFoundException, SQLException {

        List<String> cols;

        int nbRows = 0;


        //File f = new File(input);
        //try (LineIterator scanner = FileUtils.lineIterator(f, "UTF-8")) {
        //  while (scanner.hasNext()) {


        try (Scanner scanner = new Scanner(new File(input))) {
            while (scanner.hasNextLine()) {

                cols = getRecordFromLine(scanner.nextLine(), delimiter);

                if (cols.size() > nbCols) {
                    // Quand le nombre de colonnes est dépassé, faire un ALTER TABLE
                    // EXPERIMENTAL

                    for (int i = nbCols; i < cols.size(); i++) {
                        StringBuilder alterSql = new StringBuilder("alter table t0 add ");
                        String type = getType(cols.get(i));
                        colTypes.put("c" + i, type);
                        alterSql.append("c").append(i).append(" ").append(getSqlType(type, cols.get(i).length()));
                        Statement stmt2 = db.createStatement();
                        stmt2.executeQuery(alterSql.toString());
                    }
                }

                StringBuilder query = new StringBuilder();
                //query.delete(0, query.length()); //TODO: Performance ?
                query.append("insert into t0 (");

                for (int i = 0; i < cols.size(); i++) {
                    query.append("c" + i + ",");
                }

                query.replace(query.length() - 1, query.length(), ") values (");


                for (int i = 0; i < cols.size(); i++) {
                    if ("str".equals(colTypes.get("c" + i))) {
                        query.append("'").append(cols.get(i)).append("',");
                    } else {
                        query.append(cols.get(i)).append(",");
                    }
                }

                query.replace(query.length() - 1, query.length(), ")");

                System.out.println("==> INSERT QUERY: " + query);

                Statement stmt2 = db.createStatement();
                stmt2.executeQuery(query.toString());

                if (++nbRows % 1000 == 0) {
                    //System.out.println("Lignes chargées: " + nbRows);
                }
            }
        }
    }

    private void executeQuery(String sql) throws SQLException {
        PreparedStatement stmt = db.prepareStatement(sql);
        stmt.execute();
        ResultSet resultSet = stmt.getResultSet();

        while (resultSet.next()) {
            System.out.println(resultSet.getString("c1"));
        }

        // load data
        // get samples or get a description (ot create the database)
        // load each row ? could be very long.....
        // version "bourrine"
        // - lecture ligne par ligne -> copptage du nombre d'items et construction de la requete d'insert

    }



    private static String getType(String value) {
        if (NumberUtils.isParsable(value)) {
            return "num";
        }

        return "str";
    }

    private static String getSqlType(String type, Integer size) {
        if ("str".equals(type)) {
            return "varchar(" + (size * VARCHAR_SIZE_RATIO) + ")";
        }

        return "numeric";
    }

    private void analyzeCsv() throws FileNotFoundException, SQLException {

        try (final Scanner scanner = new Scanner(new File(input))) {
            while (scanner.hasNextLine() && analyzeMaxLines > 0) {
                analyzeMaxLines--;

                final List<String> cols = getRecordFromLine(scanner.nextLine(), delimiter);

                nbCols = Math.max(nbCols, cols.size());

                for (int i = 0; i < cols.size(); i++) {
                    String colName = "c" + i;
                    String value = cols.get(i);

                    // TODO: detection du format date à partir de quelques patterns
                    if (NumberUtils.isParsable(value)) {
                        String type = colTypes.get(colName);
                        if (type == null) {
                            colTypes.put(colName, "num");
                        }
                    }
                    else {
                        colTypes.put(colName, "str");
                    }

                    strLenCols.put(colName, Math.max(strLenCols.getOrDefault(colName, 0), value.length()));
                }
            }
        }

        System.out.println("==> nbCols=" + nbCols);
        System.out.println("==> strLenCols=" + strLenCols);
        System.out.println("==> colTypes=" + colTypes);


        StringBuilder createQuery = new StringBuilder("create table t0 (");

        for (int i = 0; i < nbCols; i++) {
            String colName = "c" + i;
            createQuery.append(colName).append(' ');
            createQuery.append(getSqlType(colTypes.get(colName), strLenCols.get(colName))).append(',');
        }

        createQuery.replace(createQuery.length() - 1, createQuery.length(), ")");

        System.out.println("==> CREATE TABLE QUERY: " + createQuery);

        PreparedStatement stmt = db.prepareStatement(createQuery.toString());
        stmt.execute();

    }

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
