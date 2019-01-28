package com.cassandra.util;

import org.apache.poi.ss.usermodel.Cell;
import org.apache.poi.ss.usermodel.DataFormatter;
import org.apache.poi.ss.usermodel.Row;
import org.apache.poi.ss.usermodel.Sheet;
import org.apache.poi.xssf.streaming.SXSSFWorkbook;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.datasource.SimpleDriverDataSource;

import java.io.FileInputStream;
import java.io.FileOutputStream;
import java.io.PrintStream;
import java.sql.Driver;
import java.sql.ResultSetMetaData;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Created by natarajan.iyer on 1/26/19.
 */
public class DataLoader {

    private static final SimpleDriverDataSource SIMPLE_DRIVER_DATA_SOURCE = new SimpleDriverDataSource();
    private static String headerSql, dataSql, dbUrl, dbUserName, dbPassword, personCompanyExportFileName;
    static private Pattern rxquote = Pattern.compile("\"");

    public static void main(String[] args) throws Throwable {

        Properties properties = new Properties();

        if (args.length != 1)
        {
            System.out.println("Invalid number of args, correct usage is DataLoader /usr/local/loader.properties");
            return;
        }

        if (args.length == 1) {
            properties.load(new FileInputStream(args[0]));
        }

        properties.keySet().forEach(prop -> readProperty(properties, prop));

        //input validation
        if (null == dbUrl || null == dbUserName || null == dbPassword)
        {
            System.out.println("db url or username or password is not provided in input file");
            return;
        }

        if (null == headerSql || null == dataSql)
        {
            System.out.println("headerSql or dataSql is not given in input file");
            return;
        }

        if (null == personCompanyExportFileName)
        {
            System.out.println("personCompanyExportFileName is not given in input file");
            return;
        }

        SimpleDriverDataSource dataSource = SIMPLE_DRIVER_DATA_SOURCE;
        dataSource.setDriver((Driver) Class.forName("com.mysql.jdbc.Driver").newInstance());
        dataSource.setUrl(dbUrl);
        dataSource.setUsername(dbUserName);
        dataSource.setPassword(dbPassword);

        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);

        //get header columns
        List<String> headerFields = getHeaderFields(jdbcTemplate, headerSql);

        //build the with clause
        StringBuilder builder = new StringBuilder();

        //build count sql
        builder = new StringBuilder();
        builder.append("select count(*) from ( ");
        builder.append(dataSql).append(" ) as person_company");
        String countSql = builder.toString();

        Integer maxRowCnt = jdbcTemplate
                .query(countSql,
                        new Object[]{},
                        (rs, rowNum) -> {
                            return rs.getInt(1);
                        }).stream().findFirst().orElse(null);

        // spawn tasks if maxRowCnt > 100000
        int numThreads = 1;
        if (maxRowCnt > 100000)
        {
            numThreads = (maxRowCnt/100000) + 1;
        }

        //sql query to parallel process results by chunks of 50,000
        builder = new StringBuilder();
        builder.append("select * from ( ");
        builder.append(dataSql).append(" ) as person_company where row_num between ? and ?");
        String parallelSql = builder.toString();

        ExecutorService executor = Executors.newFixedThreadPool(4);
        List<Callable<String>> tasks = new ArrayList();

        //create parallel tasks
        int startRow = 0;
        long startTime = System.currentTimeMillis();
        System.out.println("Start time = " + startTime);
        for (int i=0; i<numThreads; i++)
        {
            int endRow = startRow + 100000;
            Callable<String> task = splitResultsByRowCount("task_" + i, headerFields, parallelSql, startRow, endRow-1);
            tasks.add(task);
            startRow = endRow;
        }

        executor.invokeAll(tasks).stream()
                .map(future -> {
                    try {
                        return future.get();
                    }
                    catch (Exception e) {
                        throw new IllegalStateException(e);
                    }
                })
                .forEach(System.out::println);

        long endTime = System.currentTimeMillis();
        long elapsedTime = (endTime - startTime)/1000L;
        System.out.println("End time = " + endTime);
        System.out.println("Elapsed time in seconds = " + elapsedTime);

        executor.shutdown();

    }

    private static void readProperty(Properties propFile, Object prop) {
        String propName = (String)prop;
        if ("headerSql".equals(prop)) {
            headerSql = propFile.getProperty(propName);
        } else if ("dataSql".equals(prop)) {
            dataSql = propFile.getProperty(propName);
        } else if ("db.url".equals(prop)) {
            dbUrl = propFile.getProperty(propName);
        } else if ("db.username".equals(prop)) {
            dbUserName = propFile.getProperty(propName);
        } else if ("db.password".equals(prop)) {
            dbPassword = propFile.getProperty(propName);
        } else if ("personCompanyExportFileName".equals(prop)) {
            personCompanyExportFileName = propFile.getProperty(propName);
        }
    }

    private static List<String> getHeaderFields(JdbcTemplate jdbcTemplate,
                                                String headerSql) {
        List<String> headerFields = jdbcTemplate
                .query(headerSql,
                        new Object[]{},
                        (rs, rowNum) -> {
                            ResultSetMetaData rsmd = rs.getMetaData();
                            List<String> fieldHeaders = new ArrayList<String>();
                            for (int i=1; i < rsmd.getColumnCount(); i++)
                            {
                                Object headerValue = rs.getObject(i+1);
                                fieldHeaders.add((headerValue != null)?headerValue.toString():null);
                            }
                            return fieldHeaders;
                        }).stream().findFirst().orElse(null);
        return headerFields;
    }

    private static Callable<String> splitResultsByRowCount(String taskName, List<String> headerFields, String parallelSql, int startRow, int endRow) {
        return () -> {

            StringBuilder outputFileNameBuilder = new StringBuilder();
            outputFileNameBuilder.append(personCompanyExportFileName).append("_").append((startRow/100000) + 1).append(".csv");

//            DataFormatter formatter = new DataFormatter();
            PrintStream out = new PrintStream(new FileOutputStream(outputFileNameBuilder.toString()),
                    true, "UTF-8");

            //create the header row
            //skip the first column (rownum column)
            boolean firstHeaderCell = true;
            for(int cellnum = 0; cellnum < headerFields.size(); cellnum++){
                if ( ! firstHeaderCell ) out.print(',');
                out.print(encodeValue(headerFields.get(cellnum)));
                firstHeaderCell = false;
            }
            out.println();

            JdbcTemplate jdbcTemplate = new JdbcTemplate(SIMPLE_DRIVER_DATA_SOURCE);
            final AtomicInteger rowCnt = new AtomicInteger(0);
            jdbcTemplate
                    .query(parallelSql,
                            new Object[]{startRow, endRow},
                            (rs, rowNum) -> {
                                ResultSetMetaData rsmd = rs.getMetaData();
                                List<String> dataValues = new ArrayList<String>();
                                //skip the first column (row num)
                                for (int i=1; i < rsmd.getColumnCount()-1; i++)
                                {
                                    Object dataValue = rs.getObject(i+1);
                                    dataValues.add((dataValue != null)?dataValue.toString():null);
                                }
                                return dataValues;
                            }).stream().forEach(dataFields -> {
                int cnt = rowCnt.incrementAndGet();
                if (cnt % 1000 == 0 || cnt == endRow)
                {
                    System.out.println("current row cnt = " + rowCnt);
                }
                boolean firstCell = true;
                for(int cellnum = 0; cellnum < dataFields.size(); cellnum++){
                    if ( ! firstCell ) out.print(',');
                    out.print(encodeValue(dataFields.get(cellnum)));
                    firstCell = false;
                }
                out.println();
            });

            out.close();

            return taskName;

        };
    }

    static private String encodeValue(String value) {
        if (value == null) {
            return "";
        }
        boolean needQuotes = false;
        if ( value.indexOf(',') != -1 || value.indexOf('"') != -1 ||
                value.indexOf('\n') != -1 || value.indexOf('\r') != -1 )
            needQuotes = true;
        Matcher m = rxquote.matcher(value);
        if ( m.find() ) needQuotes = true; value = m.replaceAll("\"\"");
        if ( needQuotes ) return "\"" + value + "\"";
        else return value;
    }
}
