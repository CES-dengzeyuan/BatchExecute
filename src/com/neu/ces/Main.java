package com.neu.ces;

import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.sql.*;

public class Main {
    public static void createData(Statement stmt) throws IOException, SQLException {
        String createName = "/home/dengzeyuan/Downloads/BatchExecute/src/com/neu/ces/ddl-postgres.sql";
        String csql = Files.readString(Paths.get(createName));
        stmt.execute(csql);
    }

    public static void loadData(Statement stmt) throws IOException, SQLException {
        String loadName = "/home/dengzeyuan/Downloads/BatchExecute/src/com/neu/ces/YCSBloader.sql";
        String lsql = Files.readString(Paths.get(loadName));
        stmt.execute(lsql);
    }

    static long sumTime;
    static long averageLatency;
    static long latencyEndTime;
    static long batchLatency;
    static long endTime;
    static long usedTime;
    static long startTime;
    static long latencyStartTime;
    static int batchRange = 20;
    static int loopNum = 10;

    public static void main(String[] args) throws IOException, ClassNotFoundException, SQLException {
        Connection conn = null;
        Statement stmt = null;
        Class.forName("org.postgresql.Driver");
        conn = DriverManager.getConnection("jdbc:postgresql://localhost:5432/benchbase?sslmode=disable&ApplicationName=ycsb&reWriteBatchedInserts=true", "postgres", "Aa123456");
        System.out.println("Opened database successfully");
        conn.setAutoCommit(false);
        stmt = conn.createStatement();

        String fileName = "/home/dengzeyuan/Downloads/BatchExecute/src/com/neu/ces/YCSBworker.sql";
        String s = Files.readString(Paths.get(fileName));
        String[] SqlList = s.split("Commit;");

        for (int n = 1; n <= batchRange; n++) {
            sumTime = averageLatency = 0;
            for (int cnt = 0; cnt < loopNum; cnt++) {
                createData(stmt);
                loadData(stmt);

                int i = 0;
                startTime = latencyStartTime = System.currentTimeMillis();

                for (String sql : SqlList) {
                    i += 1;
                    if (sql.contains(";UPDATE")) {
                        stmt.addBatch(sql.split(";UPDATE")[0] + ";");
                        stmt.addBatch("UPDATE" + sql.split(";UPDATE")[1]);
                    } else {
                        stmt.addBatch(sql);
                    }
                    if (i % n == 0) {
                        stmt.executeBatch();
                        conn.commit();
                        latencyEndTime = System.currentTimeMillis();
                        batchLatency = latencyEndTime - latencyStartTime;
                        averageLatency += batchLatency * n;
                        latencyStartTime = System.currentTimeMillis();
                    }
                }
                stmt.executeBatch();
                conn.commit();
                latencyEndTime = endTime = System.currentTimeMillis();
                batchLatency = latencyEndTime - latencyStartTime;
                averageLatency += batchLatency * (SqlList.length % n);
                usedTime = endTime - startTime;
                sumTime += usedTime;
            }
            System.out.println("batch_size = " + n + " Runtime : " + sumTime / loopNum + " Latency : " + (double) averageLatency / (SqlList.length * loopNum));
        }
    }
}
