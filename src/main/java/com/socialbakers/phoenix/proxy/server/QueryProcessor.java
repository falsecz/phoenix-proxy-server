/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.socialbakers.phoenix.proxy.server;

import static com.socialbakers.phoenix.proxy.server.ValueTypeMapper.getColumnType;
import static com.socialbakers.phoenix.proxy.server.ValueTypeMapper.getValue;

import com.google.protobuf.ByteString;
import com.salesforce.phoenix.jdbc.PhoenixDriver;
import com.socialbakers.phoenix.proxy.PhoenixProxyProtos;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.commons.io.IOUtils;
import org.apache.commons.lang.StringUtils;

/**
 *
 * @author robert
 */
class QueryProcessor {
    
    static int conCounter = 0;
    private static final Logger logger = Logger.getLogger(QueryProcessor.class.getName());
    
    private Connection conn = null;
    private int connectionCounter = 0;
    private String zooKeeper;  //= "hadoops-master.us-w2.aws.ccl";
    private BlockingQueue<Runnable> linkedBlockingDeque;
    
    QueryProcessor(String zooKeeper) throws SQLException {
        this.zooKeeper = zooKeeper;
        this.linkedBlockingDeque = new ArrayBlockingQueue<Runnable>(20000);
        DriverManager.registerDriver(new PhoenixDriver());
        getConnection();
    }

    PhoenixProxyProtos.QueryResponse sendQuery(PhoenixProxyProtos.QueryRequest queryRequest) throws Exception {

        long start = System.currentTimeMillis();

        PhoenixProxyProtos.QueryResponse.Builder responseBuilder = PhoenixProxyProtos.QueryResponse.newBuilder();
        responseBuilder.setCallId(queryRequest.getCallId());
        
        String query = queryRequest.getQuery();
	PhoenixProxyProtos.QueryRequest.Type type = queryRequest.getType();
        
        Connection con = null;
        
        try {
//            System.out.println(++conCounter);
            if (type == PhoenixProxyProtos.QueryRequest.Type.UPDATE) {
                // getConnection().createStatement().execute(query);
                con = getConnection();
                PreparedStatement upsertStmt = con.prepareStatement(query);
                int rowsInserted = upsertStmt.executeUpdate();
                //		    int c = getConnection().createStatement().executeUpdate(query);
                log("Updated lines count:" + rowsInserted);
                con.commit();
            } else {
                // select data
                con = getConnection();
                ResultSet rs = con.createStatement().executeQuery(query);

                ResultSetMetaData meta = rs.getMetaData();
                int columnCount = meta.getColumnCount();

                // column metadata
                for (int i = 1; i <= columnCount; i++) {
                    String columnName = meta.getColumnName(i);
                    PhoenixProxyProtos.ColumnMapping.Type columnType = getColumnType(meta.getColumnType(i));

                    PhoenixProxyProtos.ColumnMapping column = PhoenixProxyProtos.ColumnMapping.newBuilder()
                            .setName(columnName)
                            .setType(columnType)
                            .build();
                    responseBuilder.addMapping(column);
                }

                // data
                while (rs.next()) {

                    long resultsetRowStart = System.currentTimeMillis();

                    PhoenixProxyProtos.Row.Builder rowBuilder = PhoenixProxyProtos.Row.newBuilder();

                    for (int i = 1; i <= columnCount; i++) {
                        ByteString value = getValue(rs, meta, i);
                        rowBuilder.addBytes(value);
                    }

                    PhoenixProxyProtos.Row row = rowBuilder.build();
                    responseBuilder.addRows(row);
                }
            }

        } catch (Exception e) {
            e.printStackTrace();
            PhoenixProxyProtos.QueryException exception = PhoenixProxyProtos.QueryException.newBuilder()
                    .setMessage(e.getMessage())
                    .build();
            responseBuilder.setException(exception);
        } finally {
            if (con != null) {
                try {
                    con.close();
                } catch (Exception e) {}
            }
        }

        PhoenixProxyProtos.QueryResponse response = responseBuilder.build();

//        long queryAndBuildDuration = System.currentTimeMillis() - start;
//        log("Whole query duration: " + queryAndBuildDuration + "ms");

        return response;
    }

    private Connection getConnection() throws SQLException {
//        if (conn != null) {
//            connectionCounter++;
//
//            //create new connection
//            if (connectionCounter == 500) {
//                new Thread(new Runnable() {
//                    @Override
//                    public void run() {
//                        try {
//                            System.out.println("creating new connection");
//                            Connection c = DriverManager.getConnection("jdbc:phoenix:" + zooKeeper);
//                            System.out.println("new connection created");
//
//                            conn = c;
//                            connectionCounter = 0;
//                        } catch (SQLException ex) {
//                            log(null, ex);
//                        }
//                    }
//                }).start();
//            }
//
//            return conn;
//        }

//        DriverManager.registerDriver(new PhoenixDriver());
        conn = DriverManager.getConnection("jdbc:phoenix:" + zooKeeper);
        return conn;
    }
    
    private static void log(String msg) {
        logger.log(Level.SEVERE, msg);
    }

    private static void log(String msg, Throwable t) {
        if (StringUtils.isBlank(msg) && t != null) {
            msg = t.getMessage();
        }
        logger.log(Level.SEVERE, msg, t);
    }
}
