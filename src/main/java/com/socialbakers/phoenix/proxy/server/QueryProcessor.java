package com.socialbakers.phoenix.proxy.server;

import static com.socialbakers.phoenix.proxy.server.Logger.debug;
import static com.socialbakers.phoenix.proxy.server.Logger.error;
import static com.socialbakers.phoenix.proxy.server.ValueTypeMapper.getColumnType;
import static com.socialbakers.phoenix.proxy.server.ValueTypeMapper.getValue;

import com.google.protobuf.ByteString;
import org.apache.phoenix.jdbc.PhoenixDriver;
import com.socialbakers.phoenix.proxy.PhoenixProxyProtos.DataType;
import com.socialbakers.phoenix.proxy.PhoenixProxyProtos;
import com.socialbakers.phoenix.proxy.PhoenixProxyProtos.QueryRequest.Query.Type;
import com.socialbakers.phoenix.proxy.PhoenixProxyProtos.QueryResponse.Result;
import com.socialbakers.phoenix.proxy.PhoenixProxyProtos.QueryResponse.Result.ColumnMapping;
import com.socialbakers.phoenix.proxy.PhoenixProxyProtos.QueryResponse.Result.Row;
import com.socialbakers.phoenix.proxy.PhoenixProxyProtos.QueryResponse.QueryException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.ArrayBlockingQueue;
import java.util.concurrent.BlockingQueue;

class QueryProcessor {
    
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

        PhoenixProxyProtos.QueryResponse.Builder responseBuilder = PhoenixProxyProtos.QueryResponse.newBuilder();
        responseBuilder.setCallId(queryRequest.getCallId());
        
        List<PhoenixProxyProtos.QueryRequest.Query> queries = queryRequest.getQueriesList();
        Connection con = null;
        int queryId = 0;
        try {
            for (; queryId < queries.size(); queryId++) {
                
                PhoenixProxyProtos.QueryRequest.Query query = queries.get(queryId);

                Type type = query.getType();
                String sql = query.getSql();

                con = getConnection();

                PreparedStatement preparedStatement = con.prepareStatement(sql);
                ValueTypeMapper.setPrepareStatementParameters(preparedStatement, query.getParamsList());

                Result.Builder resultBuilder = Result.newBuilder();
                if (type == Type.UPDATE) {

                    // insert/update
                    int rowsInserted = preparedStatement.executeUpdate();
                    debug("Updated lines count:" + rowsInserted);
                    con.commit();

                } else if (type == Type.QUERY) {

                    // select data
                    ResultSet rs = preparedStatement.executeQuery();

                    ResultSetMetaData meta = rs.getMetaData();
                    int columnCount = meta.getColumnCount();

                    // column metadata
                    for (int i = 1; i <= columnCount; i++) {
                        String columnName = meta.getColumnName(i);
                        DataType columnType = getColumnType(meta.getColumnType(i));

                        ColumnMapping column = ColumnMapping.newBuilder()
                                .setName(columnName)
                                .setType(columnType)
                                .build();
                        resultBuilder.addMapping(column);
                    }

                    // data
                    while (rs.next()) {
                        Row.Builder rowBuilder = Row.newBuilder();

                        for (int i = 1; i <= columnCount; i++) {
                            ByteString value = getValue(rs, meta, i);
                            rowBuilder.addBytes(value);
                        }

                        Row row = rowBuilder.build();
                        resultBuilder.addRows(row);
                    }
                }
                
                con.close();
                
                Result result = resultBuilder.build();
                responseBuilder.addResults(result);
                
            }
        } catch (Exception e) {
            error(e.getMessage(), e);
            QueryException exception = QueryException.newBuilder()
                    .setMessage(e.getMessage())
                    .setQueryId(queryId)
                    .build();
            responseBuilder.setException(exception);
        } finally {
            if (con != null) {
                try {
                    con.close();
                } catch (SQLException e) {
                    error(e);
                }
            }
        }

        PhoenixProxyProtos.QueryResponse response = responseBuilder.build();
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
}
