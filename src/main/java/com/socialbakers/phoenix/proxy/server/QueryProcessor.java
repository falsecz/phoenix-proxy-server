package com.socialbakers.phoenix.proxy.server;

import static com.socialbakers.phoenix.proxy.server.ValueTypeMapper.getColumnType;
import static com.socialbakers.phoenix.proxy.server.ValueTypeMapper.getValue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;

import org.apache.phoenix.jdbc.PhoenixDriver;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.google.protobuf.ByteString;
import com.socialbakers.phoenix.proxy.PhoenixProxyProtos;
import com.socialbakers.phoenix.proxy.PhoenixProxyProtos.DataType;
import com.socialbakers.phoenix.proxy.PhoenixProxyProtos.QueryRequest.Query.Type;
import com.socialbakers.phoenix.proxy.PhoenixProxyProtos.QueryResponse.QueryException;
import com.socialbakers.phoenix.proxy.PhoenixProxyProtos.QueryResponse.Result;
import com.socialbakers.phoenix.proxy.PhoenixProxyProtos.QueryResponse.Result.ColumnMapping;
import com.socialbakers.phoenix.proxy.PhoenixProxyProtos.QueryResponse.Result.Row;

class QueryProcessor {

	private static final Logger LOGGER = LoggerFactory.getLogger(QueryProcessor.class);

	static int maxRetries = 3;

	// private Connection conn = null;
	// private int connectionCounter = 0;
	private String zooKeeper; // = "hadoops-master.us-w2.aws.ccl";

	// private BlockingQueue<Runnable> linkedBlockingDeque;

	QueryProcessor(String zooKeeper) throws SQLException {
		this.zooKeeper = zooKeeper;
		// this.linkedBlockingDeque = new ArrayBlockingQueue<Runnable>(20000);
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
					LOGGER.debug("Updated lines count:" + rowsInserted);
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

						ColumnMapping column = ColumnMapping.newBuilder().setName(columnName).setType(columnType)
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
			LOGGER.error(e.getMessage(), e);
			QueryException exception = QueryException.newBuilder().setMessage(e.getMessage()).setQueryId(queryId)
					.build();
			responseBuilder.setException(exception);
		} finally {
			if (con != null) {
				try {
					con.close();
				} catch (SQLException e) {
					LOGGER.error(e.getMessage(), e);
				}
			}
		}

		PhoenixProxyProtos.QueryResponse response = responseBuilder.build();
		return response;
	}

	private Connection getConnection() throws SQLException {

		Connection conn = null;
		int tries = 0;

		while (conn == null) {
			tries++;
			try {
				conn = DriverManager.getConnection("jdbc:phoenix:" + zooKeeper);
			} catch (IllegalStateException e) {
				LOGGER.debug("Getting zooKeeper connection failed on " + tries + "/" + maxRetries
						+ " try. Exception msg:'" + e.getMessage() + "'.");
				if (tries >= maxRetries) {
					throw e;
				}
			}
		}

		return conn;
	}
}
