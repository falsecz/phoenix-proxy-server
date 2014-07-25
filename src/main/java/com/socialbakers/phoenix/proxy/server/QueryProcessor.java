package com.socialbakers.phoenix.proxy.server;

import static com.socialbakers.phoenix.proxy.server.ValueTypeMapper.getColumnType;
import static com.socialbakers.phoenix.proxy.server.ValueTypeMapper.getValue;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.Callable;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadPoolExecutor;

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

	private String zooKeeper; // = "hadoops-master.us-w2.aws.ccl";
	private ThreadPoolExecutor executor;

	QueryProcessor(String zookeeper, ThreadPoolExecutor executor) throws SQLException {
		this.zooKeeper = zookeeper;
		this.executor = executor;
		DriverManager.registerDriver(new PhoenixDriver());
		getConnection();
	}

	PhoenixProxyProtos.QueryResponse sendQuery(PhoenixProxyProtos.QueryRequest queryRequest) throws Exception {

		PhoenixProxyProtos.QueryResponse.Builder responseBuilder = PhoenixProxyProtos.QueryResponse.newBuilder();
		responseBuilder.setCallId(queryRequest.getCallId());

		List<PhoenixProxyProtos.QueryRequest.Query> queries = queryRequest.getQueriesList();

		int queryId = 0;
		if (queries.size() == 1) {
			PhoenixProxyProtos.QueryRequest.Query query = queries.get(queryId);
			try {
				responseBuilder.addResults(callPhoenix(query, queryId));
			} catch (Exception e) {
				LOGGER.error(e.getMessage(), e);
				QueryException.Builder exception = QueryException.newBuilder().setMessage(e.getMessage())
						.setQueryId(queryId);
				responseBuilder.setException(exception);
			}
		} else {
			List<Future<Result>> tasks = new ArrayList<Future<Result>>();
			for (queryId = 0; queryId < queries.size(); queryId++) {
				PhoenixProxyProtos.QueryRequest.Query query = queries.get(queryId);
				PhoenixReuestTask phoenixReuestTask = new PhoenixReuestTask(query, queryId);
				tasks.add(executor.submit(phoenixReuestTask));
			}

			queryId = 0;
			try {
				for (; queryId < queries.size(); queryId++) {
					Future<Result> future = tasks.get(queryId);
					responseBuilder.addResults(future.get());
				}
			} catch (Exception e) {
				LOGGER.error(e.getMessage(), e);
				QueryException.Builder exception = QueryException.newBuilder().setMessage(e.getMessage())
						.setQueryId(queryId);
				responseBuilder.setException(exception);
			}
		}

		PhoenixProxyProtos.QueryResponse response = responseBuilder.build();
		return response;
	}

	private Result callPhoenix(PhoenixProxyProtos.QueryRequest.Query query, int queryId) throws Exception {
		Result.Builder resultBuilder = Result.newBuilder();
		Connection con = getConnection();

		try {
			PreparedStatement preparedStatement = con.prepareStatement(query.getSql());
			ValueTypeMapper.setPrepareStatementParameters(preparedStatement, query.getParamsList());
			if (query.getType() == Type.UPDATE) {
				int rowsInserted = preparedStatement.executeUpdate();
				LOGGER.debug("Updated lines count:" + rowsInserted);
				con.commit();
			} else if (query.getType() == Type.QUERY) {
				ResultSet rs = preparedStatement.executeQuery();
				ResultSetMetaData meta = rs.getMetaData();
				int columnCount = meta.getColumnCount();
				// column metadata
				for (int i = 1; i <= columnCount; i++) {
					String columnName = meta.getColumnName(i);
					DataType columnType = getColumnType(meta.getColumnType(i));

					ColumnMapping column = ColumnMapping.newBuilder().setName(columnName).setType(columnType).build();
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
		} finally {
			con.close();
		}
		return resultBuilder.build();
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

	private class PhoenixReuestTask implements Callable<Result>, PriorityRejectableTask {

		private final PhoenixProxyProtos.QueryRequest.Query query;
		private int queryId;

		private PhoenixReuestTask(PhoenixProxyProtos.QueryRequest.Query query, int queryId) {
			this.query = query;
			this.queryId = queryId;
		}

		@Override
		public Result call() throws Exception {
			Thread.sleep(0L, (int) (Math.random() * 10000)); // to prevent consume too many cpu
			return callPhoenix(query, queryId);
		}

		@Override
		public int getPriority() {
			return 1;
		}

		@Override
		public void reject() {
			// this task would never be rejected because of high priority
			LOGGER.error("Something is terribly wrong. Query " + queryId + " was rejected.");
		}

	}
}
