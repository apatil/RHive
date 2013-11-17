package com.nexr.rhive.hive;

import java.sql.SQLException;
import java.util.Hashtable;
import java.util.Iterator;
import java.util.Map;
import java.util.Map.Entry;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.ThreadFactory;


public class AsyncHiveQLExecutor {
	private Hashtable<String, String> properties;
	private ExecutorService executor;
	
	public AsyncHiveQLExecutor(int nThreads, Hashtable<String, String> properties) {
		this.properties = properties;
		this.executor = Executors.newFixedThreadPool(nThreads, new DaemonThreadFactory(Executors.defaultThreadFactory()));
	}

	public Future<String> execute(String id, String query, HiveConnectionProperties connectionProperties) {
		return executor.submit(new HiveQLExecutor(id, query, connectionProperties));
	}


	private class HiveQLExecutor implements Callable<String> {
		private String id;
		private String query;
		private HiveConnectionProperties connectionProperties;

		public HiveQLExecutor(String id, String query, HiveConnectionProperties connectionProperties) {
			this.id = id;
			this.query = query;
			this.connectionProperties = connectionProperties;
		}

		@Override
		public String call() throws SQLException {
			HiveJdbcClient hiveClient = new HiveJdbcClient(connectionProperties.isServer2());
			try {
				hiveClient.connect(connectionProperties.getHost(), 
						connectionProperties.getPort(),
						connectionProperties.getDb(),
						connectionProperties.getUser(),
						connectionProperties.getPassword());
				
				setProperties(hiveClient);
				hiveClient.execute(query);
			} finally {
				try {
					if (hiveClient != null) {
						hiveClient.close();
					}
				} catch (SQLException ignored) { }
			}
			
			return id;
		}

		private void setProperties(HiveJdbcClient hiveClient) throws SQLException {
			Iterator<Entry<String, String>> iterator = properties.entrySet().iterator();

			while (iterator.hasNext()) {
				Map.Entry<String, String> entry = (Map.Entry<String, String>) iterator.next();
				hiveClient.set(entry.getKey(), entry.getValue());
			}
		}
	}
	
	public void shutdown() {
		executor.shutdown();
	}

	private static class DaemonThreadFactory implements ThreadFactory {
		private final ThreadFactory factory;
		
		DaemonThreadFactory(ThreadFactory factory) {
			if (factory == null) {
				throw new NullPointerException();
			}
			
			this.factory = factory;
		}

		@Override
		public Thread newThread(Runnable r) {
			Thread t = factory.newThread(r);
			t.setDaemon(true);
			return t;
		}
	}
}