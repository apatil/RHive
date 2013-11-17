package com.nexr.rhive.hive;

public class HiveConnectionProperties {
	private String host;
	private int port;
	private String db;
	private String user;
	private String password;
	private boolean server2;
	
	
	public HiveConnectionProperties(String host, int port, String db, String user, String password, boolean server2) {
		this.host = host;
		this.port = port;
		this.db = db;
		this.user = user;
		this.password = password;
		this.server2 = server2;
	}
	
	public String getHost() {
		return host;
	}
	
	public int getPort() {
		return port;
	}
	
	public String getDb() {
		return db;
	}
	
	public String getUser() {
		return user;
	}

	public String getPassword() {
		return password;
	}
	
	public boolean isServer2() {
		return server2;
	}
}
