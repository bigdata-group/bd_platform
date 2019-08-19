package com.bigdata.api.hbase.factory;

import java.io.IOException;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;

public class HBaseConnectionFactory {

	private Connection conn = null;
	private Configuration conf;
	public static final String ZOOKEEPER_QUORUM_KEY = "hbase.zookeeper.quorum";
	public static final String ZOOKEEPER_PORT_KEY = "hbase.zookeeper.property.clientPort";


	public HBaseConnectionFactory(Configuration conf) throws IOException {
		this.conf = conf;
		if (conf != null) {
			conn = ConnectionFactory.createConnection(conf);
		} else {
			conn = null;
		}
	}

	public HBaseConnectionFactory(String zookeeperQuorum, int zookeeperPort) throws IOException{
		this.conf = new Configuration();
		conf.set(ZOOKEEPER_QUORUM_KEY, zookeeperQuorum);
		conf.setInt(ZOOKEEPER_PORT_KEY, zookeeperPort);
		conn = ConnectionFactory.createConnection(conf);
	}

	public synchronized Connection getConnection() throws IOException {
		if (conn == null) {
			conn = ConnectionFactory.createConnection(conf);
		}
		return conn;
	}

}
