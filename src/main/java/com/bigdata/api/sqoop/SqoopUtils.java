package com.bigdata.api.sqoop;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.sqoop.client.SqoopClient;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MLink;

public class SqoopUtils {
	// persistenceId 等同 linkId setName时候name要唯一 ，否则报unique的错误
	// connId 固定 如果是3报错 invalid cid
	public static final Long HDFS_CID = 1L;
	public static final Long JDBC_CID = 2L;

	public static HashMap<Long, String> getLinkMap(SqoopClient client) {
		HashMap<Long, String> linkMap = new HashMap<Long, String>();
		for (MLink link : client.getLinks()) {

			linkMap.put(link.getPersistenceId(), link.getName());
		}
		return linkMap;
	}

	public static HashMap<Long, String> getJobMap(SqoopClient client) {
		HashMap<Long, String> jobMap = new HashMap<Long, String>();
		for (MJob job : client.getJobs()) {
			jobMap.put(job.getPersistenceId(), job.getName());
		}
		return jobMap;
	}

	public static void clearAllLink(SqoopClient client) {
		List<MLink> links = client.getLinks();
		for (MLink link : links) {
			client.deleteLink(link.getPersistenceId());
		}
	}
	
	public static void clearAllJob(SqoopClient client) {
		List<MJob> jobs = client.getJobs();
		for (MJob job : jobs) {
			client.deleteJob(job.getPersistenceId());
		}
	}

	public static Boolean isLinkExists(SqoopClient client, long linkId) {
		if (client.getLink(linkId) != null) {
			return true;
		}

		return false;
	}

	public static void main(String[] args) {

		String url = "http://192.168.2.131:12000/sqoop/";
		SqoopClient client = new SqoopClient(url);
		
		//clearAllLink(client);
		//clearAllJob(client);

		HashMap<Long, String> linkMap = getLinkMap(client);
		for (Map.Entry<Long, String> entry : linkMap.entrySet()) {
			System.out.println("linkId= " + entry.getKey() + " and name= "
					+ entry.getValue());
		}
		
		HashMap<Long, String> jobMap = getJobMap(client);
		for (Map.Entry<Long, String> entry : jobMap.entrySet()) {
			System.out.println("jobId= " + entry.getKey() + " and name= "
					+ entry.getValue());
		}

	}

}
