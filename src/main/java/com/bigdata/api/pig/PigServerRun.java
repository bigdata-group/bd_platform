package com.bigdata.api.pig;

import java.util.Iterator;

import org.apache.hadoop.conf.Configuration;
import org.apache.pig.ExecType;
import org.apache.pig.PigServer;
import org.apache.pig.backend.executionengine.ExecException;
import org.apache.pig.data.Tuple;

public class PigServerRun {
	
	public static void execPigScript(PigServer pigServer, String inputFile, String outputFile, String script){
		 try {
			 pigServer.registerQuery("A = load '" + inputFile+ "' using PigStorage(',');");
			 pigServer.registerQuery("Result = foreach A generate $0 as name;");
			 Iterator<Tuple> result = pigServer.openIterator("Result"); 
			 while (result.hasNext()) { 
			       Tuple t = result.next(); 
			       System.out.println(t); 
			 } 
			 //pigServer.store("Result", outputFile);
			 //System.out.println("OK");
		} catch (Exception e) {
			e.printStackTrace();
		}
	        
	}
	
	
	
	public static void main(String[] args) {
		try {
			Configuration hadoopConfig = new Configuration();
			hadoopConfig.set("fs.hdfs.impl", org.apache.hadoop.hdfs.DistributedFileSystem.class.getName());  
			hadoopConfig.set("fs.file.impl", org.apache.hadoop.fs.LocalFileSystem.class.getName() );
			hadoopConfig.set("fs.defaultFS", "hdfs://master:9000");
			hadoopConfig.set("yarn.resourcemanager.address", "master:8032");
		
//			PigServer pigServer =  new PigServer(ExecType.LOCAL, hadoopConfig);
//			PigServerRun.execPigScript(pigServer, "/home/hadoop/1.txt", "/dq/tmps/dmc_rule_tmps", "");
			PigServer pigServer =  new PigServer(ExecType.MAPREDUCE, hadoopConfig);
			PigServerRun.execPigScript(pigServer, "/dq/1.txt", "/dq/out1", "");
		} catch (ExecException e) {
			e.printStackTrace();
		}

	}
}
