package com.bigdata.api.sqoop;

import com.bigdata.common.PropertiesReader;
import org.apache.sqoop.client.SqoopClient;
import org.apache.sqoop.model.MDriverConfig;
import org.apache.sqoop.model.MFromConfig;
import org.apache.sqoop.model.MJob;
import org.apache.sqoop.model.MLink;
import org.apache.sqoop.model.MLinkConfig;
import org.apache.sqoop.model.MSubmission;
import org.apache.sqoop.model.MToConfig;
import org.apache.sqoop.submission.counter.Counter;
import org.apache.sqoop.submission.counter.CounterGroup;
import org.apache.sqoop.submission.counter.Counters;
import org.apache.sqoop.validation.Status;

import java.util.Properties;

// http://sqoop.apache.org/docs/1.99.6/ClientAPI.html
public class SqoopHelper {
	public static SqoopClient client = null;
	public static String fsUrl = null;
	public static String hadoopUser = "hadoop";
	static{
		Properties props = PropertiesReader.getBigDataConf();
		String sqoopUrl = props.getProperty("sqoop.url");
		fsUrl =  props.getProperty("fs.defaultFS");
		client = new SqoopClient(sqoopUrl);
		hadoopUser = props.getProperty("hadoop.user");
	}

	public static SqoopClient getSqoopClient(){
		return client;
	}

	private static MLink createDBLink(String jdbcUrl, String driverClass, String userName, String pwd){
		// 创建链接 JDBC
		MLink dbLink = client.createLink(SqoopUtils.JDBC_CID);
		// fromLink.setName("JDBC connector");//名称得唯一，每次都得换，还不如不设置
		dbLink.setCreationUser("hadoop");
		MLinkConfig fromLinkConfig = dbLink.getConnectorLinkConfig();
		fromLinkConfig.getStringInput("linkConfig.connectionString").setValue(jdbcUrl);
		fromLinkConfig.getStringInput("linkConfig.jdbcDriver").setValue(driverClass);
		fromLinkConfig.getStringInput("linkConfig.username").setValue(userName);
		fromLinkConfig.getStringInput("linkConfig.password").setValue(pwd);
		Status fromStatus = client.saveLink(dbLink);
		if (fromStatus.canProceed()) {
			return dbLink;
		} else {
			return null;
		}
	}

	private static MLink createHDFSLink(){
		// 创建链接HDFS
		MLink hdfsLink = client.createLink(SqoopUtils.HDFS_CID);
		// toLink.setName("HDFS connector");//名称得唯一，每次都得换，还不如不设置
		hdfsLink.setCreationUser(hadoopUser);
		MLinkConfig toLinkConfig = hdfsLink.getConnectorLinkConfig();
		toLinkConfig.getStringInput("linkConfig.uri").setValue(fsUrl);
		Status toStatus = client.saveLink(hdfsLink);
		if (toStatus.canProceed()) {
			return hdfsLink;
		} else {
			return null;
		}
	}

	public static MJob createJob(MLink fromLink, MLink toLink){
		// 创建一个任务
		long fromLinkId = fromLink.getPersistenceId();
		long toLinkId = toLink.getPersistenceId();
		MJob job = client.createJob(fromLinkId, toLinkId);
		// job.setName("MySQL to HDFS job");//名称得唯一，每次都得换，还不如不设置
		job.setCreationUser(hadoopUser);
		return job;
	}

	public static void saveJobAndStart(MJob job){
		Status status = client.saveJob(job);
		// 创建成功
		if (status.canProceed()) {
			// 启动任务
			MSubmission submission = client.startJob(job.getPersistenceId());
			System.out.println("JOB提交状态为 : " + submission.getStatus());
			while (submission.getStatus().isRunning()
					&& submission.getProgress() != -1) {
				System.out.println("进度 : "
						+ String.format("%.2f %%", submission.getProgress() * 100));
				// 三秒报告一次进度
				try {
					Thread.sleep(500);// 0.5秒
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			System.out.println("JOB执行结束... ...");
			System.out.println("Hadoop任务ID为 :" + submission.getExternalId());
			Counters counters = submission.getCounters();
			if (counters != null) {
				System.out.println("计数器:");
				for (CounterGroup group : counters) {
					System.out.print("\t");
					System.out.println(group.getName());
					for (Counter counter : group) {
						System.out.print("\t\t");
						System.out.print(counter.getName());
						System.out.print(": ");
						System.out.println(counter.getValue());
					}
				}
			}
			if (submission.getExceptionInfo() != null) {
				System.out.println("JOB执行异常，异常信息为 : "
						+ submission.getExceptionInfo());
			}
			System.out.println("执行完毕");

		} else {
			System.out.println("JOB创建失败。");
		}
	}


	/**
	 *将db中的数据导入到hdfs
	 * @param jdbcUrl
	 * @param driverClass
	 * @param userName
	 * @param pwd
	 * @param schemaName
	 * @param tableName
	 * @param output hdfs路径
	 * @param format TEXT_FILE  SEQUENCE_FILE
	 */
	public static void importData(String jdbcUrl, String driverClass, String userName, String pwd, String schemaName, String tableName,
								  String output, String format){
		MLink fromLink = createDBLink(jdbcUrl, driverClass, userName, pwd);
		MLink toLink = createHDFSLink();
		MJob job = createJob(fromLink, toLink);

		// 设置源链接任务配置信息
		MFromConfig fromJobConfig = job.getFromJobConfig();
		fromJobConfig.getStringInput("fromJobConfig.schemaName").setValue(schemaName);
		fromJobConfig.getStringInput("fromJobConfig.tableName").setValue(tableName);
		// fromJobConfig.getStringInput("fromJobConfig.partitionColumn").setValue("id");

		MToConfig toJobConfig = job.getToJobConfig();
		toJobConfig.getStringInput("toJobConfig.outputDirectory").setValue(output);
		toJobConfig.getEnumInput("toJobConfig.outputFormat").setValue(format);

		MDriverConfig driverConfig = job.getDriverConfig();
		driverConfig.getIntegerInput("throttlingConfig.numExtractors").setValue(1);

		saveJobAndStart(job);

	}

	public static void exportData(String jdbcUrl, String driverClass, String userName, String pwd, String schemaName, String tableName,
								  String input, String format){
		MLink fromLink = createHDFSLink();
		MLink toLink = createDBLink(jdbcUrl, driverClass, userName, pwd);
		MJob job = createJob(fromLink, toLink);

		MFromConfig fromJobConfig = job.getFromJobConfig();
		fromJobConfig.getStringInput("fromJobConfig.inputDirectory").setValue(input);

		MToConfig toJobConfig = job.getToJobConfig();
		toJobConfig.getStringInput("toJobConfig.schemaName").setValue(schemaName);
		toJobConfig.getStringInput("toJobConfig.tableName").setValue(tableName);

		// MDriverConfig driverConfig = job.getDriverConfig();
		// driverConfig.getIntegerInput("throttlingConfig.numExtractors").setValue(3);

		saveJobAndStart(job);
	}


}
