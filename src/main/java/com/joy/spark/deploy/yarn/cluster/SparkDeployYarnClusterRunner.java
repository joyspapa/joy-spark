package com.joy.spark.deploy.yarn.cluster;

import java.io.File;
import java.io.FilenameFilter;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.Path;
import org.apache.hadoop.yarn.conf.YarnConfiguration;
import org.apache.spark.SparkConf;
import org.apache.spark.deploy.yarn.Client;
import org.apache.spark.deploy.yarn.ClientArguments;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class SparkDeployYarnClusterRunner {

	protected final Logger logger = LoggerFactory.getLogger(this.getClass());

	public void submit(String appName, String schema, String brokers, String zkHosts, String hdfsUrl, String entityType)
			throws Exception {
		
		// create ClientArguments, which will be passed to Client
		ClientArguments cArgs = new ClientArguments(
				getSparkApplArgs(appName, schema, brokers, zkHosts, hdfsUrl, entityType).toArray(new String[0]));

		// create an instance of yarn Client client
		Client client = new Client(cArgs, setYarnConf(), getSparkConfig(appName, entityType));

		// submit Spark job to YARN
		client.run();
	}

	/*
	 * prepare arguments to be passed to 
	 */
	private List<String> getSparkApplArgs(String appName, String schema, String brokers, String zkHosts, String hdfsUrl, String entityType)
			throws Exception {
		List<String> argsList = new ArrayList<String>();
		argsList.add("--jar");
		argsList.add(ConfigHelper.getValue("spark.deploy.yarn.appl.main.jar"));
		argsList.add("--class");
		argsList.add(ConfigHelper.getValue("spark.deploy.yarn.appl.main.class." + entityType.toLowerCase()));
		argsList.add("--arg");
		argsList.add("--appName");
		argsList.add("--arg");
		argsList.add(appName);
		argsList.add("--arg");
		argsList.add("--schema");
		argsList.add("--arg");
		argsList.add(schema);
		argsList.add("--arg");
		argsList.add("--brokers");
		argsList.add("--arg");
		argsList.add(brokers);
		argsList.add("--arg");
		argsList.add("--zkHosts");
		argsList.add("--arg");
		argsList.add(zkHosts);
		argsList.add("--arg");
		argsList.add("--hdfsUrl");
		argsList.add("--arg");
		argsList.add(hdfsUrl);

		return argsList;
	}

	private SparkConf getSparkConfig(String appName, String entityType) throws Exception {
		// create an instance of SparkConf object
		SparkConf sparkConf = new SparkConf().setAppName(appName);
		
		// identify that you will be using Spark as YARN mode
		System.setProperty("SPARK_YARN_MODE", "true");

		sparkConf.set("spark.master", "yarn");
		sparkConf.set("spark.submit.deployMode", "cluster");

		sparkConf.set("spark.yarn.security.credentials.hbase.enabled", "false");
		sparkConf.set("spark.yarn.security.credentials.hive.enabled", "false");

		sparkConf.set("spark.streaming.stopGracefullyOnShutdown", "true");
		sparkConf.set("spark.yarn.submit.waitAppCompletion", "false");

		sparkConf.set("spark.yarn.jars", ConfigHelper.getValue("spark.deploy.yarn.common.jars.path"));
		sparkConf.set("spark.yarn.dist.jars", getDistJars());
		sparkConf.set("spark.yarn.dist.archives",
				ConfigHelper.getValue("spark.deploy.yarn.appl.log4j.properties.path"));

		// option jvm
		setOptionJVM(sparkConf, appName);

		// option spark conf
		setSparkConf(sparkConf, entityType);

		return sparkConf;
	}

	private String getDistJars() throws Exception {
		File path = new File(ConfigHelper.getValue("spark.deploy.yarn.appl.dist.jars.path"));
		final String pattern = "jar";

		String fileList[] = path.list(new FilenameFilter() {
			@Override
			public boolean accept(File dir, String name) {
				return name.endsWith(pattern); // pattern 형식으로 시작하는(여기서는 2019로 시작하는 이름)
			}
		});

		StringBuilder sb = new StringBuilder();
		// file list 출력
		for (int i = 0; i < fileList.length; i++) {
			sb.append(ConfigHelper.getValue("spark.deploy.yarn.appl.dist.jars.path"));
			sb.append(fileList[i]);
			if (i != fileList.length - 1) {
				sb.append(",");
			}
		}

		return sb.toString();
	}

	private void setSparkConf(SparkConf sparkConf, String entityType) {
		try {
			String property = ConfigHelper.getValue("spark.deploy.yarn.appl.conf." + entityType.toLowerCase());

			if (property != null && !property.isEmpty()) {
				String[] splited = property.split(",");
				for (String conf : splited) {
					String[] splitedConf = conf.split("=");
					if (splitedConf != null && splitedConf.length == 2) {
						sparkConf.set(splitedConf[0], splitedConf[1]);
					}
				}
			}
		} catch (Exception ex) {
			logger.warn("[setConf] appName=" + sparkConf.get("spark.app.name") + "cause=", ex);
		}
	}

	/*
	 * create a Yarn Configuration object
	 */
	private YarnConfiguration setYarnConf() {
		YarnConfiguration yarnConfig = new YarnConfiguration();

		try {

			String property = ConfigHelper.getValue("spark.deploy.yarn.conf.resource");

			if (property != null && !property.isEmpty()) {
				String[] splited = property.split(",");
				for (String conf : splited) {
					yarnConfig.addResource(new Path(conf));

				}
			}
		} catch (Exception ex) {
			logger.warn("[setYarnConf] cause=", ex);
		}

		return yarnConfig;
	}

	private void setOptionJVM(SparkConf sparkConf, String appName) {

		try {
			String jvm = ConfigHelper.getValue("spark.deploy.yarn.appl.option.jvm");
			String gc = ConfigHelper.getValue("spark.deploy.yarn.appl.option.gc");			
			String jmx = ConfigHelper.getValue("spark.deploy.yarn.appl.option.jmx");
			
			StringBuilder driverJavaOption = new StringBuilder();
			driverJavaOption.append(ConfigHelper.getValue("spark.deploy.yarn.appl.option.jvm.driver"));
			driverJavaOption.append(" ");
			driverJavaOption.append(jvm);
			if(gc != null && !gc.isEmpty()) {
				driverJavaOption.append(" ");
				driverJavaOption.append(gc);
			}
			if(jmx != null && !jmx.isEmpty()) {
				driverJavaOption.append(" ");
				driverJavaOption.append(jmx);
			}
			
			StringBuilder executorJavaOption = new StringBuilder();
			executorJavaOption.append(ConfigHelper.getValue("spark.deploy.yarn.appl.option.jvm.executor"));
			executorJavaOption.append(" ");
			executorJavaOption.append(jvm);
			if(gc != null && !gc.isEmpty()) {
				executorJavaOption.append(" ");
				executorJavaOption.append(gc);
			}
			if(jmx != null && !jmx.isEmpty()) {
				executorJavaOption.append(" ");
				executorJavaOption.append(jmx);
			}
			
			sparkConf.set("spark.driver.extraJavaOptions", driverJavaOption.toString().replaceAll("%appName%", appName));

			sparkConf.set("spark.executor.extraJavaOptions", executorJavaOption.toString().replaceAll("%appName%", appName));

		} catch (Exception ex) {
			logger.warn("[setOptionJVM] appName=" + appName + "cause=", ex);
		}
	}
	
	private YarnConfiguration setYarnConfNoXml() {
		YarnConfiguration yarnConfig = new YarnConfiguration();
		//		yarnConfig.set("yarn.resourcemanager.ha.rm-ids", "ecube57,ecube58");
		//		yarnConfig.set("yarn.resourcemanager.hostname.ecube57", "ecube57");
		//		yarnConfig.set("yarn.resourcemanager.hostname.ecube58", "ecube58");
		//		yarnConfig.set("yarn.resourcemanager.ha.enabled", "true");
		//		yarnConfig.set("yarn.resourcemanager.cluster-id", "ecube-yarn-ha");
		//		
		//		yarnConfig.set("dfs.nameservices", "obz-hadoop-ha");
		//		yarnConfig.set("dfs.ha.namenodes.obz-hadoop-ha", "ecube57,ecube58");
		//		yarnConfig.set("dfs.namenode.rpc-address.obz-hadoop-ha.ecube57", "ecube57:8020");
		//		yarnConfig.set("dfs.namenode.rpc-address.obz-hadoop-ha.ecube58", "ecube58:8020");
		//		yarnConfig.set("dfs.client.failover.proxy.provider.obz-hadoop-ha", "org.apache.hadoop.hdfs.server.namenode.ha.ConfiguredFailoverProxyProvider");
		//		
		//		yarnConfig.set("fs.defaultFS", "hdfs://obz-hadoop-ha");
		//		yarnConfig.set("hadoop.tmp.dir", "/data/prod/hadoop");
		
		return yarnConfig;
	}
}
