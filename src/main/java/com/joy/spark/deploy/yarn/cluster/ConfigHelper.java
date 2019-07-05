package com.joy.spark.deploy.yarn.cluster;

import java.io.IOException;
import java.io.InputStream;
import java.util.Properties;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

public class ConfigHelper {

	private static final Logger logger = LoggerFactory.getLogger(ConfigHelper.class);

	private static final String propertyFileName = "spark-yarn-deploy.properties";

	private static Properties prop;

	//private static SparkAppYarnTaskProperties sparkYarnDeployProp;
	
	private static void loadConfig() throws Exception {

		prop = new Properties();
		//sparkYarnDeployProp = new SparkAppYarnTaskProperties();
		
		try (InputStream is = ConfigHelper.class.getClassLoader().getResourceAsStream(propertyFileName)) {

			prop.load(is);
			
			logger.info("It has done loading the " + propertyFileName);

		} catch (IOException e) {
			logger.error("Error in initializing rest properties", e);
			throw e;
		}

	}

//	public static SparkAppYarnTaskProperties getSparkAppYarnTaskProperties() throws Exception {
//		if (sparkYarnDeployProp == null) {
//			loadConfig();
//		}
//
//		return sparkYarnDeployProp;
//	}
	
	private static Properties getProperties() throws Exception {
		if (prop == null) {
			loadConfig();
		}

		return prop;
	}

	public static void reloadConfig() throws Exception {
		loadConfig();
	}

	public static String getValue(String key) {
		try {
			return getProperties().getProperty(key);
		} catch(Exception ex) {
			return null;
		}
		
	}

	public static void setProperties(String key, String value) {
		try {
			getProperties().setProperty(key, value);
		} catch(Exception ex) {
			logger.error("Error in initializing rest setProperties", ex);
		}
	}

}