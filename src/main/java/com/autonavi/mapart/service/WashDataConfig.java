package com.autonavi.mapart.service;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.Properties;

public class WashDataConfig {

	private WashDataConfig() {
		
	}
	private static WashDataConfig config;
	
	private static Properties properties = new Properties();
	public void loadConfig(String file ) throws FileNotFoundException, IOException {
		properties.load(new FileReader(new File(file)));

	}
	public static WashDataConfig getInstance( ) {
		if ( config == null ) {
			config = new WashDataConfig();
		}
		return config;
	}
	
	
	public void setProperty(String key,String value) {
		properties.setProperty(key, value);
	}
	public String getProperty(String key) {
		String property = properties.getProperty(key);
		int indexOf = property.indexOf("{");
		if(indexOf >0 ) {
			String value = property.substring(indexOf + 1, property.indexOf("}"));
			property = property.replaceAll("\\$\\{"+value+"\\}", properties.getProperty(value));
		}
		return property.trim();
	}
}
