package com.autonavi.mapart.service.impl;

import static org.junit.Assert.*;

import org.junit.Test;

import com.autonavi.mapart.service.H2gisServer;
import com.autonavi.mapart.service.WashDataConfig;

public class WashDataMergeForNewTest {

	@Test
	public void testMerge() {
		H2gisServer h2gisServer = H2gisServer.getInstance();
		try {
			
			String file = "src/main/resources/config.properties";
			WashDataConfig instance = WashDataConfig.getInstance();
			instance.loadConfig(file);
			
			h2gisServer.startup();
			WashDataMergeForDelete  w8 = new WashDataMergeForDelete();
			System.out.println(w8.toString());
			w8.merge();
		} catch (Exception e) {
			
		}
		
	}

}
