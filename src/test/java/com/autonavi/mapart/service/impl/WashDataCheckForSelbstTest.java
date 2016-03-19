package com.autonavi.mapart.service.impl;

import static org.junit.Assert.*;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;

import org.junit.Test;

import com.autonavi.mapart.service.H2gisServer;
import com.autonavi.mapart.service.WashDataConfig;
import com.autonavi.mapart.service.WashDataService;
import com.autonavi.mapart.service.WashDataService.WashedData;

public class WashDataCheckForSelbstTest {

	@Test
	public void testCheck() throws FileNotFoundException, IOException, ClassNotFoundException, SQLException {
		H2gisServer h2gisServer = H2gisServer.getInstance();
		String file = "src/main/resources/config.properties";
		WashDataConfig instance = WashDataConfig.getInstance();
		instance.loadConfig(file);
		try {
			h2gisServer.startup();
			new WashDataCheckForSelbst("D:/H50F022046").check();
		} finally {
			h2gisServer.shutdown();
		}
	}

	@Test
	public void testWashDataCheckForSelbst() throws ClassNotFoundException, SQLException, FileNotFoundException, IOException {
		H2gisServer h2gisServer = H2gisServer.getInstance();
		String checkTmpTable = "TMP_CHECK_UPDATE";
		double checkCoverPercent = 0.05;
		String file = "src/main/resources/config.properties";
		WashDataConfig instance = WashDataConfig.getInstance();
		instance.loadConfig(file);
		try {
			h2gisServer.startup();
			h2gisServer.importData("D:/H50F022046/Joint_FacilityArea_Dissolve.shp", checkTmpTable);
//			new WashDataCheckForSelbst("D:/H50F022046").checkInteraction("D:/H50F022046/Joint_FacilityArea_Dissolve.shp", checkTmpTable, null, checkCoverPercent);;
		} finally {
			h2gisServer.shutdown();
		}
	}

}
