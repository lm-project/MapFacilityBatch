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

public class WashDataCoverEdgeTest {

	@Test
	public void testWash() throws FileNotFoundException, IOException, ClassNotFoundException, SQLException {
		H2gisServer h2gisServer = H2gisServer.getInstance();
		String file = "src/main/resources/config.properties";
		WashDataConfig instance = WashDataConfig.getInstance();
		instance.loadConfig(file);
		try {
			h2gisServer.startup();
			h2gisServer.importData("D:/_2_5/BD.shp", WashDataService.BD_TNAME);
			h2gisServer.importData("D:/_2_5/AN.shp", WashDataService.AUTONAVI_TNAME);
			WashedData washedDataB = new WashedData("", "D:/_2_5/BD.shp", "", WashDataService.BD_TNAME);
			WashedData washedData = new WashDataCoverEdge(washedDataB).wash();
			assertEquals("", washedData.getGarbageTableName());
		} finally {
			h2gisServer.shutdown();
		}
	}

}
