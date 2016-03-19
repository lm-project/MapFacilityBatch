package com.autonavi.mapart.service.impl;

import static org.junit.Assert.*;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.junit.Test;

import com.autonavi.mapart.service.H2gisServer;
import com.autonavi.mapart.service.WashDataService;
import com.autonavi.mapart.service.WashDataService.WashedData;

public class WashDataForFusionTest {

	@Test
	public void test() throws ClassNotFoundException, SQLException {
		H2gisServer h2gisServer = H2gisServer.getInstance();
		try {
			h2gisServer.startup();
			h2gisServer.importData("D:/EclipseWS/MapFacilityArea/data/2014-09-11[09_12_40]JointDBF/FA/BD/形状确认.shp", WashDataService.BD_TNAME);
			h2gisServer.importData("D:/EclipseWS/MapFacilityArea/data/2014-09-11[09_12_40]JointDBF/FA/AN/Joint_FacilityArea_Dissolve.shp", WashDataService.AUTONAVI_TNAME);
			h2gisServer.importData("D:/EclipseWS/MapFacilityArea/data/2014-09-11[09_12_40]JointDBF/POI/iis.shp", WashDataService.POI_TNAME);
			
			String outfile = "data/2014-09-11[09_12_40]JointDBF/";
			WashedData washedDataA = new WashDataHighMatch(outfile, "0.85", "3310", 0.3).wash();
			WashedData washedDataB = new WashDataNonUptownCover(outfile, 0.2, washedDataA).wash();
			WashedData washedDataC = new WashDataForFusion(outfile, washedDataB).wash();
			//WashedData washedDataD = new WashDataPerimeterAreaRatio(outfile, 0.2 , washedDataC).wash();
			//assertEquals("", washedData.getGarbageTableName());
		
			h2gisServer.importData("D:/EclipseWS/MapFacilityArea/data/2014-09-11[09_12_40]JointDBF/3/g/BD融合的数据.shp", "BDRESULT");
			ResultSet result = h2gisServer.executeQuery(
					"select *  FROM BDRESULT");
			while (result.next()) {
				System.out.println("('" + result.getString(1) + "', "
						+ result.getString(2) + ", '" + result.getString(3) + "')");
			}

		} finally {
			h2gisServer.shutdown();
		}
		
	}

}
