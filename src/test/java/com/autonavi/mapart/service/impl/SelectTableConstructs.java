package com.autonavi.mapart.service.impl;

import java.io.File;
import java.sql.ResultSet;

import org.junit.Test;

import com.autonavi.mapart.service.H2gisServer;
import com.autonavi.mapart.service.WashDataConfig;

public class SelectTableConstructs {

	@Test
	public void selectAnMeshPropoties(){
	
		H2gisServer h2gisServer = H2gisServer.getInstance();
		final String anMergeTable = "MERGED_AN_TABLE";// an分幅数据导入的表，最终需要输出
		try {
			h2gisServer.startup();
			String file = "src/main/resources/config.properties";
			WashDataConfig instance = WashDataConfig.getInstance();
			instance.loadConfig(file);
			String path = instance.getProperty("poiFile");
//			h2gisServer.importData( path, anMergeTable);
			impPOI(path, anMergeTable);
			String sql = "select * from "+anMergeTable+".columns";
			ResultSet rs = h2gisServer.executeQuery(sql);
			while(rs.next()){
				System.out.println("=======================================================");
				System.out.println(rs.getString(3));
//				System.out.println(rs.getString(2));
//				System.out.println(rs.getString(3));
//				System.out.println(rs.getString(4));
//				System.out.println(rs.getString(5));
//				System.out.println(rs.getString(6));
			}
		}catch(Exception e){
			e.printStackTrace();
		}finally {
			h2gisServer.shutdown();
		}
	}
	private void impPOI(String shpDir, String PoiTable) {
		String shapPath = null;
		File[] files = new File(shpDir).listFiles();

		for (File file : files) {
			if (file.isDirectory()) {
				continue;
			} else {
				if (file.getName().endsWith("shp")) {
					shapPath = file.getAbsolutePath();
				}
			}
		}
		if (shapPath!=null) {
			H2gisServer.getInstance().importData(shapPath, PoiTable);
		}
	}
}
