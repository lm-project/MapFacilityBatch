package com.autonavi.mapart.service;

import java.io.File;
import java.sql.SQLException;

import org.apache.log4j.Logger;

import com.autonavi.mapart.service.WashDataService.WashedData;

/**
 * wash.jar 3
 * 
 *  step1
 *     r
 *     g
 *  step2
 *     r
 *     g
 *  step3
 *  	r
 *  	g
 *  step4
 *  ... ...
 *
 */
public class WashDataImport {
	public Logger log = Logger.getLogger(getClass());

	private String amapFile;
	private String poiFile;

	public WashDataImport() {
		super();
		WashDataConfig properties = WashDataConfig.getInstance();
		
		this.amapFile = properties.getProperty("amapFile");
		this.poiFile = properties.getProperty("poiFile");
	}

	public String getShpFile(String dir) {
		File files = new File(dir);
		if( files.exists() ){
			for (File file : new File(dir).listFiles()) {
				if (file.getName().endsWith("shp")) {
					return file.getAbsolutePath();
				}
			}
		}
		return null;
	}

	public WashedData importData(String bddir, String garbagedir) throws SQLException {
		H2gisServer instance = H2gisServer.getInstance();
		String result_file = getShpFile(bddir);
//		String garbage_file = getShpFile(garbagedir);
		instance.importData(result_file, WashDataService.BD_TNAME);
		log.debug("输入的BD拼图路径："+result_file);
//		instance.importData(garbage_file, WashDataService.GB_TNAME);
		//导入AMAP和 POI数据
		String anShp = getShpFile(amapFile);
		instance.importData( anShp,WashDataService.AUTONAVI_TNAME );
		log.debug("输入的AN拼图路径："+anShp);
		instance.importData(getShpFile(poiFile), WashDataService.POI_TNAME);
		return new WashedData(garbagedir, result_file, WashDataService.GB_TNAME, WashDataService.BD_TNAME);
	}
}
