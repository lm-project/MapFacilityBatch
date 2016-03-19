package com.autonavi.mapart.service.impl;

import java.io.File;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashSet;
import java.util.Random;

import com.autonavi.mapart.service.H2gisServer;

/**
 * 和其他要素的检查
 * @author huandi.yang
 *
 */
public class WashDataCheckForMultiShp extends WashDataCheckForSelbst {

	private Collection<String> tableNames = new HashSet<String>();
	public WashDataCheckForMultiShp(String checkDir) {
		super(checkDir);
		String tableName = "";
		Collection<File> anShpFiles = new ArrayList<File>();
		findShpFiles(checkDir, anShpFiles); 
		for(File file : anShpFiles) {
			String tmpTname = getTname(tableName,tableNames);
			H2gisServer.getInstance().importData(file.getAbsolutePath() , tmpTname);
		}
	}

	protected String getTname(String tableName,Collection<String> tableNames) {
		String tname = tableName + new Random(1000).nextInt();
		if(tableNames.add(tname) ) {
			return tname;
		}
		return getTname(tableName,tableNames);
	}

	
	protected String[] getOtherElementTable() {
		return tableNames.toArray(new String[tableNames.size()]);
	}
}
