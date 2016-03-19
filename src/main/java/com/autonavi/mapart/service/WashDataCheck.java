package com.autonavi.mapart.service;

import java.io.File;
import java.util.Collection;

import org.apache.log4j.Logger;

/**
 * 数据检查
 * @author huandi.yang
 *
 */
public abstract class WashDataCheck extends WashDataService {
	
	public Logger log = Logger.getLogger(getClass());
	
	public abstract void check();

	public WashedData wash() {
		return null;
	}
	
	protected Collection<File> findShpFiles(String dir, Collection<File> shpFiles) {
		File[] files = new File(dir).listFiles();
		if (files == null) {
			return shpFiles;
		}

		for (File file : files) {
			if (file.isDirectory()) {
				findShpFiles(file.getAbsolutePath(), shpFiles);
			} else {
				if (file.getName().endsWith("shp")) {
					shpFiles.add(file);
				}
			}
		}
		return shpFiles;
	}

}
