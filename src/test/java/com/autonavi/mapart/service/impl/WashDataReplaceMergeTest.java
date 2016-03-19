package com.autonavi.mapart.service.impl;

import static org.junit.Assert.*;

import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.SQLException;

import org.junit.Test;

import com.autonavi.mapart.service.H2gisServer;
import com.autonavi.mapart.service.WashDataConfig;

public class WashDataReplaceMergeTest {

	
	@Test
	public void test() throws FileNotFoundException, IOException, ClassNotFoundException, SQLException {
		WashDataConfig.getInstance().loadConfig("src/main/resources/config.properties");
		H2gisServer.getInstance().startup();

		WashDataMergeForReplace merge = new WashDataMergeForReplace();
		merge.merge();
	}

}
