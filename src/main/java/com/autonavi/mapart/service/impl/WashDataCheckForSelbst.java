package com.autonavi.mapart.service.impl;

import java.io.File;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collection;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.autonavi.mapart.service.H2gisServer;
import com.autonavi.mapart.service.WashDataCheck;
import com.autonavi.mapart.service.WashDataConfig;

/**
 * 自压盖检查
 * @author huandi.yang
 *
 */
public class WashDataCheckForSelbst extends WashDataCheck {
	
	private Logger log = Logger.getLogger("checkLogger");
	private String checkDir;
	private double checkCoverPercent;
	private Collection<File> anShpFiles = new ArrayList<File>();
	private final String checkTmpTable = "TMP_CHECK_UPDATE";// 临时表
	public WashDataCheckForSelbst(String checkDir) {
		super();
		WashDataConfig properties = WashDataConfig.getInstance();
		this.checkCoverPercent = Double.parseDouble(properties.getProperty("checkCoverPercent"));
		this.checkDir = checkDir;
	}

	@Override
	public void check() {
		log.debug("------------自压盖检查Begin------------");
		findShpFiles(checkDir, anShpFiles);
		for(File file : anShpFiles){
			try {
				H2gisServer.getInstance().importData(file.getAbsolutePath(), checkTmpTable);
				String[] path = file.getAbsolutePath().split("\\\\");
				log.debug("图幅号："+ path[path.length-2]);
				for(String otherElementTable : getOtherElementTable() ) {
					checkInteraction(checkTmpTable, otherElementTable, checkCoverPercent);
				}
			} catch (SQLException e) {
				e.printStackTrace();
			} finally {
				log.debug("--------------------");
				H2gisServer.getInstance().execute("drop table " + checkTmpTable);
			}
		}
		log.debug("------------自压盖检查End------------");
	}

	protected String[] getOtherElementTable() {
		return new String[]{ checkTmpTable };
	}

	protected void checkInteraction(String checkTmpTable, String otherElementTable, double checkCoverPercent) throws SQLException {
		String sql = getCheckSql(checkTmpTable, otherElementTable,
				checkCoverPercent);
		ResultSet rs = H2gisServer.getInstance().executeQuery(sql);
		log.debug("===压盖面积 >=" + checkCoverPercent +"的设施区域名称===");
		Set<List<String>> set = new HashSet<List<String>>();  
		while(rs.next()){
			List<String> list = Arrays.asList(new String[]{rs.getString(1),rs.getString(2)});
			Collections.sort(list);
			set.add(list);
		}
		
		for(List<String> l : set) {
			log.debug( StringUtils.join(l," and "));
		}
	}

	/**
	 * @param checkTmpTable
	 * @param otherElementTable
	 * @param checkCoverPercent
	 * @return
	 */
	protected String getCheckSql(String checkTmpTable,
			String otherElementTable, double checkCoverPercent) {
		String sql = "select a.NAME_CHN,b.NAME_CHN from "
				+ checkTmpTable
				+ " a, "
				+ otherElementTable
				+ " b where a.THE_GEOM <> b.THE_GEOM and ST_Intersects(a.THE_GEOM,b.THE_GEOM) = true"
				+ " and ( "
				+ " (ST_Area(a.THE_GEOM) >= ST_Area(b.THE_GEOM) and ST_Area(ST_Intersection(a.THE_GEOM, b.THE_GEOM))/ST_Area(b.THE_GEOM) >= "
				+ checkCoverPercent
				+ ") or ( "
				+ " ST_Area(a.THE_GEOM) <= ST_Area(b.THE_GEOM) and ST_Area(ST_Intersection(a.THE_GEOM, b.THE_GEOM))/ST_Area(a.THE_GEOM) >= "
				+ checkCoverPercent +"))";
		return sql;
	}

}
