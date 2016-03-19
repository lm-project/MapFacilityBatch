package com.autonavi.mapart.service;

import java.io.File;
import java.sql.ResultSet;
import java.util.ArrayList;
import java.util.Collection;

import org.apache.log4j.Logger;

public abstract class AbstractWashDataMerge extends WashDataService implements WashDataMerge {

	public Logger log = Logger.getLogger(getClass());

	public abstract void merge();

	public static String rootdir = WashDataConfig.getInstance().getProperty("amapFile");

	public WashedData wash() {
		return null;
	};

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

	protected void impShps(String shpDir, String mainTable, boolean isBD) {
		ArrayList<File> shpFiles = new ArrayList<File>();
		findShpFiles(shpDir, shpFiles);
		for (File f : shpFiles) {
			appendDataIntoTable(f.getAbsolutePath(), mainTable, isBD);
		}
	}
	
	/**
	 * 导入POI数据
	 * @param shpDir
	 * @param PoiTable
	 */
	protected void impPOI(String shpDir, String PoiTable) {
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
		if (shapPath!=null && !exitsTable(PoiTable)) {
			H2gisServer.getInstance().importData(shapPath, PoiTable);
		}
	}

	private void appendDataIntoTable(String shp, String mainTable, boolean isBD) {
		String tmpTname = "TMP_MERGE";
		H2gisServer.getInstance().importData(shp, tmpTname);
		if (!exitsTable(mainTable)) {
			String sql = "create table " + mainTable + " as select * from " + tmpTname + " where 1=2";
			log.debug(sql);
			H2gisServer.getInstance().execute(sql);
		}
		H2gisServer.getInstance().execute(getInsertSql(tmpTname, mainTable, isBD));
		drop(tmpTname);
	}

	protected boolean exitsTable(String mainTable) {
		ResultSet rs = H2gisServer.getInstance().executeQuery(
				"select count(1) from information_schema.tables where table_schema ='PUBLIC' "
						+ "and table_type = 'TABLE' and lower(table_name) = '" + mainTable.toLowerCase() + "'");

		try {
			return rs.next() ? rs.getInt(1) > 0 : false;
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}
	}

	private String getInsertSql(String tmpTname, String mainTable, boolean isBD) {
		return isBD ? "insert into " + mainTable + " (THE_GEOM) select THE_GEOM from " + tmpTname :
			" insert into "
				+ mainTable + " (THE_GEOM,NAME_CHN,MESH,POI_ID,FA_TYPE,AREA_FLAG) "
				+ " select THE_GEOM,NAME_CHN,MESH,POI_ID,FA_TYPE,AREA_FLAG from " + tmpTname;
	}

	protected void drop(String... tnames) {
		for(String tname: tnames) {
			if(exitsTable( tname )) {
				H2gisServer.getInstance().execute("drop table " + tname);
			}
		}
	}
	protected class SetResultSet {
		String NAME_CHN;
		String POI_ID;
		Object THE_GEOM;
		private String MESH;
		private String FA_TYPE;
		private String AREA_FLAG;
		private String GUID;
		private int POI_TYPE;

		public  SetResultSet() {}
		public SetResultSet(String NAME_CHN, String POI_ID,String MESH,
					int POI_TYPE, Object THE_GEOM, String FA_TYPE,String AREA_FLAG,String GUID) {
			super();
			this.NAME_CHN = NAME_CHN;
			this.POI_ID = POI_ID;
			this.MESH = MESH;
			this.POI_TYPE = POI_TYPE;
			this.THE_GEOM = THE_GEOM;
			this.FA_TYPE = FA_TYPE;
			this.AREA_FLAG = AREA_FLAG;
			this.GUID = GUID;
		}
		
		public int getPOI_TYPE() {
			return POI_TYPE;
		}
		public void setPOI_TYPE(int pOI_TYPE) {
			POI_TYPE = pOI_TYPE;
		}
		public String getMESH() {
			return MESH;
		}
		public void setMESH(String mESH) {
			MESH = mESH;
		}
		public String getFA_TYPE() {
			return FA_TYPE;
		}
		public void setFA_TYPE(String fA_TYPE) {
			FA_TYPE = fA_TYPE;
		}
		public String getAREA_FLAG() {
			return AREA_FLAG;
		}
		public void setAREA_FLAG(String aREA_FLAG) {
			AREA_FLAG = aREA_FLAG;
		}
		public String getGUID() {
			return GUID;
		}
		public void setGUID(String gUID) {
			GUID = gUID;
		}
		
		public String getNAME_CHN() {
			return NAME_CHN;
		}

		@SuppressWarnings("unused")
		public void setNAME_CHN(String nAME_CHN) {
			NAME_CHN = nAME_CHN;
		}

		@SuppressWarnings("unused")
		public String getPOI_ID() {
			return POI_ID;
		}

		@SuppressWarnings("unused")
		public void setPOI_ID(String pOI_ID) {
			POI_ID = pOI_ID;
		}

		public Object getTHE_GEOM() {
			return THE_GEOM;
		}

		@SuppressWarnings("unused")
		public void setTHE_GEOM(Object tHE_GEOM) {
			THE_GEOM = tHE_GEOM;
		}

		@Override
		public String toString() {
			return "('" + NAME_CHN + "', '" + POI_ID + "', '" + THE_GEOM
					+ "'),";
		}
	}

}
