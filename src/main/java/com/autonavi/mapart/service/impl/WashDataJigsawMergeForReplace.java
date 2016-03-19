package com.autonavi.mapart.service.impl;

import java.io.File;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;

import org.apache.log4j.Logger;

import com.autonavi.mapart.service.AbstractWashDataMerge;
import com.autonavi.mapart.service.DateFormat;
import com.autonavi.mapart.service.H2gisServer;
import com.autonavi.mapart.service.WashDataConfig;
/**
 * 输入 AN拼图数据 AN删除的数据 BD新增 step6 step4 1 : 1 替换 step7 查找POI
 * 
 * 
 */
public class WashDataJigsawMergeForReplace  extends AbstractWashDataMerge{
	private Logger mergeLog = Logger.getLogger("mergeLogger");
	private Collection<File> anShpFiles = new ArrayList<File>();
	private String anJigsawDir; //an 拼图路径
	private double areaCoverPercent;
	private String mergeDir; 	// 要替换融合的文件路径
	private String precision;
	private String sources;
	private String updatetime;
	private String proname;
	private String jigsawMergeFile; //拼图融合路径
	public WashDataJigsawMergeForReplace() {
		super();
		WashDataConfig properties = WashDataConfig.getInstance();
		this.anJigsawDir = properties.getProperty("amapFile");
		this.mergeDir = properties.getProperty("replaceMergeFile");
		this.jigsawMergeFile = properties.getProperty("jigsawMergeFile");
		
		this.areaCoverPercent = Double.parseDouble(properties.getProperty("areaCoverPercent"));
		this.precision = properties.getProperty("PRECISION");
	    this.sources = properties.getProperty("SOURCES");
	    this.updatetime = DateFormat.getStringCurrentDateShort2();
	    this.proname = properties.getProperty("PRONAME");
	}

	@Override
	public void merge() {
		mergeLog.debug("<----------------------拼图替换融合开始！ ---------------------->");
		findShpFiles(anJigsawDir, anShpFiles);

		String anDelTable = "TMP_AN_ADD";
		mergeLog.debug("imp an ..................." + mergeDir + "/AN");
		impShps(mergeDir + "/AN", anDelTable, false);
		count(anDelTable);
		String bdReplaceTable = "TMP_BD_ADD";
		mergeLog.debug("imp bd ..................." + mergeDir + "/BD");
		impShps(mergeDir + "/BD", bdReplaceTable, true);
		count(bdReplaceTable);
		for (File file : anShpFiles) {
			final String anMergeTable = "MERGED_AN_TABLE";// an拼图数据导入的表，最终需要输出
			final String mergeTmpTable = "TMP_MERGE_UPDATE";// 临时
			final String interactDelAn = "TMP_MERGED_AN_TABLE";//an删除与单图幅文件的交集
			try {
				H2gisServer.getInstance().importData(file.getAbsolutePath(), anMergeTable);
				mergeLog.info("AN拼图的数据数量:" + count(anMergeTable));
				
				createInteractionAn(anDelTable, anMergeTable, interactDelAn);
				mergeLog.debug("AN拼图与AN替换交集的数量"+count(interactDelAn, false));
				
				createMergeTmpTable(interactDelAn, bdReplaceTable, mergeTmpTable);
				int mergeCount = count(mergeTmpTable,false);
				mergeLog.info("\t需要融合的数据数量:" + mergeCount);
				if (mergeCount == 0) {
					continue;
				}

				deleteAn(anMergeTable, mergeTmpTable);
				insertBdIntoAn(anMergeTable, mergeTmpTable);
				mergeLog.info("\t最终写入数量:" + count(anMergeTable));
				String repalaceMergePath = jigsawMergeFile+"/替换融合/"+"拼图替换融合_4_"+count(anMergeTable)+".shp";
				mergeLog.debug("融合保存路径："+repalaceMergePath);
				H2gisServer.getInstance().outputShape(repalaceMergePath,anMergeTable);
			} finally {
				drop(anMergeTable,mergeTmpTable,interactDelAn);
			}

		}// end for
		
		mergeLog.debug("<----------------------拼图替换融合结束！ ---------------------->");
	}

	private void createInteractionAn(String anDelTable, final String anMergeTable, final String interactAn) {
		H2gisServer.getInstance().execute(
				"create table " + interactAn + " as select distinct an.* from " + anMergeTable + " bd," + anDelTable
						+ " an " +getWhereSql());
	}

	/**
	 * 创建需要融合的临时表
	 */
	private void createMergeTmpTable(String anTable, String bdTable, String mergeTable) {
		String sql = "create table "+ mergeTable+ " as "
				+ " select distinct an.NAME_CHN,an.MESH,an.POI_ID,an.FA_TYPE,an.AREA_FLAG,an.POI_GUID,"
				+ " bd.THE_GEOM from "
				+ bdTable+ " bd, "
				+ anTable + " an "
				+ "where ST_Intersects(bd.THE_GEOM, an.THE_GEOM) = true ";// +getWhereSql();
		H2gisServer.getInstance().execute(sql);
	}

	private String getWhereSql() {
		return " where ST_Area(ST_Intersection(an.THE_GEOM,bd.THE_GEOM))/ST_Area(bd.THE_GEOM)>="
				+ areaCoverPercent ;
	}
	/**
	 * 删除高德的数据
	 */
	private void deleteAn(String anTable, String anDelTable) {
		String sql = "delete from " + anTable + " an where exists ( select 1 from " + anDelTable
				+ " bd  where an.NAME_CHN = bd.NAME_CHN)";
		H2gisServer.getInstance().execute(sql);
		mergeLog.debug("\t删除后AN的数量:" + count(anTable));
	}

	/**
	 * 把临时表中的数据追加到最终的文件中
	 * @throws SQLException 
	 */
	private void insertBdIntoAn(String anTable, String mergeTable) {
		String sql1 = "select distinct the_geom, NAME_CHN,MESH,POI_ID,FA_TYPE,AREA_FLAG,poi_guid"
				+ " from " + mergeTable;
		ResultSet rs = H2gisServer.getInstance().executeQuery(sql1);
		try {
			List<SetResultSet> list2 = new ArrayList<>();
			while(rs.next()){
				String NAME_CHN = rs.getString(2) == null ? "":rs.getString(2);
				String POI_ID = rs.getString(4)== null ? "0":rs.getString(4);
				String THE_GEOM = rs.getString(1)== null ? "":rs.getString(1);
				String MESHCODE = rs.getString(3)== null ? "":rs.getString(3);
				String FA_TYPE = rs.getString(5)== null ? "":rs.getString(5);
				String AREA_FLAG = rs.getString(6)== null ? "0":rs.getString(6);
				String guid = rs.getString(7)== null ? "":rs.getString(7);
				list2.add(new SetResultSet(NAME_CHN, POI_ID,THE_GEOM,
						MESHCODE,FA_TYPE,AREA_FLAG,guid));
			}
			for(SetResultSet list:list2){
				String sql2 = "insert into " + anTable 
						+ " (THE_GEOM,NAME_CHN,POI_ID,MESH,FA_TYPE,AREA_FLAG,poi_guid,"
						+ "PRECISION,SOURCES,UPDATETIME,PRONAME) "
						+ "values('"+list.getTHE_GEOM()+"','"+list.getNAME_CHN()+"','"+list.getPOI_ID()+"','"
						+list.getMESH()+"','"+list.getFA_TYPE()+"','"+list.getAREA_FLAG()+ "','"+list.getGUID()+ "','"
						+ precision + "','"+sources+"','" + updatetime+"','" + proname+"')";
				H2gisServer.getInstance().execute(sql2);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
	}
	
//	private void insertBdIntoAn(String anTable, String mergeTable) {
//		String sql = "insert into " + anTable + " (THE_GEOM,NAME_CHN,MESH,POI_ID,FA_TYPE,AREA_FLAG) "
//				+ " select distinct the_geom, NAME_CHN,MESH,POI_ID,FA_TYPE,AREA_FLAG from " + mergeTable;
//		H2gisServer.getInstance().execute(sql);
//	}the_geom, NAME_CHN,MESH,POI_ID,FA_TYPE,AREA_FLAG,poi_guid
	private class SetResultSet {
		private String NAME_CHN;
		private String POI_ID;
		private Object THE_GEOM;
		private String MESH;
		private String FA_TYPE;
		private String AREA_FLAG;
		private String GUID;
		public SetResultSet(String NAME_CHN, String POI_ID, Object THE_GEOM,
				String MESH, String FA_TYPE, String AREA_FLAG, String guid) {
			super();
			this.NAME_CHN = NAME_CHN;
			this.POI_ID = POI_ID;
			this.THE_GEOM = THE_GEOM;
			this.AREA_FLAG = AREA_FLAG;
			this.FA_TYPE = FA_TYPE;
			this.MESH = MESH;
			this.GUID = guid;
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
		@Override
		public String toString() {
			return "('" + NAME_CHN + "', '" + POI_ID + "', '" + THE_GEOM
					+ "'),";
		}
	}
	

}
