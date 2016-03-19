package com.autonavi.mapart.service.impl;

import java.io.UnsupportedEncodingException;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

import org.apache.commons.lang.StringUtils;

import com.autonavi.mapart.service.H2gisServer;
import com.autonavi.mapart.service.WashDataConfig;
import com.autonavi.mapart.service.WashDataService;

/**
 * 1、筛选出BD未予AN压盖的部分<br>
 * 2、将该部分融合到AN数据中<br>
 * 3、对该区域内的POI的POI_TYPE进行筛选，范围：120300-120304，
 * 筛选出名称重复最多，且最短的POI进行相关关联。（暂时关联POI_ID、NAME_CHN）<br>
 * 
 * 输出：<br>
 * AN融合后数据<br>
 * BD融合的数据<br>
 * BD不符合条件的数据<br>
 */
public class WashDataForFusion extends WashDataService {

	private String outFilePath;
	private String BD_2_R;
	private WashedData preWashedData;
	private static int counter = 0;
	private String nonCoverMergeFile;        //未压盖融合路径
	final String bdBakTname = BD_TNAME + "_BAK_C";
	final String BD_MERGE_C = "BD_MERGE_C";

	public WashDataForFusion(WashedData washedData) {
		super();
		WashDataConfig properties = WashDataConfig.getInstance();
		this.outFilePath = properties.getProperty("outputPath");
		this.preWashedData = washedData;
		this.nonCoverMergeFile = properties.getProperty("nonCoverMergeFile");
	}

	// fro test
	public WashDataForFusion(String outFilePath, WashedData preWashedData) {
		super();
		this.outFilePath = outFilePath;
		this.preWashedData = preWashedData;
	}
	
	@Override
	public WashedData wash() {
		BD_2_R = preWashedData.getResultTableName();
		WashDataService.WashedData washedData = new WashDataService.WashedData(
				outFilePath + "/3/g/BD融合的数据.shp", outFilePath + "/3/r/BD不符合融合数据.shp", 
				BD_MERGE_C, bdBakTname);
		try {
			//输出bd不满足条件的数据（BD与AN压盖）
			step1();
			H2gisServer.getInstance().outputShape(washedData.getResultFile(),
					washedData.getResultTableName());
			/**
			 * 输出BD未予AN压盖的部分(BD符合条件的数据)
			 */
			step2();

			/**
			 * 输出BD未予AN压盖的部分与POI的点包含关系数据进行筛选
			 * 
			 */
			step3();

			/**
			 * 输出 BD融合的数据 输出 AN融合后数据
			 * 
			 */
			if (step4()) {
				H2gisServer.getInstance().outputShape(washedData.getGarbageFile(),
						washedData.getGarbageTableName());
				H2gisServer.getInstance().outputShape(
						nonCoverMergeFile+"/BD未与AN压盖_3_"+count(washedData.getGarbageTableName())+".shp",
						washedData.getGarbageTableName());
				
				H2gisServer.getInstance().outputShape(
						outFilePath + "/a/AN数据融合后.shp", AUTONAVI_TNAME);
			}
		} catch (Exception e) {
			e.printStackTrace();
		}
		return washedData;
	}

	private boolean step4() throws SQLException, UnsupportedEncodingException {
		ResultSet result = H2gisServer.getInstance().executeQuery(
				"select NAME_CHN, POI_ID, THE_GEOM  FROM BD_MERGE_C");
		List<String> list = new ArrayList<String>();
		while (result.next()) {
			list.add("('" +  new String(result.getString(1).getBytes("UTF-8")) + "', " + result.getString(2)
					+ ", '" + result.getString(3) + "')");
		}
		log.info("AN数据融合前:");
		count(AUTONAVI_TNAME);
		if (list.size() == 0) {
			log.info("AN数据无融合!");
			return false;
		} else {
			H2gisServer.getInstance().execute(
					"insert into " + AUTONAVI_TNAME
							+ "(NAME_CHN, POI_ID, THE_GEOM) values "
							+ StringUtils.join(list, ","));
			log.info("AN数据融合后:");
			count(AUTONAVI_TNAME);
			return true;
		}
	}

	private void step3() throws SQLException {
		// 找出THE_GEOM相同的记录
		String sql = "select NAME_CHN, POI_ID, THE_GEOM  FROM "+ BD_MERGE_C + " order by THE_GEOM";
		ResultSet reslut1 = H2gisServer.getInstance().executeQuery(sql);
		List<SetResultSet> list = new ArrayList<SetResultSet>();
		while (reslut1.next()) {
			list.add(new SetResultSet(reslut1.getString(1), reslut1
					.getString(2), reslut1.getObject(3)));
		}

		if (list.size() == 0) {
			log.info("BD无符合融合的数据！");
		} else {
			// 标记第一条记录
			SetResultSet setkey = list.get(0);
			Object objectkey = setkey.getTHE_GEOM();
			SetResultSet set = null;
			Object object = null;
			String NAME_CHN = setkey.getNAME_CHN();
			StringBuffer ss = null;
			if ("".equals(NAME_CHN)) {
				ss = new StringBuffer("");
			} else {
				ss = new StringBuffer(setkey.getNAME_CHN() + ",");
			}

			List<String> rs = new ArrayList<String>();
			for (int i = 1; i < list.size() - 1; i++) {
				set = list.get(i);
				object = set.getTHE_GEOM();
				NAME_CHN = set.getNAME_CHN();
				if (!"".equals(NAME_CHN)) {
					ss.append(NAME_CHN + ",");
					if (object.equals(objectkey)) {
						if (i + 2 == list.size()) {
							set = list.get(i + 1);
							if (set.getTHE_GEOM().equals(objectkey)) {
								ss.append(set.getNAME_CHN() + ",");
								String string = executeResultSet(ss.toString(),
										setkey);
								if (!"".equals(string)) {
									rs.add(string);
								}
							}
						}
					} else {
						String string = executeResultSet(ss.toString(), setkey);
						if (!"".equals(string)) {
							rs.add(string);
						}
						setkey = set;
						objectkey = object;
						ss = new StringBuffer("");
						ss.append(set.getNAME_CHN() + ",");
						if (i + 2 == list.size()) {
							set = list.get(i + 1);
							if (set.getTHE_GEOM().equals(objectkey)) {
								ss.append(set.getNAME_CHN() + ",");
								string = executeResultSet(ss.toString(), setkey);
								if (!"".equals(string)) {
									rs.add(string);
								}
							}

						}
					}
				}// end if
			}// end for
			if (rs.size() > 0) {
				rs.add("''");
				H2gisServer.getInstance().execute(
						"delete from BD_MERGE_C bd where"
								+ " bd.NAME_CHN not in ("
								+ org.apache.commons.lang.StringUtils.join(rs,
										",") + ")");
			}
		}// end if

		count("BD_MERGE_C");
		if (reslut1 != null) {
			reslut1.close();
		}

	}

	private void step1() throws SQLException {
		H2gisServer.getInstance().execute(
				"create table " + bdBakTname + " as select * from " + BD_2_R
						+ " bd where EXISTS( select 1 from " + AUTONAVI_TNAME
						+ " an  where "
						+ " ST_Intersects(an.THE_GEOM, bd.THE_GEOM) = true)");
		log.debug("BD不符合融合数据数量:"+count(bdBakTname));
		count(bdBakTname);
	}

	// 判断是否相交：BOOLEAN ST_Intersects(GEOMETRY geomA, GEOMETRY geomB);
	// 判断是否相接：BOOLEAN ST_Touches(GEOMETRY geomA, GEOMETRY geomB);
	private void step2() throws SQLException {
		String sql1 = "delete from " + BD_2_R + " bd where EXISTS( select 1 from "
				+ AUTONAVI_TNAME + " an where "
				+ " ST_Intersects(an.THE_GEOM, bd.THE_GEOM) = true)";
		H2gisServer.getInstance().execute(sql1);
		log.info("满足BD融合条件数据数量："+count(BD_2_R));
		
		// 创建新表 BD_MERGE_C， 数据为BD中与AN不压盖区域与该区域中POI点的积(M x N)
		String sql2 = "create table " + BD_MERGE_C + " as "
				+ "select NAME_CHN , POI_ID, THE_GEOM  from " + AUTONAVI_TNAME + " where 1=2";
		H2gisServer.getInstance().execute(sql2);
		
		//插入满足条件bd的geom；
		ResultSet set1 = H2gisServer.getInstance().executeQuery(
				"select distinct THE_GEOM FROM " + BD_2_R);
		List<String> list1 = new ArrayList<String>();
		while (set1.next()) {
			list1.add("('', '" + set1.getString(1) + "')");
		}
		H2gisServer.getInstance().execute("insert into " + BD_MERGE_C + "(NAME_CHN, THE_GEOM) values "
						+ org.apache.commons.lang.StringUtils.join(list1, ","));
		
		//插入bd的geom与poi相关联的数据
		String sql3 = "select distinct poi.NAME_CHN,  poi.POI_ID, bd.THE_GEOM FROM "
				+ BD_MERGE_C + " bd left join  " + POI_TNAME + " poi WHERE  "
				+ " ST_Contains(bd.THE_GEOM, poi.THE_GEOM) = true ";
		ResultSet set2 = H2gisServer.getInstance().executeQuery(sql3);
		List<String> list2 = new ArrayList<String>();
		while (set2.next()) {
			list2.add("('" + set2.getString(1) + "', '" + set2.getString(2)
					+ "', '" + set2.getString(3) + "')");
		}
		if (list2.size() > 0) {
			H2gisServer.getInstance().execute("insert into BD_MERGE_C values "
							+ StringUtils.join(list2, ","));
		}
		
		log.debug("满足条件的bd数据和poi相关联后的数量："+count("BD_MERGE_C"));
	}

	/**
	 * 统计数据表中数据量并统计关键字 统计规则 :
	 * 对该区域内的POI的POI_TYPE进行筛选，范围：120300-120304，筛选出名称重复最多，且最短的POI进行相关关联
	 * 
	 * @param tableName
	 *            数据表名
	 * @throws SQLException
	 */
	public String executeResultSet(String str, SetResultSet set)
			throws SQLException {
		String[] strmap = str.split(",");
		if (strmap.length > 2) {
			// 处理字符串
			int k = 0;
			int i = 0;
			for (int j = 0; j < strmap.length - 1; j++) {
				counter = 0;
				int i1 = stringNumbers(str, strmap[j]);
				if (i1 > k) {
					k = i1;
					i = j;
				}
			}

			H2gisServer.getInstance().execute(
					"delete from BD_MERGE_C bd where"
							+ " bd.NAME_CHN ='' and THE_GEOM='"
							+ set.getTHE_GEOM() + "'");
			return "'" + strmap[i] + "'";
		} // end if
		return "";
	}

	// 字符串遍历
	public static int stringNumbers(String str, String f) {
		int k = str.indexOf(f);
		if (k == -1) {
			return 0;
		} else if (k != -1) {
			counter++;
			if (str.length() > k + f.length()) {

			}
			stringNumbers(str.substring(k + f.length()), f);
			return counter;
		}
		return 0;
	}

	class SetResultSet {
		String NAME_CHN;
		String POI_ID;
		Object THE_GEOM;

		public SetResultSet(String NAME_CHN, String POI_ID, Object THE_GEOM) {
			super();
			this.NAME_CHN = NAME_CHN;
			this.POI_ID = POI_ID;
			this.THE_GEOM = THE_GEOM;
		}

		public String getNAME_CHN() {
			return NAME_CHN;
		}

		public void setNAME_CHN(String nAME_CHN) {
			NAME_CHN = nAME_CHN;
		}

		public String getPOI_ID() {
			return POI_ID;
		}

		public void setPOI_ID(String pOI_ID) {
			POI_ID = pOI_ID;
		}

		public Object getTHE_GEOM() {
			return THE_GEOM;
		}

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
