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
import com.autonavi.mapart.service.ReadShap;
import com.autonavi.mapart.service.WashDataConfig;
import com.autonavi.mapart.service.WashDataService;

/**
 * 将已筛选出BD未予AN压盖的部分数据融合到AN分幅数据中 规则对该区域内的POI的POI_TYPE进行筛选，范围：120300-120304，
 * 筛选出名称重复最多，且最短的POI进行相关关联。（暂时关联POI_ID、NAME_CHN）<br>
 * 
 * 输出：<br>
 * AN融合后数据<br>
 * BD融合的数据<br>
 * BD不符合条件的数据<br>
 *
 * @author qiang.cai
 *
 */
public class WashDataJigsawMergeForUnintersect extends AbstractWashDataMerge {

	private Logger mergeLog = Logger
			.getLogger(WashDataJigsawMergeForUnintersect.class);

	private Collection<File> anShpFiles = new ArrayList<File>();
	private String meshDir;// an分幅文件目录
	private String POI_TNAME;// poi table name
	private String POI_FILE;// poi file path
	private static int counter = 0;
	private String outputPath;// 文件输出目录
	private String anMergeDir;  // 替换融合后文件的保存路径
	private String nonCoverMergeFile;// 未压盖融合文件目录
	private String precision;
	private String sources;
	private String updatetime;
	private String proname;
	public WashDataJigsawMergeForUnintersect() {
		super();
		WashDataConfig properties = WashDataConfig.getInstance();
		this.meshDir = properties.getProperty("anMesh");
		POI_TNAME = WashDataService.POI_TNAME;
		POI_FILE = properties.getProperty("poiFile");
		this.outputPath = properties.getProperty("outputPath");
		this.anMergeDir = properties.getProperty("meshMergeFile");
		this.nonCoverMergeFile = properties.getProperty("nonCoverMergeFile");
		this.precision = properties.getProperty("PRECISION");
	    this.sources = properties.getProperty("SOURCES");
	    this.updatetime = DateFormat.getStringCurrentDateShort2();
	    this.proname = properties.getProperty("PRONAME");
	}

	@Override
	public String toString() {
		// for test
		return "WashDataMergeForNew [anShpFiles=" + anShpFiles + ", meshDir="
				+ meshDir + ", mergeDirs=" + nonCoverMergeFile + ", POI_TNAME="
				+ POI_TNAME + ", outputPath=" + outputPath + "]";
	}

	@Override
	public void merge() {
		mergeLog.debug("<----------------------未压盖融合开始！ ---------------------->");
		boolean isMerge = false;
		String TMP = "tmp", 					// 临时表用来存储导入文件时的shapefile的BOX
		bdReplaceTable = "TMP_BD_ADD";			//删除融合符合条件的bd表
		findShpFiles(meshDir, anShpFiles);		//导入an mesh
		
		super.impPOI(POI_FILE, POI_TNAME);		//导入POI表
		mergeLog.info("POI记录数：" + count(POI_TNAME));
		impShps(nonCoverMergeFile, bdReplaceTable, true);
		mergeLog.debug("imp bd ..................." + nonCoverMergeFile);
		mergeLog.info("bd中要新增记录总数：" + count(bdReplaceTable));
		
		final String mergeTmpTable = "TMP_MERGE_NEW";// 生成临时融合表
		try{
			isMerge = createMergeTmpTable(bdReplaceTable, mergeTmpTable);// 提取全部融合数据
			if (isMerge) {
				// 提取全部融合数据 isMerge = true 融合
				String sql = "Create table " + TMP + "(THE_GEOM POLYGON)";// 创建an分幅shap文件BOX空间表 tmp
				H2gisServer.getInstance().execute(sql);// 暂为空表
				for (File file : anShpFiles) {
					final String anMergeTable = "MERGED_AN_TABLE";// 导入an分幅的表，最终需要输出
					try {
						String[] ss = file.getAbsolutePath().split("\\\\");
						String mesh = ss[ss.length - 2];
						H2gisServer.getInstance().importData(file.getAbsolutePath(), anMergeTable);
						H2gisServer.getInstance().execute("delete from " + TMP);// 清空tmp中记录
						String sql2 = "insert into " + TMP + " values(' " + ReadShap.read(file.getAbsolutePath()) + "')";// 插入最新BOX
						H2gisServer.getInstance().execute(sql2);
						insertBdIntoAn( mergeTmpTable, anMergeTable, TMP, mesh);// 开始融入
						log.debug("未压盖的融合路径："+anMergeDir+"/"+ ss[ss.length - 4] + "/" + ss[ss.length - 3] + "/" + ss[ss.length - 2] + "/" + ss[ss.length - 1]);
						H2gisServer.getInstance()
								.outputShape(anMergeDir+"/未压盖融合/"+ ss[ss.length - 4] + "/" + ss[ss.length - 3] + "/" + ss[ss.length - 2] + "/"  + ss[ss.length - 1], anMergeTable);
					}finally{
						drop(anMergeTable);
					}
				}// end for
			}
		}catch (Exception e) {
			log.debug("未压盖融合："+e);
		}finally {
			if (isMerge) {
				drop(mergeTmpTable);
				drop(TMP);
			}
		}
		mergeLog.debug("<----------------------未压盖融合结束！ ---------------------->");
	}

	/**
	 * 创建需要融合的临时表
	 * 
	 * @throws SQLException
	 */
	private boolean createMergeTmpTable(String bdTable, String mergeTable) {
		step1(bdTable, mergeTable);
		return step2(bdTable, mergeTable);
	}

	/**
	 * 过滤mergeTable中的GEOM的数据重复记录，留下与主POI关联的记录
	 * 
	 * @param bdTable
	 * @param mergeTable
	 * @throws SQLException
	 */
	private boolean step2(String bdTable, String mergeTable) {
		ResultSet reslut2 = H2gisServer.getInstance().executeQuery(
				"select NAME_CHN, POI_ID, BD_THE_GEOM FROM " + mergeTable
						+ " order by BD_THE_GEOM, NAME_CHN");
		List<SetResultSet> list2 = new ArrayList<>();
		try {
			while (reslut2.next()) {
				list2.add(new SetResultSet(reslut2.getString(1), reslut2
						.getString(2), reslut2.getObject(3)));
			}
		} catch (Exception e) {
			e.printStackTrace();
			return false;
		}

		// 标记第一条记录
		SetResultSet setkey = list2.get(0);// 标记第一个为KEY
		Object objectkey = setkey.getTHE_GEOM();
		SetResultSet set = null;
		Object object = null;
		String NAME_CHN = setkey.getNAME_CHN();
		StringBuffer ss = null;

		// 将第一个记录中的NAME_CHN插入
		if ("".equals(NAME_CHN)) {
			ss = new StringBuffer("");
		} else {
			ss = new StringBuffer(setkey.getNAME_CHN() + ",");
		}

		List<String> rs = new ArrayList<>();// 保存tmp表中需要留下的记录
		for (int i = 1; i < list2.size() - 1; i++) {
			set = list2.get(i);
			object = set.getTHE_GEOM();
			NAME_CHN = set.getNAME_CHN();
			if (NAME_CHN != null && !"".equals(NAME_CHN)) { // 排除NAME_CHN为空的记录
				if (object.equals(objectkey)) {
					ss.append(NAME_CHN + ",");
					if (i + 2 == list2.size()) { // 如果 遍历到list的倒数第三个的时候
						set = list2.get(i + 1);
						if (set.getTHE_GEOM().equals(objectkey)) {
							ss.append(set.getNAME_CHN() + ",");
							String string = executeResultSet(ss.toString(),
									setkey, mergeTable);
							if (!"".equals(string)) {
								rs.add(string);
							}
						}// end
					}// end
				} else {
					String string = executeResultSet(ss.toString(), setkey,
							mergeTable);
					if (!"".equals(string)) {
						rs.add(string);
					}
					setkey = set;
					objectkey = object;
					ss = new StringBuffer("");
					ss.append(set.getNAME_CHN() + ",");
					if (i + 2 == list2.size()) {
						set = list2.get(i + 1);
						if (set.getTHE_GEOM().equals(objectkey)) {
							ss.append(set.getNAME_CHN() + ",");
							string = executeResultSet(ss.toString(), setkey,
									mergeTable);
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
					"delete from " + mergeTable + " bd where"
							+ " bd.NAME_CHN not in ("
							+ org.apache.commons.lang.StringUtils.join(rs, ",")
							+ ")");
		}
		return true;
	}

	/**
	 * 找出全部的BD数据与POI相交的数据集插入到TMP表中<br>
	 * 
	 * @param anTable
	 * @param bdTable
	 * @param mergeTable
	 */
	private void step1(String bdTable, String mergeTable) {
		String sql = "create table "+ mergeTable + " as "
				+ "select poi.NAME_CHN, poi.POI_ID, poi.MESHCODE,poi.guid, poi.THE_GEOM, bd.THE_GEOM BD_THE_GEOM FROM "
				+ bdTable + " bd" + " left join  " + POI_TNAME + " poi WHERE  "
				+ " ST_Contains(bd.THE_GEOM, poi.THE_GEOM) = true ";
		H2gisServer.getInstance().execute(sql);
		int cout1 = count(mergeTable);
		mergeLog.info(mergeTable + "和poi相匹配的融合记录数：" +cout1 );

		if (cout1 > 0) {
			H2gisServer.getInstance().execute("delete FROM "+ bdTable + " bd"
									+ " where exists( select 1 from "
									+ POI_TNAME + " poi WHERE  "
									+ " ST_Contains(bd.THE_GEOM, poi.THE_GEOM) = true)");
			mergeLog.info(bdTable + "和poi相匹配的bd剩余的记录数：" +count(bdTable));
		}// end if
		String sql2 = "INSERT INTO " + mergeTable
				+ "(BD_THE_GEOM) SELECT THE_GEOM FROM " + bdTable;
		H2gisServer.getInstance().execute(sql2);
		mergeLog.info("需要融合的表"+mergeTable + "记录数：" + count(mergeTable));
	}


	/**
	 * 把临时表中的数据追加到最终的文件中 规则： 融入表中的数据 <br>
	 * 如有有POI点则找出POI点所在an图幅，将该记录融入<br>
	 * 否则 将记录融入与之相交的an图幅中
	 * 
	 * @param anTable
	 * @param mergeTable
	 * @param meshcode
	 *            TODO
	 * @param countdelete
	 *            TODO
	 * @param tmp
	 */
	private void insertBdIntoAn(String mergeTable, String anMergeTable, String TMP, String meshcode) {
		
		int count2 = step3(anMergeTable, mergeTable, TMP, "THE_GEOM",meshcode);
		mergeLog.info("\t需要融合的数据数量:" + count(anMergeTable));
	}

	private int step3(String anTable, String mergeTable, String TMP, String geom_string, String meshcode) {
		int count1 = count(anTable);
		mergeLog.info(meshcode + "导入分幅的原始数据数量:" + count1);
		String sql1 =  "select  m.BD_THE_GEOM, m.NAME_CHN, m.POI_ID, m.MESHCODE m.guid "
				+ "from "+ mergeTable + " m , " + TMP + " T"
				+ " WHERE ST_Intersects(m."+ geom_string + ", T.THE_GEOM)=true";
		ResultSet rs = H2gisServer.getInstance().executeQuery(sql1);
		List<SetResultSet> list2 = new ArrayList<>();
		try {
			while(rs.next()){
				mergeLog.debug("NAME_CHN======"+rs.getString(2));
				mergeLog.debug("POI_ID======"+rs.getString(3));
				mergeLog.debug("BD_THE_GEOM======"+rs.getObject(1));
				mergeLog.debug("MESHCODE======"+rs.getString(4));
				mergeLog.debug("guid======"+rs.getString(5));
				String NAME_CHN = rs.getString(1) == null ? "":rs.getString(2);
				String POI_ID = rs.getString(2)== null ? "0":rs.getString(3);
				String BD_THE_GEOM = rs.getString(3)== null ? "":rs.getString(1);
				String MESHCODE = rs.getString(4)== null ? "":rs.getString(4);
				String guid = rs.getString(7)== null ? "":rs.getString(5);
				SetResultSet sts = new SetResultSet();
				sts.setNAME_CHN(NAME_CHN);
				sts.setPOI_ID(POI_ID);
				sts.setTHE_GEOM(BD_THE_GEOM);
				sts.setMESH(MESHCODE);
				sts.setGUID(guid);
				list2.add(sts);
			}
			for(SetResultSet list:list2){
				String sql2 = "insert into " + anTable 
						+ " (THE_GEOM,NAME_CHN,POI_ID,MESH,poi_guid,"
						+ "PRECISION,SOURCES,UPDATETIME,PRONAME) "
						+ "values('"+list.getTHE_GEOM()+"','"+list.getNAME_CHN()+"','"+list.getPOI_ID()+"','"
						+list.getMESH()+"','"+list.getGUID()+ "','"
						+ precision + "','"+sources+"','" + updatetime+"','" + proname+"')";
				H2gisServer.getInstance().execute(sql2);
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		int count2 = count(anTable);
		mergeLog.debug("融合后的拼图数量："+count2);
		// 删除已融入的记录
		String sql2 = "delete from " + mergeTable + " m where exists ("
				+ " select  1 from " + TMP + " T WHERE ST_Intersects(m."
				+ geom_string + ", T.THE_GEOM)=true)";
		H2gisServer.getInstance().execute(sql2);
		mergeLog.debug("删除融合的MergeTable剩余的数量："+count(mergeTable));
		return count2 - count1;
	}

	/**
	 * 统计数据表中数据量并统计关键字 统计规则 :
	 * 对该区域内的POI的POI_TYPE进行筛选，范围：120300-120304，筛选出名称重复最多，且最短的POI进行相关关联
	 * 
	 * @param tableName
	 *            数据表名
	 * @throws SQLException
	 */
	private String executeResultSet(String str, SetResultSet set,
			String mergeTable) {
		boolean haveReplaceAll = false;
		str = str.substring(0, str.length() - 1);
		if (str.indexOf(")") > -1) {
			haveReplaceAll = true;
			str = str.replaceAll("\\)", "");
		}

		String[] strmap = str.split(",");

		if (strmap.length >= 2) {
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
			try {
				H2gisServer.getInstance().execute(
						"delete from " + mergeTable + " where"
								+ " NAME_CHN ='' and  BD_THE_GEOM='"
								+ set.getTHE_GEOM() + "'");
			} catch (Exception e) {
				mergeLog.info(e);
			}
			if (haveReplaceAll && strmap[i].indexOf("(") > -1) {
				haveReplaceAll = true;
			} else {
				haveReplaceAll = false;
			}
			return "'" + strmap[i] + (haveReplaceAll ? ")" : "") + "'";
		} // end if
		return "";
	}

	// 字符串遍历
	private static int stringNumbers(String str, String f) {
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

	private class SetResultSet {
		String NAME_CHN;
		String POI_ID;
		Object THE_GEOM;
		private String MESH;
		private String FA_TYPE;
		private String AREA_FLAG;
		private String GUID;
		
		
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
		public  SetResultSet() {}
		public SetResultSet(String NAME_CHN, String POI_ID, Object THE_GEOM) {
			super();
			this.NAME_CHN = NAME_CHN;
			this.POI_ID = POI_ID;
			this.THE_GEOM = THE_GEOM;
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
