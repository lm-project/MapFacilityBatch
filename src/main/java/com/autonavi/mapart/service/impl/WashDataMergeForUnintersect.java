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
public class WashDataMergeForUnintersect extends AbstractWashDataMerge {

	private Logger mergeLog = Logger
			.getLogger(WashDataMergeForUnintersect.class);

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
	public WashDataMergeForUnintersect() {
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
			// 提取全部融合数据 isMerge = true 融合
			step1(bdReplaceTable, mergeTmpTable);
			isMerge = step2( bdReplaceTable, mergeTmpTable);
////			isMerge = createMergeTmpTable(bdReplaceTable, mergeTmpTable);// 提取全部融合数据
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
	/*private boolean createMergeTmpTable(String bdTable, String mergeTable) {
		step1(bdTable, mergeTable);
		return step2(bdTable, mergeTable);
	}*/

	/**
	 * 过滤mergeTable中的GEOM的数据重复记录，留下与主POI关联的记录
	 * 
	 * @param bdTable
	 * @param mergeTable
	 * @throws SQLException
	 */
	private boolean step2(String bdTable, String mergeTable)throws SQLException {
		String sql = "select NAME_CHN, POI_ID, BD_THE_GEOM,POI_TYPE FROM " + mergeTable
						+ " order by BD_THE_GEOM, NAME_CHN";
		ResultSet reslut2 = H2gisServer.getInstance().executeQuery(sql);
		List<SetResultSet> list2 = new ArrayList<SetResultSet>();
		while (reslut2.next()) {
			list2.add(new SetResultSet(reslut2.getString(1),reslut2.getString(2),
					reslut2.getObject(3),reslut2.getInt(4)));
		}
		
		Object last_the_geom = list2.get(0).getTHE_GEOM();
		StringBuffer name_chn_str = new StringBuffer();
		Collection<SetResultSet> list1 = new ArrayList<SetResultSet>();
		for(SetResultSet list : list2){
			Object the_geom = list.getTHE_GEOM();
			String name_chn = list.getNAME_CHN();
			if (name_chn == null || "".equals(name_chn)) { // 排除NAME_CHN为空的记录
				continue;
			}
			if ( last_the_geom.equals(the_geom) || name_chn_str.length() == 0 ) {
				log.debug("========有poi的重复数据："+name_chn);
				log.debug(the_geom);
				list1.add(list);
				name_chn_str.append( name_chn + "," );//name_chn_str 存放的是NAME_CHN非空且bd的the_geom 有重复 
			}else {
				String string = executeResultSet(list1,mergeTable,name_chn_str.toString());
				log.debug("--------不重复的数据："+name_chn);
				log.debug(the_geom);
				list1.removeAll(list1);
				last_the_geom = the_geom;
				name_chn_str = new StringBuffer(name_chn + ",");
				list1.add(list);
			}
		}
		executeResultSet(list1,mergeTable,name_chn_str.toString());
		log.debug("去重poi后剩余的数量=======:"+count(mergeTable));
		return true;

	}
	/**
	 * 统计数据表中数据量并统计关键字 统计规则 :
	 * 对该区域内的POI的POI_TYPE进行筛选，范围：120300-120304，筛选出名称重复最多，且最短的POI进行相关关联
	 * 
	 * @param tableName
	 *            数据表名
	 * @throws SQLException
	 */
	private String executeResultSet(Collection<SetResultSet> list1,
			String mergeTable,String name_chn_str) {
		log.debug("去重的参数信息："+list1.toString());
		log.debug("重复的字串："+name_chn_str);
		Collection<String> name_chn_col =  new ArrayList<String>();
		int nums = 0;
		String name_chn = "";
		Object the_geom = null;
		log.debug("重复的数量："+list1.size());
		if(list1.size()<=1){return "";}
		for(SetResultSet srs:list1){
			the_geom = srs.getTHE_GEOM();
			int poi_type = srs.getPOI_TYPE();
			name_chn = srs.getNAME_CHN();
			if(poi_type>=120300&&poi_type<=120304){
				counter = 0;
				int num = stringNumbers(name_chn_str,name_chn);
				log.debug("去重的子串："+name_chn+"频率大小："+num);
				if(num>nums){
					nums = num;
					name_chn_col.clear();
					name_chn_col.add(name_chn);
				}else if(num==nums){
					name_chn_col.add(name_chn);
				}
				
			}else{
				name_chn_col.add(name_chn);
			}
		}
		String name_ch = "";
		log.debug("去重留下的字串数量："+name_chn_col.size());
		if(name_chn_col.size()>=1){
			for(String name : name_chn_col){
				log.debug("去重留下的字串："+name);
				name_ch = name_ch.equals("")?name : 
					name_ch.length()>name.length() ? name : name_ch;
			}
		}
		mergeLog.debug("name_chn："+name_ch);
		try {
			String sql1 = "delete from " + mergeTable + " bd where"
					+ " bd.NAME_CHN not in ('"+ name_ch +"')"
					+ " and bd.BD_THE_GEOM='"+ the_geom + "'";
			H2gisServer.getInstance().execute( sql1 );
			mergeLog.debug("删除重复的记录后剩余的数量："+count(mergeTable));
		} catch (Exception e) {
			mergeLog.info(e);
		}
//		return "'" +name_ch+ "'";
		return null;
	}

	/**
	 *给bd的geom关联an和poi参数
	 * @param ree 
	 * @param anTable
	 * @param bdTable
	 * @param mergeTable
	 * @throws SQLException
	 */
    private void insertDataToMergeTable(Collection<SetResultSet> ree,String mergeTable,String mergeTmpTable){
    	createEmptyTable(mergeTmpTable, mergeTable);
    	for(SetResultSet list:ree){
    		String sql1 = "INSERT INTO " + mergeTable + "(NAME_CHN,POI_ID,MESHCODE,"
    				+ " poi_type,BD_THE_GEOM,guid) "
    				+ "values('"+list.getNAME_CHN()+"','"+list.getPOI_ID()+"','"
					+list.getMESH()+"','"+list.getPOI_TYPE()+"','"+list.getTHE_GEOM()+"','"
    				+list.getGUID()+ "')";
    		H2gisServer.getInstance().execute(sql1);
		}
    }
	
	private void step1(String bdTable, String mergeTable) throws SQLException {
		String mergeTmpTable = "mergeTmpTable";
		String sql = "create table "+ mergeTmpTable + " as "
				+ "select distinct poi.NAME_CHN,poi.POI_ID,poi.MESHCODE,poi.guid, poi.THE_GEOM,poi.poi_type, "
				+ "bd.THE_GEOM BD_THE_GEOM FROM "
				+ bdTable + " bd left join " + POI_TNAME + " poi "
				+ "WHERE ST_Contains(bd.THE_GEOM, poi.THE_GEOM) = true ";
		H2gisServer.getInstance().execute(sql);
		
		log.debug("临时融合表的数量："+count(mergeTmpTable));
		
		String sql1 = "select distinct poi.NAME_CHN,poi.POI_ID,poi.MESHCODE,poi.guid, poi.THE_GEOM,poi.poi_type, "
				+ "bd.THE_GEOM BD_THE_GEOM FROM "
				+ bdTable + " bd left join " + POI_TNAME + " poi "
				+ "WHERE ST_Contains(bd.THE_GEOM, poi.THE_GEOM) = true ";
		ResultSet rs = H2gisServer.getInstance().executeQuery(sql1);
		Collection<SetResultSet> ree = new ArrayList<SetResultSet>();
		while(rs.next()){
			SetResultSet rss = new SetResultSet();
			rss.setNAME_CHN(rs.getString(1));
			rss.setPOI_ID(rs.getString(2));
			rss.setMESH(rs.getString(3));
			rss.setGUID(rs.getString(4));
			rss.setPOI_TYPE(rs.getInt(6));
			rss.setTHE_GEOM(rs.getString(7));
			boolean ok  = true;
			for(SetResultSet r:ree ){
				if(r.getNAME_CHN().equals(rs.getString(1))&&r.getTHE_GEOM().equals(rs.getString(7))){
					ok = false;
				}
			}
			if(ok){ree.add(rss);}
			log.debug("---------------------长度为："+ree.size());
		}
		insertDataToMergeTable(ree,mergeTable,mergeTmpTable);
		int cout1 = count(mergeTable);
		mergeLog.info(mergeTable + "和poi相匹配的融合记录数(有重复，因为一个geom可能匹配多个poi)：" +cout1 );
//	          删除与poi关联的bd数据
		if (cout1 > 0) {
			String sql2 = "delete FROM " + bdTable + " bd"
						+ " where exists( select 1 from " + POI_TNAME + " poi WHERE "
						+ " ST_Contains(bd.THE_GEOM, poi.THE_GEOM) = true )";
			H2gisServer.getInstance().execute(sql2);
			mergeLog.info(bdTable + "和poi相匹配的bd剩余的记录数：" +count(bdTable));
		}
//		和poi不匹配的bd数据放入融合表
		String sql3 = "INSERT INTO " + mergeTable + "(BD_THE_GEOM) "
				+ "SELECT bd.THE_GEOM FROM " 
				+ bdTable +" bd";
		H2gisServer.getInstance().execute(sql3);
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
		int count2 = step3(anMergeTable, mergeTable, TMP, "BD_THE_GEOM",meshcode);
		mergeLog.info("\t需要融合的数据数量:" + count2);
	}

	private int step3(String anTable, String mergeTable, String TMP, String geom_string, String meshcode) {
		int count1 = count(anTable);
		mergeLog.info(meshcode + "导入分幅的原始数据数量:" + count1);
		String sql1 =  "select m.BD_THE_GEOM,m.NAME_CHN,m.POI_ID,m.MESHCODE,m.guid "
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
				String NAME_CHN = rs.getString(2) == null ? "":rs.getString(2);
				String POI_ID = rs.getString(3)== null ? "0":rs.getString(3);
				String BD_THE_GEOM = rs.getString(1)== null ? "":rs.getString(1);
				String MESHCODE = rs.getString(4)== null ? "":rs.getString(4);
				String guid = rs.getString(5)== null ? "":rs.getString(5);
				SetResultSet sts = new SetResultSet();
				sts.setNAME_CHN(NAME_CHN);
				sts.setPOI_ID(POI_ID);
				sts.setTHE_GEOM(BD_THE_GEOM);
				sts.setMESH(MESHCODE);
				sts.setGUID(guid);
				sts.setFA_TYPE("");
				sts.setAREA_FLAG("0");
				list2.add(sts);
			}
			for(SetResultSet list:list2){
				String sql2 = "insert into " + anTable 
						+ " (THE_GEOM,NAME_CHN,POI_ID,MESH,FA_TYPE,AREA_FLAG,poi_guid,"
						+ "PRECISION,SOURCES,UPDATETIME,PRONAME) "
						+ "values('"+list.getTHE_GEOM()+"','"+list.getNAME_CHN()+"','"+list.getPOI_ID()+"','"
						+list.getMESH()+"','"+list.getFA_TYPE()+ "','"+list.getAREA_FLAG()+ "','"+list.getGUID()+ "','"
						+ precision + "','"+sources+"','" + updatetime+"','" + proname+"')";
				H2gisServer.getInstance().execute(sql2);
			}
			
		} catch (Exception e) {
			e.printStackTrace();
		}
		
		int count2 = count(anTable);
		mergeLog.debug("融合后分幅的数量："+count2);
		// 删除已融入的记录
		String sql2 = "delete from " + mergeTable + " m where exists ("
				+ " select  1 from " + TMP + " T WHERE ST_Intersects(m."
				+ geom_string + ", T.THE_GEOM)=true)";
		H2gisServer.getInstance().execute(sql2);
		mergeLog.debug("删除融合的MergeTable剩余的数量："+count(mergeTable));
		return count2 - count1;
	}


	/**
	 * jjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjjj
	 * 直接发布到渲染（搜索）
	 * amap发布
	 * 背景CMS
	 * 
	 * 统计数据表中数据量并统计关键字 统计规则 :
	 * 对该区域内的POI的POI_TYPE进行筛选，范围：120300-120304，筛选出名称重复最多，且最短的POI进行相关关联
	 * 
	 * @param tableName
	 *            数据表名
	 * @throws SQLException
	 */
//	private String executeResultSet(String str, SetResultSet set,
//			String mergeTable) {
//		boolean haveReplaceAll = false;
//		str = str.substring(0, str.length() - 1);
//		if (str.indexOf(")") > -1) {
//			haveReplaceAll = true;
//			str = str.replaceAll("\\)", "");
//		}
//
//		String[] strmap = str.split(",");
//
//		if (strmap.length >= 2) {
//			// 处理字符串
//			int k = 0;
//			int i = 0;
//			for (int j = 0; j < strmap.length - 1; j++) {
//				counter = 0;
//				int i1 = stringNumbers(str, strmap[j]);
//				if (i1 > k) {
//					k = i1;
//					i = j;
//				}
//			}
//			try {
//				H2gisServer.getInstance().execute(
//						"delete from " + mergeTable + " where"
//								+ " NAME_CHN ='' and  BD_THE_GEOM='"
//								+ set.getTHE_GEOM() + "'");
//			} catch (Exception e) {
//				mergeLog.info(e);
//			}
//			if (haveReplaceAll && strmap[i].indexOf("(") > -1) {
//				haveReplaceAll = true;
//			} else {
//				haveReplaceAll = false;
//			}
//			return "'" + strmap[i] + (haveReplaceAll ? ")" : "") + "'";
//		} // end if
//		return "";
//	}

//	// 字符串遍历
//	private static int stringNumbers(String str, String f) {
//		int k = str.indexOf(f);
//		if (k == -1) {
//			return 0;
//		} else if (k != -1) {
//			counter++;
//			if (str.length() > k + f.length()) {
//
//			}
//			stringNumbers(str.substring(k + f.length()), f);
//			return counter;
//		}
//		return 0;
//	}
	// 字符串遍历
	private static int stringNumbers(String str, String f) {
		int k = str.indexOf(f);
		System.out.println("---------------------------------------------------------------"+k);
		if (k == -1) {
			return counter;
		} else if (k != -1) {
			counter++;
			stringNumbers(str.substring(k + f.length()), f);
			return counter;
		}
		return counter;
	}

	private class SetResultSet {
		String NAME_CHN;
		String POI_ID;
		Object THE_GEOM;
		private String MESH;
		private String FA_TYPE;
		private String AREA_FLAG;
		private String GUID;
		private int POI_TYPE;
		
		public  SetResultSet() {}
		public SetResultSet(String NAME_CHN, String POI_ID, Object THE_GEOM, int POI_TYPE) {
			super();
			this.NAME_CHN = NAME_CHN;
			this.POI_ID = POI_ID;
			this.THE_GEOM = THE_GEOM;
			this.POI_TYPE = POI_TYPE;
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
