package com.autonavi.mapart.service.impl;

import java.io.File;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import org.apache.log4j.Logger;

import com.autonavi.mapart.service.AbstractWashDataMerge;
import com.autonavi.mapart.service.DateFormat;
import com.autonavi.mapart.service.H2gisServer;
import com.autonavi.mapart.service.ReadShap;
import com.autonavi.mapart.service.WashDataConfig;
import com.autonavi.mapart.service.WashDataService;

/**
 * 对@reference WashDataHighMatch的导出结果(未排除)进行删除融合
 * 
 * @author qiang.cai
 *
 */
public class WashDataMergeForDelete extends AbstractWashDataMerge {

	private Logger mergeLog = Logger.getLogger(WashDataMergeForDelete.class);
	
	private Collection<File> anShpFiles = new ArrayList<File>();
	private String meshDir;// an分幅文件目录
	private String POI_TNAME;// poi table name
	private String POI_FILE;// poi file path
	private String mergeDir;// 删除融合文件目录
	private String anMergeDir;  // 替换融合后文件的保存路径
	private String precision;
	private String sources;
	private String updatetime;
	private String proname;
	private static int counter = 0;
	
	private String outputPath;

	public WashDataMergeForDelete() {
		super();
		WashDataConfig properties = WashDataConfig.getInstance();
		this.meshDir = properties.getProperty("anMesh");
		POI_TNAME = WashDataService.POI_TNAME;
		POI_FILE = properties.getProperty("poiFile");
		
		this.outputPath = properties.getProperty("outputPath");
		this.mergeDir = properties.getProperty("deleteMergeFile");
		this.anMergeDir = properties.getProperty("meshMergeFile");
		
		this.precision = properties.getProperty("PRECISION");
	    this.sources = properties.getProperty("SOURCES");
	    this.updatetime = DateFormat.getStringCurrentDateShort2();
	    this.proname = properties.getProperty("PRONAME");
		
	}

	@Override
	public String toString() {
		// for test
		return "WashDataMergeForNew [anShpFiles=" + anShpFiles + ", meshDir="
				+ meshDir + ", mergeDir=" + mergeDir + ", POI_TNAME="
				+ POI_TNAME + ", outputPath=" + outputPath + "]";
	}

	@Override
	public void merge() {
		
		mergeLog.debug("<----------------------分幅删除融合开始！ ---------------------->");
		boolean isMerge = false;
		String TMP = "tmp", 					// 临时表用来存储导入文件时的shapefile的BOX
	    anDelTable = "TMP_AN_ADD",				//删除融合符合条件的an表
		bdReplaceTable = "TMP_BD_ADD";			//删除融合符合条件的bd表
		findShpFiles(meshDir, anShpFiles);		//导入an mesh
		super.impPOI(POI_FILE, POI_TNAME);		//导入POI表
		mergeLog.info("POI记录数：" + count(POI_TNAME));
		
		impShps(mergeDir + "/AN", anDelTable, false);
		mergeLog.debug("imp an " + mergeDir + "/AN -----》 "+anDelTable+"table");
		mergeLog.debug("an中要删除的记录总数：" + count(anDelTable));
		
		impShps(mergeDir + "/BD", bdReplaceTable, true);
		mergeLog.debug("imp bd " + mergeDir + "/BD -----》 "+bdReplaceTable+"table");
		mergeLog.info("bd中要新增记录总数："+ count(bdReplaceTable));

		final String mergeTmpTable = "TMP_MERGE_NEW";// 生成临时融合表
		try {
			// 提取全部融合数据 isMerge = true 融合
			step1(anDelTable, bdReplaceTable, mergeTmpTable);
			isMerge = step2( bdReplaceTable, mergeTmpTable);
////			isMerge = createMergeTmpTable(anDelTable, bdReplaceTable, mergeTmpTable);
			mergeLog.debug("step2 after ---------------------"+count(mergeTmpTable));
			if (isMerge) {
				// 创建an分幅shap文件BOX空间表 tmp
				H2gisServer.getInstance().execute("Create table " + TMP + "(THE_GEOM POLYGON)");// 暂为空表
			}
			for (File file : anShpFiles) {
				final String anMergeTable = "MERGED_AN_TABLE";	// 导入an分幅的表，最终需要输出
				try {
					String[] ss = file.getAbsolutePath().split("\\\\");
					String mesh = ss[ss.length - 2];
					H2gisServer.getInstance().importData(file.getAbsolutePath(), anMergeTable);
					int count1 = deleteAn(anMergeTable, anDelTable, mesh);// 先删除AN
					int count2 = 0;
					if (isMerge) {
						mergeLog.info("开始进行融合!");
						String sql = "delete from " + TMP;// 清空tmp中记录
						H2gisServer.getInstance().execute(sql);
						String sql2 = "insert into " + TMP + " values(' " + ReadShap.read(file.getAbsolutePath()) + "')";// 插入最新BOX
						H2gisServer.getInstance().execute(sql2);
						count2 = insertBdIntoAn(anMergeTable, mergeTmpTable, TMP, mesh, count1);// 开始融入
					}
					if (count1 + count2 > 0) {
						mergeLog.info("\t最终数量:" + count(anMergeTable));
						String fileoutPath = anMergeDir + "/删除融合/" + ss[ss.length - 4] + "/" + ss[ss.length - 3] + "/" + ss[ss.length - 2] + "/" + ss[ss.length - 1];
						H2gisServer.getInstance().outputShape(fileoutPath, anMergeTable);
					}
				} finally {
					drop(anMergeTable);
				}
			}// end for
			if (isMerge) {
				mergeLog.debug("总数量："+sum);
				if (count(mergeTmpTable) > 0) {
					mergeLog.info("删除AN成功,BD未入完全融合到AN分图幅中!");
				}
			}
		} catch (SQLException e) {
			e.printStackTrace();
		} finally {
			if (isMerge) {
				drop(mergeTmpTable);
				drop(TMP);
			}
		}
		mergeLog.debug("<----------------------删除融合结束！ ---------------------->");
	}
	
	/**
	 * 过滤mergeTable中的GEOM的数据重复记录，留下与主POI关联的记录
	 * 
	 * @param bdTable
	 * @param mergeTable
	 * @throws SQLException
	 */
	private boolean step2(String bdTable, String mergeTable)throws SQLException {
		String sql = "select NAME_CHN,POI_ID,MESHCODE,"
    				+ " poi_type,BD_THE_GEOM,FA_TYPE,AREA_FLAG,guid FROM " + mergeTable
						+ " order by BD_THE_GEOM, NAME_CHN";
		ResultSet rs = H2gisServer.getInstance().executeQuery(sql);
		List<SetResultSet> list2 = new ArrayList<SetResultSet>();
		while (rs.next()) {
			list2.add(new SetResultSet(rs.getString(1),rs.getString(2),
					rs.getString(3),rs.getInt(4),rs.getObject(5),
					rs.getString(6),rs.getString(7),rs.getString(8)));
			
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
		log.debug("000000000000000000000000000000000000:"+count(mergeTable));
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
    				+ " poi_type,BD_THE_GEOM,FA_TYPE,AREA_FLAG,guid) "
    				+ "values('"+list.getNAME_CHN()+"','"+list.getPOI_ID()+"','"
					+list.getMESH()+"','"+list.getPOI_TYPE()+"','"+list.getTHE_GEOM()+"','"
    				+list.getFA_TYPE()+"','"+list.getAREA_FLAG()+ "','"+list.getGUID()+ "')";
    		H2gisServer.getInstance().execute(sql1);
		}
    }
	
	private void step1(String anTable, String bdTable, String mergeTable) throws SQLException {
		String mergeTmpTable = "mergeTmpTable";
		String sql = "create table "+ mergeTmpTable + " as "
				+ "select distinct poi.NAME_CHN,poi.POI_ID,poi.MESHCODE,poi.guid, poi.THE_GEOM,poi.poi_type, "
				+ "an.FA_TYPE,an.AREA_FLAG, "
				+ "bd.THE_GEOM BD_THE_GEOM FROM "
				+ bdTable + " bd ," + POI_TNAME + " poi, "+anTable+" an "
				+ "WHERE ST_Contains(bd.THE_GEOM, poi.THE_GEOM) = true "
				+ "and ST_Intersects(bd.THE_GEOM, an.THE_GEOM) = true " ;
		H2gisServer.getInstance().execute(sql);//+ " left join "
		
		
		
		String sql1 = "select distinct poi.NAME_CHN,poi.POI_ID,poi.MESHCODE,poi.guid, poi.THE_GEOM,poi.poi_type, "
				+ "an.FA_TYPE,an.AREA_FLAG, "
				+ "bd.THE_GEOM BD_THE_GEOM FROM "
				+ bdTable + " bd, " + POI_TNAME + " poi, "+anTable+" an "
				+ "WHERE ST_Contains(bd.THE_GEOM, poi.THE_GEOM) = true "
				+ "and ST_Intersects(bd.THE_GEOM, an.THE_GEOM) = true " ;
		ResultSet rs = H2gisServer.getInstance().executeQuery(sql1);
		Collection<SetResultSet> ree = new ArrayList<SetResultSet>();
		while(rs.next()){
			SetResultSet rss = new SetResultSet();
			log.debug("====================="+ree.size());
			rss.setNAME_CHN(rs.getString(1));
			rss.setPOI_ID(rs.getString(2));
			rss.setMESH(rs.getString(3));
			rss.setGUID(rs.getString(4));
			rss.setPOI_TYPE(rs.getInt(6));
			rss.setFA_TYPE(rs.getString(7));
			rss.setAREA_FLAG(rs.getString(8));
			rss.setTHE_GEOM(rs.getString(9));
			boolean ok  = true;
			for(SetResultSet r:ree ){
				if(r.getNAME_CHN().equals(rs.getString(1))&&r.getTHE_GEOM().equals(rs.getString(9))){
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
						+ " where exists( select 1 from " + POI_TNAME + " poi, "+anTable+" an  WHERE"
						+ " ST_Contains(bd.THE_GEOM, poi.THE_GEOM) = true "
						+ "and ST_Intersects(bd.THE_GEOM, an.THE_GEOM) = true )";
			H2gisServer.getInstance().execute(sql2);
			mergeLog.info(bdTable + "和poi相匹配的bd剩余的记录数：" +count(bdTable));
		}
//		和poi不匹配的bd数据放入融合表
		String sql3 = "INSERT INTO " + mergeTable + "(BD_THE_GEOM,FA_TYPE,AREA_FLAG) "
				+ "SELECT bd.THE_GEOM,an.FA_TYPE,an.AREA_FLAG FROM " 
				+ bdTable +" bd,"+anTable+" an WHERE ST_Intersects(bd.THE_GEOM, an.THE_GEOM) = true";
		H2gisServer.getInstance().execute(sql3);
		mergeLog.info("需要融合的表"+mergeTable + "记录数：" + count(mergeTable));
	}

	/**
	 * 删除高德的数据
	 * @param meshcode TODO
	 * @return TODO
	 */
	private int deleteAn(String anTable, String anDelTable, String meshcode) {
		int count1 = count(anTable);
		mergeLog.info(meshcode + "导入的原始数据数量:" + count1);
		String sql = "delete from " + anTable + " an where exists ( select 1 from " + anDelTable
				+ " m where m.NAME_CHN = an.NAME_CHN )";
		H2gisServer.getInstance().execute(sql);
		int count2 = count(anTable);
		mergeLog.info("删除后的数量为:" + count2+"    删除的记录数："+(count1 - count2) );
		return count1 - count2;
	}

	/**
	 * 把临时表中的数据追加到最终的文件中 规则： 融入表中的数据 <br>
	 * 如有有POI点则找出POI点所在an图幅，将该记录融入<br>
	 * 否则 将记录融入与之相交的an图幅中
	 * 
	 * @param anTable
	 * @param mergeTable
	 * @param meshcode TODO
	 * @param countdelete TODO
	 * @param tmp
	 * anMergeTable, mergeTmpTable, TMP, mesh, count1
	 */
	private int insertBdIntoAn(String anTable, String mergeTable, String TMP, String meshcode, int countdelete) {
		int count1 = step3(anTable, mergeTable, TMP, "THE_GEOM",meshcode);
		mergeLog.info("融合后与原始数据数量差值:" + count1);
		int count2 = step3(anTable, mergeTable, TMP, "BD_THE_GEOM",meshcode);
		mergeLog.info("\t需要融合的数据数量:" + (count1 + count2));
		
		return count1 + count2;
	}
	int sum = 0;
	private int step3(String anTable, String mergeTable, String TMP, String geom_string, String meshcode) {
		mergeLog.debug("删除融合的MergeTable的数量："+count(mergeTable));
		int count1 = count(anTable);
		String sql1 = " select m.NAME_CHN, m.POI_ID, m.BD_THE_GEOM, m.MESHCODE,"
				+ "m.FA_TYPE,m.AREA_FLAG,m.guid "
				+ "from " + mergeTable + " m , " + TMP
				+ " T WHERE ST_Intersects(m."+geom_string+", T.THE_GEOM)=true";
		ResultSet rs = H2gisServer.getInstance().executeQuery(sql1);
		List<SetResultSet> list2 = new ArrayList<>();
		try {
			while(rs.next()){
				mergeLog.debug("NAME_CHN======"+rs.getString(1));
				mergeLog.debug("POI_ID======"+rs.getString(2));
				mergeLog.debug("BD_THE_GEOM======"+rs.getObject(3));
				mergeLog.debug("MESHCODE======"+rs.getString(4));
				mergeLog.debug("FA_TYPE======"+rs.getString(5));
				mergeLog.debug("AREA_FLAG======"+rs.getString(6));
				mergeLog.debug("guid======"+rs.getString(7));
				String NAME_CHN = rs.getString(1) == null ? "":rs.getString(1);
				String POI_ID = rs.getString(2)== null ? "0":rs.getString(2);
				String BD_THE_GEOM = rs.getString(3)== null ? "":rs.getString(3);
				String MESHCODE = rs.getString(4)== null ? "":rs.getString(4);
				String FA_TYPE = rs.getString(5)== null ? "":rs.getString(5);
				String AREA_FLAG = rs.getString(6)== null ? "0":rs.getString(6);
				String guid = rs.getString(7)== null ? "":rs.getString(7);
				SetResultSet sts = new SetResultSet();
				sts.setNAME_CHN(NAME_CHN);
				sts.setPOI_ID(POI_ID);
				sts.setTHE_GEOM(BD_THE_GEOM);
				sts.setMESH(MESHCODE);
				sts.setFA_TYPE(FA_TYPE);
				sts.setAREA_FLAG(AREA_FLAG);
				sts.setGUID(guid);
				list2.add(sts);
			}
			sum = list2.size()+sum;
			for(SetResultSet list:list2){
				mergeLog.debug("============================================");
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
		
		int count2 = count(anTable);
		mergeLog.debug(meshcode+"融合后的分幅数量："+count2);
		// 删除已融入的记录
		mergeLog.debug("插入该图幅的总数量为："+list2.size());
		if(list2.size()>0){
			String sql3 = "delete from " + mergeTable + " m where exists ("
					+ " select  1 from " + TMP
					+ " T WHERE ST_Intersects(m."+geom_string+", T.THE_GEOM)=true)";
			H2gisServer.getInstance().execute(sql3);
		}
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

	// 字符串遍历
	private static int stringNumbers(String str, String f) {
		int k = str.indexOf(f);
		System.out.println("--------------------------------------------"+k);
		if (k == -1) {
			return counter;
		} else if (k != -1) {
			counter++;
			stringNumbers(str.substring(k + f.length()), f);
			return counter;
		}
		return counter;
	}

	/**
	 * 找出全部的BD数据与POI相交的数据集插入到TMP表中<br>
	 * 
	 * @param anTable
	 * @param bdTable
	 * @param mergeTable
	 */
}
