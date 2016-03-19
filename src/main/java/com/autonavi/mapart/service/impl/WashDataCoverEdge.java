package com.autonavi.mapart.service.impl;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import com.autonavi.mapart.service.H2gisServer;
import com.autonavi.mapart.service.WashDataConfig;
import com.autonavi.mapart.service.WashDataService;

/**
 * step 8
 * 对@reference 第7步 的导出结果(未排除)进行处理,过滤BD与AN压边的bd数据
 * 1.输入：第7步未排除的BD数据、AN数据拼图;
 * 2.AN与BD压盖,压盖面积占BD面积的20%（可调整）以下；
 * 3.输出:满足条件2的BD数据(存入“未压盖”文件夹)、不满足条件2的BD数据
 * @author huandi.yang
 *
 */
public class WashDataCoverEdge extends WashDataService {
	
	private String outFilePath;       //输出路径
	private double areaCoverPercent;  //压盖百分比
	private WashedData preWashedData; //前一步的清洗结果
	private String nonCoverMergeFile; //未压盖路径
	public WashDataCoverEdge(WashedData washedData){
		super();
		WashDataConfig properties = WashDataConfig.getInstance();
		this.outFilePath = properties.getProperty("outputPath");
		this.areaCoverPercent = Double.parseDouble(properties.getProperty("bdCoverAnEdgePercent"));
		this.preWashedData = washedData;
		this.nonCoverMergeFile = properties.getProperty("nonCoverMergeFile");
	}

	@Override
	public WashedData wash() {
		final String STEP_NAME = "BD压盖AN边界";
		WashDataService.WashedData washedData = new WashDataService.WashedData(outFilePath + "/8/g/" + STEP_NAME
				+ "_garbage.shp", outFilePath + "/8/r/" + STEP_NAME + ".shp", "BD_COVER_AN_EDGE", preWashedData.getResultTableName());
		
		try {
			//过滤BD压盖AN边界
			filterCoverEdge(areaCoverPercent, preWashedData.getResultTableName(), washedData.getGarbageTableName());
			
			//过滤结果
			filterResult(washedData.getResultTableName(), washedData.getGarbageTableName());
			
			//导出shape
			H2gisServer.getInstance().outputShape(washedData.getGarbageFile(), washedData.getGarbageTableName());
			H2gisServer.getInstance().outputShape(washedData.getResultFile(), washedData.getResultTableName());
			H2gisServer.getInstance().outputShape(nonCoverMergeFile+"/BD压盖AN边界_8_"+count(washedData.getGarbageTableName())+".shp", washedData.getGarbageTableName());
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return washedData;
	}

	private void filterResult(String resultTableName, String garbageTableName) throws SQLException {
		String sql = "delete from " + resultTableName + " bd where bd.THE_GEOM in (select THE_GEOM from " + garbageTableName + ")";
		H2gisServer.getInstance().execute(sql);
		log.debug("不满足BD压盖AN边界");
		count(resultTableName);
		
	}

	/**
	 * 过滤BD压盖AN边界
	 * @param areaCoverPercent2
	 * @param resultTableName
	 * @param garbageTableName
	 * @throws SQLException 
	 */
	private void filterCoverEdge(double areaCoverPercent,
			String resultTableName, String garbageTableName) throws SQLException {
		String sql = "create table BD_CoverEdge_tmp as select bd.THE_GEOM from "
				+ AUTONAVI_TNAME
				+ " an,"
				+ resultTableName
				+ " bd "
				+ " where ST_Intersects(an.THE_GEOM,bd.THE_GEOM) = true and "
				+ "ST_Area(ST_Intersection(an.THE_GEOM, bd.THE_GEOM))/ST_Area(an.THE_GEOM) <="
				+ areaCoverPercent;
		H2gisServer.getInstance().execute(sql);
		count("BD_CoverEdge_tmp");
		String sql2 = "select bd.THE_GEOM, an.THE_GEOM, ST_Area(ST_Intersection(an.THE_GEOM, bd.THE_GEOM)) intersect_area, "
				+ "ST_Area(bd.THE_GEOM) bd_area from "
				+ AUTONAVI_TNAME
				+ " an, BD_CoverEdge_tmp bd "
				+ " where ST_Intersects(an.THE_GEOM,bd.THE_GEOM) = true";
		ResultSet rs = H2gisServer.getInstance().executeQuery(sql2);
		Map<Map<String, Double>, Map<String, Double>> bdCoverEdges = new HashMap<Map<String, Double>, Map<String, Double>>();
		while(rs.next()) {
			//Key：bd geom和面积
			Map<String, Double> bd = new HashMap<String, Double>();
			String bdGeom = rs.getObject(1).toString(); //bd geom
			double bd_area = Double.parseDouble(rs.getObject(4).toString());//bd 面积
			bd.put(bdGeom, bd_area);
			//Value：an geom和相交面积
			String anGeom = rs.getObject(2).toString();//an geom
			double intersect_area = Double.parseDouble(rs.getObject(3).toString());//an 相交面积
			Map<String, Double> ans = bdCoverEdges.remove(bd);
			if ( ans == null ) {
				ans = new HashMap<String, Double>();
			}
			ans.put(anGeom, intersect_area);
			bdCoverEdges.put(bd, ans);
		}
		H2gisServer.getInstance().execute("create table " + garbageTableName + " as select THE_GEOM from " + resultTableName + " where 1=2 ");
		for(Map<String, Double> bd : bdCoverEdges.keySet()){
			double bd_area = (double) bd.values().toArray()[0];
			String bd_geom = (String) bd.keySet().toArray()[0];
			Map<String, Double> ans = bdCoverEdges.get(bd);
			if(isCoverPercent(bd_area,ans,areaCoverPercent)){
				String sql3 = "insert into " + garbageTableName + "(THE_GEOM) values(ST_GeomFromText('" +bd_geom + "'))";
				H2gisServer.getInstance().execute(sql3);
				log.debug("插入一个BD压盖多个ANSQL"+sql3);
			}
		}
		log.debug("BD压盖AN边界，压盖面积<="+areaCoverPercent);
		count(garbageTableName);
	}

	/**
	 * 判断多个AN与BD压盖，如果每个压盖边界都占BD面积的5%以内，返回true，否则false
	 * @param bd_area
	 * @param ans
	 * @param areaCoverPercent
	 * @return
	 */
	private boolean isCoverPercent(double bd_area, Map<String, Double> ans, double areaCoverPercent) {
		for(double intsec_area : ans.values()){
			if(intsec_area / bd_area >= areaCoverPercent){
				return false;
			}
		}
		return true;
	}
	
}
