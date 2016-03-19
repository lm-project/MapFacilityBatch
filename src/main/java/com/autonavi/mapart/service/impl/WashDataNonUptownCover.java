package com.autonavi.mapart.service.impl;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;

import com.autonavi.mapart.service.H2gisServer;
import com.autonavi.mapart.service.WashDataConfig;
import com.autonavi.mapart.service.WashDataService;

/**
 * 对@reference WashDataHighMatch的导出结果(未排除)进行处理,过滤BD压盖AN非小区部分数据
 * 
 * 1.输入：WashDataHighMatch未排除的BD数据、AN数据拼图;
 * 2.以AN数据为准，对于BD与AN压盖，且BD压盖AN面积占BD总面积80%（可调整）以上进行筛选;
 * 3.fa_type != 3110 (非小区)
 * 4.输出:满足条件2&3的BD数据、不满足条件2&3的BD数据
 * 
 * @author huandi.yang
 * 
 */
public class WashDataNonUptownCover extends WashDataService {
	
	private String outFilePath;
	private double areaCoverPercent;
	private WashedData preWashedData;
	private String peopleSure;
	public WashDataNonUptownCover(WashedData washedData){
		super();
		WashDataConfig properties = WashDataConfig.getInstance();
		this.outFilePath = properties.getProperty("outputPath");
		this.peopleSure = properties.getProperty("peopleSure");
		this.areaCoverPercent = Double.parseDouble(properties.getProperty("bdCoverNonUptownPercent"));
		this.preWashedData = washedData;
		
	}
	
	// for test
	public WashDataNonUptownCover(String outFilePath, double areaCoverPercent,
			WashedData preWashedData) {
		super();
		this.outFilePath = outFilePath;
		this.areaCoverPercent = areaCoverPercent;
		this.preWashedData = preWashedData;
	}




	@Override
	public WashedData wash() {
		final String STEP_NAME = "低匹配度_非小区压盖";
		WashDataService.WashedData washedData = new WashDataService.WashedData(outFilePath + "/2/g/" + STEP_NAME
				+ "_garbage.shp", outFilePath + "/2/r/" + STEP_NAME + ".shp", "BD_NONUPTOWN_NOMERGE", preWashedData.getResultTableName());
		log.debug("preWashedData.resultTableName:"+preWashedData.getResultTableName());
		
		try {
			//判断压盖
			filterNonUptownCover(areaCoverPercent, preWashedData.getResultTableName(), washedData.getGarbageTableName());
			
			//过滤结果
			filterResult(washedData.getResultTableName(), washedData.getGarbageTableName());
			
			H2gisServer.getInstance().outputShape(washedData.getGarbageFile(), washedData.getGarbageTableName());
			H2gisServer.getInstance().outputShape(washedData.getResultFile(), washedData.getResultTableName());
			
			
		} catch (SQLException e) {
			e.printStackTrace();
		}
		
		return washedData;
	}

	private void filterResult(String resultTableName, String garbageTableName) throws SQLException {
		String sql = "delete from " + resultTableName + " bd where bd.THE_GEOM in (select THE_GEOM from " + garbageTableName + ")";
		H2gisServer.getInstance().execute(sql);
		log.debug("非小区压盖小于"+areaCoverPercent);
		count(resultTableName);

	}
	
	
	/**
	 * 过滤BD压盖AN面积超过80%,Fa_type != 3110的BD数据
	 * @param areaCoverPercent
	 * @param garbageTableName
	 * @param resultTableName 
	 * @throws SQLException 
	 */
	private void filterNonUptownCover(double areaCoverPercent, String resultTableName, String garbageTableName) throws SQLException {
		Collection<String> bdGeomData = new ArrayList<String>();
		Collection<String> anGeomData = new ArrayList<String>();
//		String sql = "create table " + garbageTableName + " as select distinct bd.THE_GEOM from "
//						+ AUTONAVI_TNAME + " an," + resultTableName +" bd  WHERE  ST_Intersects(an.THE_GEOM,BD.THE_GEOM) = true "
//						+ "and ST_Area(ST_Intersection(an.THE_GEOM, bd.THE_GEOM))/ST_Area(bd.THE_GEOM) >= " + areaCoverPercent 
//						+ " and an.FA_TYPE !=3110";
//		H2gisServer.getInstance().execute(sql);
		createEmptyTable(resultTableName, garbageTableName);
		createEmptyTable(AUTONAVI_TNAME, "AN_TMP_2");
		String sql = "select distinct bd.THE_GEOM, an.THE_GEOM from "
				+ AUTONAVI_TNAME + " an," + resultTableName +" bd  WHERE  ST_Intersects(an.THE_GEOM,BD.THE_GEOM) = true "
				+ "and ST_Area(ST_Intersection(an.THE_GEOM, bd.THE_GEOM))/ST_Area(bd.THE_GEOM) >= " + areaCoverPercent 
				+ " and an.FA_TYPE !=3110";
		ResultSet rs = H2gisServer.getInstance().executeQuery(sql);
		while(rs.next()){
			bdGeomData.add(rs.getString(1));
			anGeomData.add(rs.getString(2));
		}
		createBdTmpTable(bdGeomData, resultTableName, garbageTableName);
		H2gisServer.getInstance().outputShape(peopleSure+"/BD/低匹配度_非小区压盖_2_"+count(garbageTableName)+".shp", garbageTableName);
		createAnTmpTable(anGeomData, AUTONAVI_TNAME, "AN_TMP_2");
		H2gisServer.getInstance().outputShape(peopleSure+"/AN/低匹配度_非小区压盖_2_"+count("AN_TMP_2")+".shp", "AN_TMP_2");

		log.debug("非小区压盖大于"+areaCoverPercent);
	}
	//保存符合条件的AN、BDshuju

}
