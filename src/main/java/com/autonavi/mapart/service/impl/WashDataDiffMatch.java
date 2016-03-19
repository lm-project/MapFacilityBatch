package com.autonavi.mapart.service.impl;

import java.sql.SQLException;

import com.autonavi.mapart.service.H2gisServer;
import com.autonavi.mapart.service.WashDataConfig;
import com.autonavi.mapart.service.WashDataService;

/**
 * step 9
 * 对@reference 第6步 的导出结果(未排除)进行处理,过滤BD与AN面积差值在50%以上的数据
 * 1.输入：第6步未排除的BD数据、AN数据拼图;
 * 2.AN与BD压盖,面积差值在50%（可调整）以上;
 * 3.输出:满足条件2的BD数据、不满足条件2的BD数据
 * @author huandi.yang
 *
 */
public class WashDataDiffMatch extends WashDataService {
	
	private String outFilePath;
	private double maxDiffCoverPercent; //第六步参数
	private WashedData preWashedData;
	
	public WashDataDiffMatch(WashedData washedData){
		super();
		WashDataConfig properties = WashDataConfig.getInstance();
		this.outFilePath = properties.getProperty("outputPath");
		this.maxDiffCoverPercent = Double.parseDouble(properties.getProperty("maxDiffCoverPercent"));
		this.preWashedData = washedData;
	}
	

	@Override
	public WashedData wash() {
		final String STEP_NAME = "压盖面积差值大";
		WashDataService.WashedData washedData = new WashDataService.WashedData(outFilePath + "/9/g/" + STEP_NAME
				+ "_garbage.shp", outFilePath + "/9/r/" + STEP_NAME + ".shp", "BD_DIFFCOVER_NOMERGE", preWashedData.getResultTableName());
		
		try {
			//过滤满足条件的BD数据
			filterDiffCover(maxDiffCoverPercent, preWashedData.getResultTableName(), washedData.getGarbageTableName());
			//过滤结果
			filterResult(washedData.getResultTableName(), washedData.getGarbageTableName());
			
			//导出shape
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
		log.debug("AN与BD压盖,面积差值不满足>=50%");
		count(resultTableName);
	}


	/**
	 * 过滤AN与BD压盖,面积差值在50%以上的BD数据
	 * @param maxDiffCoverPercent2
	 * @param resultTableName
	 * @param garbageTableName
	 * @throws SQLException 
	 */
	private void filterDiffCover(double maxDiffCoverPercent,
			String resultTableName, String garbageTableName) throws SQLException {
		double tmpCoverPercent = 1 - maxDiffCoverPercent;
		String sql = "create table "
				+ garbageTableName
				+ " as SELECT distinct bd.THE_GEOM FROM "
				+ AUTONAVI_TNAME
				+ " an,"
				+ resultTableName
				+ " bd  WHERE ST_Intersects(an.THE_GEOM,BD.THE_GEOM) = true "
				+ " and ( "
				+ " (ST_Area(AN.THE_GEOM) >=ST_Area(BD.THE_GEOM) and ST_Area(BD.THE_GEOM)/ST_Area(AN.THE_GEOM)<= "
				+ tmpCoverPercent
				+ ") or (ST_Area(AN.THE_GEOM) <= ST_Area(BD.THE_GEOM) and ST_Area(AN.THE_GEOM)/ST_Area(BD.THE_GEOM) <= "
				+ tmpCoverPercent + "))";
		H2gisServer.getInstance().execute(sql);
		log.debug("AN与BD压盖,面积差值>="+maxDiffCoverPercent);
		count(garbageTableName);
		
	}

}
