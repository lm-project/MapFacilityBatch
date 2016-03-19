package com.autonavi.mapart.service.impl;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.stream.Collectors;

import com.autonavi.mapart.service.H2gisServer;
import com.autonavi.mapart.service.WashDataConfig;
import com.autonavi.mapart.service.WashDataService;

/**
 * 对@reference C 的导出结果(未排除)进行处理,过滤BD压盖多个AN数据
 * 
 * 1.输入：C未排除的BD数据、AN数据拼图;
 * 2.BD与多个AN压盖，且AN与BD压盖面积占AN总面积的80%（可调整）的数量在2个（可调整）或以上
 * 3.输出:满足条件2的BD数据、不满足条件2的BD数据、满足条件2的AN数据
 * @author huandi.yang
 *
 */
public class WashDataBdCoverMultiAn extends WashDataService {
	
	private String outFilePath;       //输出路径
	private double areaCoverPercent;  //压盖百分比
	private WashedData preWashedData; //前一步的清洗结果
	private int coverNum;             //压盖个数
	private String peopleSure;        //人工确认路径
	private String bdCoverMutilAnFile;//bd压盖多个AN输出路径
	public WashDataBdCoverMultiAn(WashedData washedData){
		super();
		WashDataConfig properties = WashDataConfig.getInstance();
		
		this.outFilePath = properties.getProperty("outputPath");
		this.peopleSure = properties.getProperty("peopleSure");
		this.areaCoverPercent = Double.parseDouble(properties.getProperty("bdCoverMultiAnPercent"));
		this.coverNum = Integer.parseInt(properties.getProperty("bdCoverMultiAnNum"));
		this.preWashedData = washedData;
		this.bdCoverMutilAnFile = properties.getProperty("bdCoverMultiAnFile");
	}

	/* (non-Javadoc)
	 * @see com.autonavi.mapart.service.WashDataService#wash()
	 */
	@Override
	public WashedData wash() {
		final String STEP_NAME = "BD压盖多个AN";
		WashDataService.WashedData washedData = new WashDataService.WashedData(outFilePath + "/5/g/" + STEP_NAME
				+ "_garbage.shp", outFilePath + "/5/r/" + STEP_NAME + ".shp", "BD_COVER_MULTI_AN", preWashedData.getResultTableName());
		try {
			log.info("table name :"+washedData.getGarbageTableName());
			createEmptyTable(BD_TNAME, washedData.getGarbageTableName());
			//过滤BD压盖至少2个AN
			filterMultiCover(areaCoverPercent, coverNum, preWashedData.getResultTableName(), washedData.getGarbageTableName());
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
		log.debug("非BD压盖多个AN");

		count(resultTableName);
	}

	/**
	 * @param areaCoverPercent
	 * @param coverNum
	 * @param resultTableName
	 * @param garbageTableName
	 * @throws SQLException 
	 */
	private void filterMultiCover(double areaCoverPercent, int coverNum,
			String resultTableName, String garbageTableName) throws SQLException {
		
		String	sql = "select bd.THE_GEOM, an.THE_GEOM, ST_Area(ST_Intersection(an.THE_GEOM, bd.THE_GEOM)) intersect_area, "
				+ "ST_Area(bd.THE_GEOM) bd_area from "+ AUTONAVI_TNAME +" an,"+resultTableName + " bd " +
        " where ST_Intersects(an.THE_GEOM,bd.THE_GEOM) = true and ST_Area(ST_Intersection(an.THE_GEOM, bd.THE_GEOM))/ST_Area(an.THE_GEOM) >" + areaCoverPercent;
		ResultSet rs = H2gisServer.getInstance().executeQuery(sql);
		Map<Polygon, Collection<Polygon>> bd2ans = new HashMap<Polygon, Collection<Polygon>>();
		while(rs.next()) {	
			Polygon anPolygon = new Polygon(rs.getString(2),rs.getDouble(3));//Value：an geom和相交面积
			Polygon bdPolygon = new Polygon(rs.getString(1),rs.getDouble(4));//Key：bd geom和面积
			Collection<Polygon> tmpANs = bd2ans.remove(bdPolygon);
			if(tmpANs == null ) {
				tmpANs = new ArrayList<Polygon>();
			}
			tmpANs.add(anPolygon);
			bd2ans.put(bdPolygon, tmpANs);
		}
		Collection<String> anGeomData = new HashSet<String>();
		Collection<String> bdGeomData = new HashSet<String>();
		for(Polygon bd : bd2ans.keySet()){
			Collection<Polygon> ans = bd2ans.get(bd);
			if (!isCoveredNumOver2(ans)) {
				continue;
			}

			if (!isAreaPercentOk(bd.getArea(), getAreaList(ans))) {
				continue;
			}
			bdGeomData.add(bd.getPolygon());
			anGeomData.addAll(ans.stream().map(an -> an.getPolygon()).collect(Collectors.toSet()));
		}
		log.debug(coverNum+"个AN压盖一个BD，压盖面积大于" +areaCoverPercent);
		
		output(createBdTmpTable(bdGeomData,BD_TNAME,garbageTableName),  peopleSure
				+ "/BD/BD压盖多个AN_5_"+count(garbageTableName)+".shp");
		String tmpAnTable = "TMP_AN_5";
		createEmptyTable(AUTONAVI_TNAME, tmpAnTable);
//		output(createAnTmpTable(anGeomData, AUTONAVI_TNAME, tmpAnTable), bdCoverMutilAnFile + "/AN/BD压盖多个AN_5_"
//				+ count(tmpAnTable) + ".shp");
		//AN输出到人工确认
		output(createAnTmpTable(anGeomData, AUTONAVI_TNAME, tmpAnTable), peopleSure + "/AN/BD压盖多个AN_5_"
				+ count(tmpAnTable) + ".shp");
	}

	private Collection<Double> getAreaList(Collection<Polygon> ans) {
		return ans.stream().map( an -> an.getArea()).collect(Collectors.toList());
	}

}
