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
 * step 7 对@reference 第6步 的导出结果(未排除)进行处理,过滤AN压盖多个BD数据
 * 
 * 1.输入：第6步未排除的BD数据、AN数据拼图; 2.AN与多个BD压盖，且AN与BD压盖面积占AN总面积的85%（可调整）的数量在2个（可调整）或以上
 * 3.输出:满足条件2的BD数据、不满足条件2的BD数据,满足条件2的AN数据
 *
 */
public class WashDataAnCoverMultiBd extends WashDataService {

	private String outFilePath; // 输出路径
	private double areaCoverPercent; // 压盖百分比
	private WashedData preWashedData; // 前一步的清洗结果
	private int coverNum; // 压盖个数
	private String deleteMergeFile; // 输出符合条件的AN数据路径
	private String peopleSure;        //人工确认路径
	public WashDataAnCoverMultiBd(WashedData washedData) {
		super();

		WashDataConfig properties = WashDataConfig.getInstance();
		this.outFilePath = properties.getProperty("outputPath");
		this.areaCoverPercent = Double.parseDouble(properties.getProperty("anCoverMultiBdPercent"));
		this.preWashedData = washedData;
		this.coverNum = Integer.parseInt(properties.getProperty("anCoverMultiBdNum"));
		this.deleteMergeFile = properties.getProperty("deleteMergeFile");
		this.peopleSure = properties.getProperty("peopleSure");
	}

	/*
	 * (non-Javadoc)
	 * 
	 * @see com.autonavi.mapart.service.WashDataService#wash()
	 */
	@Override
	public WashedData wash() {
		final String STEP_NAME = "多个BD压盖一个AN";
		WashDataService.WashedData washedData = new WashDataService.WashedData(outFilePath + "/7/g/" + STEP_NAME
				+ "_garbage.shp", outFilePath + "/7/r/" + STEP_NAME + ".shp", "MULTI_BD_AN_COVER",
				preWashedData.getResultTableName());

		try {
			createEmptyTable(BD_TNAME, washedData.getGarbageTableName());

			// 过滤AN压盖至少2个BD
			filterMultiCover(areaCoverPercent, coverNum, preWashedData.getResultTableName(),
					washedData.getGarbageTableName());
			// 过滤结果
			filterResult(washedData.getResultTableName(), washedData.getGarbageTableName());

			// 导出shape
			H2gisServer.getInstance().outputShape(washedData.getGarbageFile(), washedData.getGarbageTableName());
			H2gisServer.getInstance().outputShape(washedData.getResultFile(), washedData.getResultTableName());

		} catch (SQLException e) {
			e.printStackTrace();
		}

		return washedData;
	}

	private void filterResult(String resultTableName, String garbageTableName) throws SQLException {
		String sql = "delete from " + resultTableName + " bd where bd.THE_GEOM in (select THE_GEOM from "
				+ garbageTableName + ")";
		H2gisServer.getInstance().execute(sql);
		log.debug("删除多个BD压盖一个AN的BD");
		count(resultTableName);
	}

	/**
	 * @param areaCoverPercent
	 * @param coverNum
	 * @param resultTableName
	 * @param garbageTableName
	 * @throws SQLException
	 */
	private void filterMultiCover(double areaCoverPercent, int coverNum, String resultTableName, String garbageTableName)
			throws SQLException {

		String sql = "select an.THE_GEOM,bd.THE_GEOM, ST_Area(ST_Intersection(an.THE_GEOM, bd.THE_GEOM)) intersect_area, "
				+ "ST_Area(an.THE_GEOM) an_area from "
				+ AUTONAVI_TNAME
				+ " an,"
				+ resultTableName
				+ " bd "
				+ " where ST_Intersects(an.THE_GEOM,bd.THE_GEOM) = true and  ST_Area(ST_Intersection(an.THE_GEOM, bd.THE_GEOM))/ST_Area(bd.THE_GEOM) >"
				+ areaCoverPercent +" order by an.THE_GEOM";
		ResultSet rs = H2gisServer.getInstance().executeQuery(sql);
		Map<Polygon, Collection<Polygon>> an2bds = new HashMap<Polygon, Collection<Polygon>>();
		while (rs.next()) {
			Polygon anPolygon = new Polygon(rs.getString(1),rs.getDouble(4));

			Collection<Polygon> tmpBDs = an2bds.remove(anPolygon);
			if (tmpBDs == null) {
				tmpBDs = new ArrayList<Polygon>();
			}
			Polygon bdPolygon = new Polygon(rs.getString(2),rs.getDouble(3));
			tmpBDs.add(bdPolygon);
			an2bds.put(anPolygon, tmpBDs);
		}

		Collection<String> anGeomData = new HashSet<String>();
		Collection<String> bdGeomData = new HashSet<String>();
		for (Polygon an : an2bds.keySet()) {
			Collection<Polygon> bds = an2bds.get(an);
			boolean coveredNumOver2 = isCoveredNumOver2(bds);
			if ( ! coveredNumOver2) {
				continue;
			}
			log.debug("======================================>2");
			boolean areaPercentOk = isAreaPercentOk(an.getArea(), getAreaList(bds));
			if (!areaPercentOk) {
				continue;
			}
			anGeomData.add(an.getPolygon());
			bdGeomData.addAll(bds.stream().map(bd -> bd.getPolygon()).collect(Collectors.toSet()));
		}
		output(createBdTmpTable(bdGeomData, BD_TNAME, garbageTableName), deleteMergeFile + "/BD/多个BD压盖一个AN_7_"
				+ count(garbageTableName) + ".shp");
		//输出到人工确认
		output(garbageTableName, peopleSure + "/BD/多个BD压盖一个AN_7_"
				+ count(garbageTableName) + ".shp");
		String tmpAnTable = "TMP_AN_7";
		createEmptyTable(AUTONAVI_TNAME, tmpAnTable);
		createAnTmpTable(anGeomData, AUTONAVI_TNAME, tmpAnTable);
		count(tmpAnTable);
		String finalAnTable = "FNL_AN_7";
		output(selectAllAnDataByGeom(tmpAnTable, finalAnTable), deleteMergeFile + "/AN/多个BD压盖一个AN_7_"
				+ count(finalAnTable) + ".shp");
		
		//输出到人工确认
		output(finalAnTable, peopleSure + "/AN/多个BD压盖一个AN_7_"
				+ count(finalAnTable) + ".shp");
		log.debug(coverNum + "个BD压盖一个AN，压盖面积大于" + areaCoverPercent);
	}

	private Collection<Double> getAreaList(Collection<Polygon> bds) {
		return bds.stream().map(bd -> bd.getArea()).collect(Collectors.toList());
	}

}
