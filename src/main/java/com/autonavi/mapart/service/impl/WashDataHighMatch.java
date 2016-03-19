package com.autonavi.mapart.service.impl;

import java.sql.SQLException;

import com.autonavi.mapart.service.H2gisServer;
import com.autonavi.mapart.service.WashDataConfig;
import com.autonavi.mapart.service.WashDataService;

/**
 * step 1
 * 1.输入：AN设施区域拼图、BD数据拼图； <br\>
 * 2.以AN数据为准，对压盖面积大于等于85%（可调整）以上的两个形状进行对比； <br\>
 * 3.对面积差值在20%（可调整）内的BD数据进行排除； <br\>
 * a)AN>BD 0.8 <= BD/AN <br\>
 * b) BD>AN 1.25 > BD/AN <br\>
 * 4.输出：BD排除部分、BD未排除部分。 <br\>
 * 
 */
public class WashDataHighMatch extends WashDataService {

	private String filePath;
	private String areaCoverPercent;
	private String faType;
	private double diffCoverPercent;
	private final String GARBAGE_TNAME = "BD_GARBAGE";
	private final String RESULT_TNAME = BD_TNAME + "_BAK";
	private WashedData preWashedData;
	private String peopleSure;
	public WashDataHighMatch(WashedData washedData) {
		super();
		WashDataConfig properties = WashDataConfig.getInstance();

		this.filePath = properties.getProperty("outputPath");
		this.areaCoverPercent = properties.getProperty("areaCoverPercent");//areaCoverPercent=0.8
		this.diffCoverPercent = Double.parseDouble(properties.getProperty("diffCoverPercent"));//diffCoverPercent=0.3
		this.faType = properties.getProperty("FA_TYPE");
		this.preWashedData = washedData;
		this.peopleSure = properties.getProperty("peopleSure");
	}
	
	// for test
	public WashDataHighMatch(String filePath, String areaCoverPercent,
			String faType, double diffCoverPercent) {
		super();
		this.filePath = filePath;
		this.areaCoverPercent = areaCoverPercent;
		this.faType = faType;
		this.diffCoverPercent = diffCoverPercent;
	}
	

	/**
	 * 
	 * @param areaCoverPercent
	 *            // 压盖面积参数可调节
	 * @param diffCoverPercent
	 *            差额参数可调节
	 * @param filePath
	 */
	@Override
	public WashedData wash() {
		String step = "形状匹配高";
		WashDataService.WashedData washedData = new WashDataService.WashedData(filePath + "/1/g/"+step+".shp", filePath+ "/1/r/形状匹配低.shp", GARBAGE_TNAME, RESULT_TNAME);
		try {
			log.debug("begin process step 1");
			/**
			 * 输出不需要融合的BD数据
			 */
			step1(areaCoverPercent, diffCoverPercent);
			/**
			 * BD压盖多个AMap的数据
			 */
			step2();

			/**
			 * 面积差小于20%
			 */
			step3(areaCoverPercent);

			/**
			 * 满足压盖>80%、面积差< 20% 的数据
			 */
			H2gisServer.getInstance().outputShape(washedData.getGarbageFile(), washedData.getGarbageTableName());
			/**
			 * 不满足压盖>80%、面积差< 20% 的数据
			 */
			step4(areaCoverPercent);
			H2gisServer.getInstance().outputShape(washedData.getResultFile(), washedData.getResultTableName());

		} catch (Exception e) {
			e.printStackTrace();
		}
		return washedData;
	}

	/**
	 * 1.以AN数据为准，压盖面积大于等于85%以上； <br\>
	 * 2.面积差值在20%内的BD数据
	 */
	private void step1(String areaCoverPercent, double diffCoverPercent) throws SQLException {
		double tmpCoverPercent = 1 - diffCoverPercent;
		H2gisServer
				.getInstance()
				.execute(
						"create table BD_GARBAGE as SELECT distinct bd.THE_GEOM  FROM "
								+ AUTONAVI_TNAME + " an,"
								+ preWashedData.getResultTableName() + " bd "
								+ "WHERE an.FA_TYPE='" + faType + "' "
								+ "and ST_Intersects(an.THE_GEOM,BD.THE_GEOM) = true "
								+ "and ST_Area(ST_Intersection(an.THE_GEOM, bd.THE_GEOM))"
									+ "/ST_Area(an.THE_GEOM) >= " + areaCoverPercent
								+ " and ( (ST_Area(AN.THE_GEOM) >=ST_Area(BD.THE_GEOM) "
										+ "and ST_Area(BD.THE_GEOM)/ST_Area(AN.THE_GEOM)>= "+ tmpCoverPercent+ ")"
									+ " or (ST_Area(AN.THE_GEOM) <= ST_Area(BD.THE_GEOM) "
										+ "and ST_Area(AN.THE_GEOM)/ST_Area(BD.THE_GEOM) >= " + tmpCoverPercent + "))");
	}

	/**
	 * 把BD覆盖多个Amap的数据过滤出来
	 * 
	 * @throws SQLException
	 */
	private void step2() throws SQLException {
		H2gisServer
				.getInstance()
				.execute(
						"create table BD_GARBAGE_1 as select THE_GEOM FROM "
								+ "("
								+ "SELECT bd.THE_GEOM"
								+ ",(select count(1) from "
								+ AUTONAVI_TNAME
								+ " an where ST_Intersects(an.THE_GEOM,bd.THE_GEOM) = true"
								+ ") num "
								+ "FROM BD_GARBAGE bd ) WHERE NUM > 1");
	}

	private void step3(String areaCoverPercent) throws SQLException {
		H2gisServer.getInstance().execute(
				"delete from " + GARBAGE_TNAME + " b where " + " exists ( select 1 from BD_GARBAGE_1 b1"
						+ "	 where ST_Intersects(b.THE_GEOM,b1.THE_GEOM) = true "
						+ "and ST_Area(ST_Intersection(b.THE_GEOM, b1.THE_GEOM))/ST_Area(b.THE_GEOM) >= "
						+ areaCoverPercent + ")");
		log.debug("满足条件压盖>80%和面积差<20%的数据");
		count(GARBAGE_TNAME);
	}

	private void step4(String areaCoverPercent) throws SQLException {
		H2gisServer.getInstance().execute("create table " + RESULT_TNAME + " as select * from " + preWashedData.getResultTableName() );
		H2gisServer.getInstance().execute(
				"delete from " + RESULT_TNAME + " bd where " + " exists( select 1 from " + GARBAGE_TNAME
						+ " an where an.THE_GEOM = BD.THE_GEOM )");
		log.debug("不满足条件压盖>80%和面积差<20%的数据");
		count(RESULT_TNAME);
	}

}
