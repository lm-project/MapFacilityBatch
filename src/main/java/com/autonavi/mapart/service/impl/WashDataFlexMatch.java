package com.autonavi.mapart.service.impl;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Collection;
import java.util.HashSet;

import com.autonavi.mapart.service.H2gisServer;
import com.autonavi.mapart.service.WashDataConfig;
import com.autonavi.mapart.service.WashDataService;

/**
 * step 6
 * 对@reference WashDataBdCoverMultiAn 的导出结果进行处理
 * 
 * 1.输入：第五步(WashDataBdCoverMultiAn)未排除的BD数据、AN数据拼图;
 * 2.AN与BD压盖面积超过85%(步骤1参数)，且X<面积差值≤N(N=50%可调整),X=N-20%(步骤1参数)
 * 3.输出:满足条件2的BD数据、不满足条件2的BD数据、满足条件2的AN数据
 *
 */
public class WashDataFlexMatch extends WashDataService {
	
	private String outFilePath;
	private double areaCoverPercent;    //步骤1参数(压盖面积百分比)
	private double diffCoverPercent;    //步骤1参数(面积差值百分比)
	private WashedData preWashedData;
	private String replaceMergeFile;
	private String peopleSure;        //人工确认路径
	public WashDataFlexMatch(WashedData washedData){
		super();
		WashDataConfig properties = WashDataConfig.getInstance();
		this.outFilePath = properties.getProperty("outputPath");
		this.areaCoverPercent = Double.parseDouble(properties.getProperty("areaCoverPercent"));
		this.diffCoverPercent = Double.parseDouble(properties.getProperty("diffCoverPercent"));
		this.preWashedData = washedData;
		this.replaceMergeFile = properties.getProperty("replaceMergeFile");
		this.peopleSure = properties.getProperty("peopleSure");

	}

	@Override
	public WashedData wash() {
		 try {
			final String STEP_NAME = "形状灵活匹配";
			WashDataService.WashedData washedData = new WashDataService.WashedData(outFilePath + "/6/g/" + STEP_NAME
					+ "_garbage.shp", outFilePath + "/6/r/" + STEP_NAME + ".shp", "BD_FLEXCOVER_NOMERGE", preWashedData.getResultTableName());
			log.debug("preWashedData.getResultTableName()========:"+preWashedData.getResultTableName());
			log.debug("=========="+washedData.getResultTableName());
			log.debug("输入bd的数据数量（第五步剩余的bd数据）===："+count(preWashedData.getResultTableName()));
			//创建BD符合条件的表
			createEmptyTable(BD_TNAME,washedData.getGarbageTableName());
//			step1( preWashedData.getResultTableName(), washedData.getGarbageTableName() );
			//查找满足条件BD数据
			filterFlexCover( preWashedData.getResultTableName(), washedData.getGarbageTableName());
			//过滤结果
			filterResult(washedData.getResultTableName(), washedData.getGarbageTableName());
			//导出shape(满足条件的bd数据)
			H2gisServer.getInstance().outputShape(washedData.getGarbageFile(), washedData.getGarbageTableName());
			//导出shape(不满足条件的bd数据)
			H2gisServer.getInstance().outputShape(washedData.getResultFile(), washedData.getResultTableName());
			 
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return null;
	}
	/**
	 * 删除满足条件的BD数据
	 * @param resultTableName
	 * @param garbageTableName
	 * @throws SQLException
	 */
	private void filterResult(String resultTableName, String garbageTableName) throws SQLException {
		String sql = "delete from " + resultTableName + " bd where bd.THE_GEOM in (select THE_GEOM from " + garbageTableName + ")";
		H2gisServer.getInstance().execute(sql);
		log.debug("BD与AN面积压盖及差值不满足条件");
		count(resultTableName);
	}
	
	/**
	 * 1.以AN数据为准，压盖面积大于等于85%以上； <br\>
	 * 2.面积差值在20%内的BD数据
	 */
	
	private void filterFlexCover( String resultTableName, String garbageTableName) throws SQLException {
		step1(resultTableName, garbageTableName);
//		step2(resultTableName);
		log.debug("an================"+anGeomData.size());
		log.debug("bd================"+bdGeomData.size());
		output(createBdTmpTable(bdGeomData,BD_TNAME,garbageTableName),
				replaceMergeFile+"/BD/形状灵活匹配_6_"+count(garbageTableName)+".shp");
		//输出到人工确认路径
		output(garbageTableName, peopleSure+"/BD/形状灵活匹配_6_"+count(garbageTableName)+".shp");
		String tmpAnTable = "TMP_AN_6";
		createEmptyTable(AUTONAVI_TNAME,tmpAnTable);
		createAnTmpTable(anGeomData,AUTONAVI_TNAME,tmpAnTable);
		count(tmpAnTable);
		String finalAnTable = "FNL_AN_6";
		output(selectAllAnDataByGeom(tmpAnTable, finalAnTable), replaceMergeFile+"/AN/形状灵活匹配_6_"+count(finalAnTable)+".shp" );
		//输出到人工确认路径
		output(finalAnTable, peopleSure+"/AN/形状灵活匹配_6_"+count(finalAnTable)+".shp" );

	}
	
	/**
	 * 1.以AN数据为准，压盖面积大于等于85%以上； <br\>
	 * 2.面积差值在20%内的BD数据
	 * 3.输出an.the_geom,bd.the_geom
	 * @return 
	 * @throws SQLException 
	 */
	private final String tmp = "TMP1";
	private final String tmp2 = "TMP2";
	Collection<String> anGeomData = new HashSet<String>();
	Collection<String> bdGeomData = new HashSet<String>();
	private void step1(String resultTableName, String garbageTableName) throws SQLException{
		
		String sql2 = "create table " + tmp + " as select distinct THE_GEOM FROM "
				+ "("
				+ "SELECT bd.THE_GEOM"
				+ ",(select count(1) from "
				+ AUTONAVI_TNAME
				+ " an where ST_Intersects(bd.THE_GEOM,an.THE_GEOM) = true"
				+ ") num "
				+ "FROM "+resultTableName+" bd ) WHERE NUM = 1";
		H2gisServer.getInstance().execute(sql2);
		log.debug("tmp的数量："+count(tmp));
		
		String sql3 = "create table " + tmp2 + "  as select distinct THE_GEOM FROM "
				+ "("
				+ "SELECT an.THE_GEOM"
				+ ",(select count(1) from "
				+ resultTableName
				+ " bd where ST_Intersects(an.THE_GEOM,bd.THE_GEOM) = true"
				+ ") num "
				+ "FROM "+AUTONAVI_TNAME+" an ) WHERE NUM = 1";
		H2gisServer.getInstance().execute(sql3);
		log.debug("tmp2的数量："+count(tmp2));
		
		String sql1 = "SELECT distinct bd.THE_GEOM,an.THE_GEOM "
				+ "FROM "+ tmp2 + " an," + tmp+ " bd "
				+ "WHERE ST_Intersects(an.THE_GEOM,bd.THE_GEOM) = true "
				+ "and (ST_Area(ST_Intersection(an.THE_GEOM, bd.THE_GEOM))/ST_Area(an.THE_GEOM)"
						+ " >="+ areaCoverPercent
				+ " or ST_Area(ST_Intersection(an.THE_GEOM, bd.THE_GEOM))/ST_Area(bd.THE_GEOM)"
						+ " >="+ areaCoverPercent+")";

		ResultSet rs = H2gisServer.getInstance().executeQuery(sql1);
		while(rs.next()){
			anGeomData.add(rs.getString(2));
			bdGeomData.add(rs.getString(1));
		}
		log.debug("bd压盖一个an满足的条件："+bdGeomData.size());
	}
	private final String tmp3 = "TMP3";
	private void step2(String resultTableName){
		String sql1 = "create table " + tmp3 + " as select distinct THE_GEOM FROM "
				+ "("
				+ "SELECT bd.THE_GEOM"
				+ ",(select count(1) from "
				+ AUTONAVI_TNAME
				+ " an where ST_Intersects(an.THE_GEOM,bd.THE_GEOM) = true"
				+ ") num "
				+ "FROM "+resultTableName+" bd ) WHERE NUM > 1";
		H2gisServer.getInstance().execute(sql1);
		log.debug("bd压盖2个an以上的bd数量："+count(tmp3, false));
		String sql2 = "select bd.THE_GEOM,an.THE_GEOM,"
				+ "ST_Area(ST_Intersection(an.THE_GEOM, bd.THE_GEOM)),"
				+ "ST_Area(bd.THE_GEOM), ST_Area(an.THE_GEOM)"
				+ " from " + tmp3 + " bd, " + AUTONAVI_TNAME + " an "
				+ "where ST_Intersects(bd.THE_GEOM,an.THE_GEOM) = true";
		ResultSet rs = H2gisServer.getInstance().executeQuery(sql2);
//		Map<String, Collection<String>> bdAnsGeoms = new HashMap<String, Collection<String>>();
		try {
			while (rs.next()) {
				if(rs.getDouble(3)/rs.getDouble(4)>=0.8 || rs.getDouble(3)/rs.getDouble(5)>=0.8 ){
					bdGeomData.add(rs.getString(1));
					anGeomData.add(rs.getString(2));
				}
//				Collection<String> tmpANs = bdAnsGeoms.remove(rs.getString(1));
//				if(tmpANs == null ) {
//					log.debug("------------------------------------------------");
//					tmpANs = new ArrayList<String>();
//				}
//				tmpANs.add(rs.getString(2));
//				log.debug("an的数量："+tmpANs.size());
//				bdAnsGeoms.put(rs.getString(1), tmpANs);
			}
			log.debug("bd压盖一个an和bd压盖2个an以上满足条件的bd数量:"+anGeomData.size());
		} catch (Exception e) {
			e.printStackTrace();
		}
		
	}
	
	
}
