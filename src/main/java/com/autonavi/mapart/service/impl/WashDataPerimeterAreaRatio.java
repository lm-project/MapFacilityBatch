package com.autonavi.mapart.service.impl;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Arrays;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Map.Entry;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.stream.Collectors;

import org.apache.commons.lang.StringUtils;

import com.autonavi.mapart.service.H2gisServer;
import com.autonavi.mapart.service.WashDataConfig;
import com.autonavi.mapart.service.WashDataService;

/**
 * step4
 * 对@reference WashDataHighMatch的导出结果进行处理 <br/>
 * 输出:周长/面积 > 0.2 的数据
 * 
 */
public class WashDataPerimeterAreaRatio extends WashDataService {

	private String outFilePath;
	private double lengt2AreaRatio;
	private double areaRatio;
	private double highPercentRatio;
	private WashedData preWashedData;
    private String replaceMergeFile;
    private String needProcessAnTable;
    private String peopleSure;
	public WashDataPerimeterAreaRatio(WashedData washedData) {
		super();
		WashDataConfig properties = WashDataConfig.getInstance();
		
		this.outFilePath = properties.getProperty("outputPath");
		this.replaceMergeFile=properties.getProperty("replaceMergeFile");
		this.preWashedData = washedData;
		this.needProcessAnTable = preWashedData.getResultTableName();//BD拼图
		this.peopleSure = properties.getProperty("peopleSure");
		
		this.areaRatio =  Double.parseDouble(properties.getProperty("areaRatio"));

		this.lengt2AreaRatio =  Double.parseDouble(properties.getProperty("lengthAreaRatio"));
		this.highPercentRatio = Double.parseDouble(properties.getProperty("areaCoverPercent"));
	}

	@Override
	public WashedData wash() {
		final String STEP_NAME = "形状确认";
		log.debug("begin process step 4," + STEP_NAME + ",proc table :"+ needProcessAnTable + "," + count(needProcessAnTable));
		WashDataService.WashedData washedData = new WashDataService.WashedData(outFilePath + "/4/g/" + STEP_NAME
				+ "_garbage.shp", outFilePath + "/4/r/" + STEP_NAME + ".shp", "BD_PARATIO_G_MERGE", "BD_PARATIO_MERGE");
		try {
			log.debug("查询符合面积/长度比值条件的数据");
			Map<String, Collection<String>> intersections = findAllIntersections();
			log.debug("插入结果数据");
			final String outputAnTable = washedData.getResultTableName();
			insertPolygons(outputAnTable, filterResult(intersections));

			final int num = count(outputAnTable);
			H2gisServer.getInstance().outputShape(washedData.getResultFile(), outputAnTable);
			log.debug("保存替换融合的bd数据" + num );
			H2gisServer.getInstance().outputShape( replaceMergeFile+"/BD/高匹配度周长面积比确认_4_"+num+".shp", outputAnTable);
			H2gisServer.getInstance().outputShape( peopleSure+"/BD/高匹配度周长面积比确认_4_"+num+".shp", outputAnTable);
			
			final String anTableName = createAnPolygonByBd(outputAnTable);
			final int anTnameNum = count(anTableName);
			log.debug("保存替换融合的an数据 " + anTnameNum);
			H2gisServer.getInstance().outputShape( replaceMergeFile+"/AN/高匹配度周长面积比确认_4_"+anTnameNum+".shp", anTableName);
			H2gisServer.getInstance().outputShape( peopleSure+"/AN/高匹配度周长面积比确认_4_"+anTnameNum+".shp", anTableName);
		} catch (SQLException e) {
			e.printStackTrace();
		}
		return washedData;
	}

	/**
	 * 正查 BD : AN，反查 AN : BD
	 * 
	 * @return 满足条件的BD数据
	 * @throws SQLException
	 */
	private Map<String, Collection<String>> findAllIntersections() throws SQLException  {
		Map<String, Collection<String>> intersectionsA = findIntersections(getDiffAB(true));
		log.debug("find intersection size(B,A) :" + intersectionsA.size());
		deleteInterestionFromBd(intersectionsA);
		Map<String, Collection<String>> intersectionsB = findIntersections(getDiffAB(false));
		log.debug("find intersection size(A,B) :" + intersectionsB.size());
		for (Entry<String, Collection<String>> e : intersectionsB.entrySet()) {
			Collection<String> values = intersectionsA.remove(e.getKey());
			if (values == null) {
				values = e.getValue();
			} else {
				values.addAll(e.getValue());
			}
			intersectionsA.put(e.getKey(), values);
		}
		return intersectionsA;
	}

	private void deleteInterestionFromBd(Map<String, Collection<String>> geom2Diffs) throws SQLException {
		log.debug("删除前的An数据:"+count(needProcessAnTable));
		geom2Diffs.keySet().stream().forEach(anGeom -> {
			H2gisServer.getInstance().execute("delete from  " + this.needProcessAnTable
					+ " where THE_GEOM = ST_GeomFromText('"+anGeom+"')" );
		});
		log.debug("删除后的An数据:"+count(needProcessAnTable));
	}

	private String getDiffAB(boolean forward) {
		String diff = forward ? "bd.THE_GEOM, an.THE_GEOM" : "an.THE_GEOM, bd.THE_GEOM";
		return "select ST_Difference(" + diff + "),bd.THE_GEOM from " + this.needProcessAnTable + " bd ,"
				+ AUTONAVI_TNAME + " an " + " where ST_Intersects(an.THE_GEOM,BD.THE_GEOM) = true";
	}

	private Collection<String> filterResult(Map<String, Collection<String>> intersections) throws SQLException {
		 return intersections.entrySet().stream().filter(e -> isOk(e.getValue())).map(e -> e.getKey()).collect(Collectors.toSet());
	}

	private boolean isOk(Collection<String> diffpolygons) {
		String tableName = "TMP_LengthArea";
		try {
			H2gisServer.getInstance().execute("create table "+tableName+" as select THE_GEOM from "
		              +this.needProcessAnTable+" where 1=2 ");
			diffpolygons.stream().forEach(polygon -> {
				H2gisServer.getInstance().execute("insert into " + tableName +"(THE_GEOM) values(ST_GeomFromText('" + polygon + "'))");
			});
			
			String sql = "select count(1) from " + tableName + " where ST_Area(THE_GEOM) > "+areaRatio+" and ST_Area(THE_GEOM)/ST_Length(THE_GEOM)> "+this.lengt2AreaRatio;
			ResultSet rs = H2gisServer.getInstance().executeQuery("select ST_Area(THE_GEOM),ST_Length(THE_GEOM)  from " + tableName);
			while(rs.next()) {
				log.debug(rs.getDouble(1)+"," + rs.getDouble(2));
			}
			rs.close();
			return H2gisServer.getInstance().queryForInt(sql) > 0;
		} catch (Exception e) {
			log.debug(e);
		} finally {
			H2gisServer.getInstance().execute("drop table " + tableName);
		}
		return false;
	}

	private void insertPolygons(String resultTableName, Collection<String> rsPolygons) throws SQLException {
		H2gisServer.getInstance().execute(
				"create table " + resultTableName + " as select * from " + BD_TNAME + " where 1=2 ");
		rsPolygons.stream().forEach( polygon -> {
			H2gisServer.getInstance().execute(
						"insert into " + resultTableName + "(THE_GEOM) values (ST_GeomFromText('" + polygon + "'))");
		});
	}

	/**
	 * 第一列是差分的结果
	 * 第二列是bd的数据
	 */
	private Map<String, Collection<String>> findIntersections(String sql) throws SQLException  {
		ResultSet rs = H2gisServer.getInstance().executeQuery(sql);
		Map<String, Collection<String>> bd2diffPolygons = new HashMap<String, Collection<String>>();
		while (rs.next()) {
			String diffGeometry = rs.getObject(1).toString();
			log.debug(diffGeometry);
			if (StringUtils.startsWith(diffGeometry, "POLYGON EMPTY")) {
				continue;
			}
			String bdGeometry = rs.getObject(2).toString();
			if (StringUtils.startsWith(diffGeometry, "MULTIPOLYGON")) {
				Collection<String> mu = parseMultiPolygon(diffGeometry);
				bd2diffPolygons.put(bdGeometry, mu);
			} else {
				bd2diffPolygons.put(bdGeometry, new HashSet<String>(Arrays.asList(diffGeometry)));
			}
		}
		rs.close();
		return bd2diffPolygons;
	}

	
	/**
	 * 获取An的原始数据
	 */
	private String createAnPolygonByBd(String resultTableName) {
		String anTname = "FNL_AN_4";
		String sql = "create table "+anTname+" as select an.* from " + AUTONAVI_TNAME + " an "
				+ " where exists ( select 1 from  "+resultTableName+" bd "
						+ " where ST_Intersects(an.THE_GEOM,BD.THE_GEOM) = true  )";
		H2gisServer.getInstance().execute(sql);
		return anTname;
	}
	
	Collection<String> parseMultiPolygon(String multiPolygon) {
		Pattern p = Pattern.compile("(([\\., [0-9]\\+]*))");
		Matcher matcher = p.matcher(multiPolygon);
		Collection<String> rs = new HashSet<String>();
		while (matcher.find()) {
			if (StringUtils.isBlank(matcher.group()) || matcher.group().equals(", ")) {
				continue;
			}
			rs.add("POLYGON((" + matcher.group() + "))");
		}
		return rs;
	}
}
