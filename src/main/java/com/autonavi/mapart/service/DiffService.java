package com.autonavi.mapart.service;

import java.sql.SQLException;

import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import com.autonavi.mapart.service.WashDataService.WashedData;
import com.autonavi.mapart.service.impl.WashDataAnCoverMultiBd;
import com.autonavi.mapart.service.impl.WashDataBdCoverMultiAn;
import com.autonavi.mapart.service.impl.WashDataCoverEdge;
import com.autonavi.mapart.service.impl.WashDataDiffMatch;
import com.autonavi.mapart.service.impl.WashDataFlexMatch;
import com.autonavi.mapart.service.impl.WashDataForFusion;
import com.autonavi.mapart.service.impl.WashDataHighMatch;
import com.autonavi.mapart.service.impl.WashDataJigsawMergeForDelete;
import com.autonavi.mapart.service.impl.WashDataJigsawMergeForReplace;
import com.autonavi.mapart.service.impl.WashDataMergeForDelete;
import com.autonavi.mapart.service.impl.WashDataMergeForReplace;
import com.autonavi.mapart.service.impl.WashDataMergeForUnintersect;
import com.autonavi.mapart.service.impl.WashDataNonUptownCover;
import com.autonavi.mapart.service.impl.WashDataPerimeterAreaRatio;

/**
 * 执行检查融合主程序 需求 <br>
 * 
 * Shape文件格式<br>
 * AN shp 列------ THE_GEOM NAME_CHN MESH POI_ID FA_TYPE AREA_FLAG <br>
 * BD shp 列---------- THE_GEOM STYLE <br>
 * POI shp 列----------
 * 参考:<br>
 * http://www.h2gis.org/docs/dev/SHPWrite/ <br>
 * http://www.h2gis.org/docs/dev/SHPRead/ <br>
 * http://www.h2gis.org/docs/dev/DBFRead/ <br>
 * 
 */
public class DiffService {
	public static Logger log = Logger.getLogger(DiffService.class);
	private static String process;
	/**
	 * java  -jar polygonFilter.jar config.properties
	 * java  -jar polygonFilter.jar config.properties filter "1,2,3"
	 * java  -jar polygonFilter.jar config.properties merge
	 * java  -jar polygonFilter.jar config.properties check
	 * @param args
	 * @throws Exception
	 */
	private static void out(){
		log.debug("配置参数有误！");
		return;
	}
	private static void mergeData(int step) throws ClassNotFoundException, SQLException {
		WashDataMerge[] merges = new WashDataMerge[] { new WashDataMergeForReplace(), 
				new WashDataMergeForDelete(),
				new WashDataMergeForUnintersect(),
				new WashDataJigsawMergeForReplace(),
				new WashDataJigsawMergeForDelete()};
		log.debug("融合步骤："+step);
		try {
			H2gisServer.getInstance().startup();
			merges[step-1].merge();
		} finally {
			H2gisServer.getInstance().shutdown();
		}
	}

	/**
	 * 1 WashDataHighMatch : 面积差20%内（保留成果）<p>
	 * 2 WashDataNonUptownCover 非小区压盖（人工确认）<p>
	 * 3 WashDataForFusion未压盖（未压盖）<p> 
	 * 4 WashDataPerimeterAreaRatio 压盖80%面积差（替换融合）<p>
	 * 5 WashDataBdCoverMultiAn BD压盖多个AN（人工确认）<p>
	 * 6 WashDataFlexMatch .面积差20%以上（替换融合）<p>
	 * 7 WashDataAnCoverMultiBd .BD压盖多个AN（删除融合）<p>
	 * 8 WashDataCoverEdge 压边（未压盖） <p>
	 * 9 WashDataDiffMatch 第6步 的导出结果(未排除)进行处理,过滤BD与AN面积差值在50%以上的数据
	 */
	private static WashDataService[] getWashServices() throws SQLException {

		String bddir = WashDataConfig.getInstance().getProperty("bdFile");
		WashedData importData = new WashDataImport().importData(bddir,"");
		
		return new WashDataService[] { new WashDataHighMatch(importData),
				new WashDataNonUptownCover(importData), new WashDataForFusion(importData),
				new WashDataPerimeterAreaRatio(importData), new WashDataBdCoverMultiAn(importData),
				new WashDataFlexMatch(importData), new WashDataAnCoverMultiBd(importData),
				new WashDataCoverEdge(importData), new WashDataDiffMatch(importData), };
	}
	private static void wash(int step) throws Exception {
		try {
			H2gisServer.getInstance().startup();
			WashDataService[] washServices = getWashServices();
			washServices[step-1].wash();
		} finally {
			H2gisServer.getInstance().shutdown();
			log.debug("End process step : " + step);
			log.debug("-----------------------------------------");
		}
	}
	
	private static void processing(int stepNum) throws Exception{
		if (process.contains("filter")) {
			wash(stepNum);
		}else if (process.contains("merge")) {
			mergeData(stepNum);
		}else{
			 out();
		}
//		if (process.contains("check")) {
//		// do check
//		}
	}
	private static void setInputFile(int stepNum, String bdPath, String anPath, WashDataConfig instance){
		String rootPath = instance.getProperty("outputPath") + "/";
		if(StringUtils.isNotBlank(anPath)) {
			String inputFile1 = process.contains("filter")||
					(process.contains("merge")&&(stepNum==4||stepNum==5))?"amapFile":
					process.contains("merge")&&(stepNum==1||stepNum==2||stepNum==3)?"anMesh":"";
			log.debug("\tload "+ inputFile1 +"      file:" + rootPath+anPath);
			instance.setProperty(inputFile1, rootPath+anPath);
		}
		if(StringUtils.isNotBlank(bdPath)){
			String inputFile2 = process.contains("filter")?"bdFile":
				process.contains("merge")&&stepNum==1? "replaceMergeFile":
					process.contains("merge")&&stepNum==2? "deleteMergeFile":
						process.contains("merge")&&stepNum==3?"nonCoverMergeFile":"";
			log.debug("\tload "+inputFile2+"      file:" +rootPath+bdPath);
			instance.setProperty(inputFile2, rootPath+bdPath);
		}
	}
	private static void resolveSteps(WashDataConfig instance , String stepPars) throws Exception {
		String[] steps = stepPars.split(",");
		int preStep = 0;
		for(String step : steps) {
			step = StringUtils.trim(step);				//删除字符串两端的空格
			log.debug("执行的步骤信息："+step);
			String[] prePath = step.split(" ");
			int stepNum = Integer.parseInt(prePath[0]);
			String bdPath = prePath.length >=2 ? prePath[1] :
						preStep!=0?preStep+"/r" : "";
			String anPath = prePath.length >=3 ? prePath[2]:"";
			log.debug("执行的步骤："+stepNum);
			log.debug("设置bd的输入路径："+bdPath);
			log.debug("设置an的输入路径"+anPath);
			
			setInputFile(stepNum, bdPath, anPath, instance);
			processing(stepNum);
			preStep = stepNum;
		}// end for
	}
	public static void main(String[] args) throws Exception {
//		if(args.length != 3 || !args[0].contains("config.properties")){
//			log.debug("批处理配置参数出错！");
//			return;
//		}
//		process = args[1];
//		WashDataConfig instance = WashDataConfig.getInstance();
//		instance.loadConfig(args[0]);
//		resolveSteps(instance,args[2]);
//		
		process = /*args[1];*/"filter"/*"merge"*/;
		String steps = /*args[2] = *//*"2,1,5,6,7,4 1/g"*//*"3 7/r AN_MERGE/JIGSAW/删除融合,8"*/
				/*"5 删除融合 AN_MERGE/JIGSAW/替换融合"*/"6 5/r";
		WashDataConfig instance = WashDataConfig.getInstance();
		instance.loadConfig("src/main/resources/config.properties");
		resolveSteps(instance,steps);/*args[2]*/
	}
}
