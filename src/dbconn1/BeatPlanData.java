package dbconn1;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.ArrayList;
import java.util.Locale;
import java.util.Map;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.view.Stale;
import com.couchbase.client.java.view.ViewQuery;
import com.couchbase.client.java.view.ViewResult;
import com.couchbase.client.java.view.ViewRow;
import com.couchbase.client.java.document.*;

public class BeatPlanData {
	public static Bucket bucket;
	private static Logger logger=Logger.getLogger("gSales");
	public static String updateViewName;
	public static String docName;
	public static String configFile = File.separator+"src"+File.separator+"config.properties";
	public static String servlet_url = "";
	public static String servlet_name = "DecisionBeatPlan";
	public static String password;
	public static String scoreField = "beat_plan_points";
	public static DBOperations dbOp = null;	
	
	
	public ViewResult getBeatPlanData(String docName, String viewName) throws Exception{
		//ViewResult result = bucket.query(ViewQuery.from(docName, viewName).stale(Stale.FALSE));	
		ViewResult result = dbOp.executeQuery(bucket, docName, viewName);
		Date date = new Date();				
		
		logger.debug(Locale.getDefault());		
		Calendar cal = Calendar.getInstance();
		cal.setMinimalDaysInFirstWeek(4);
		cal.setFirstDayOfWeek(Calendar.MONDAY);
		cal.setTime(date);		
		int currentWeek = cal.get(cal.WEEK_OF_YEAR);
		int previousWeek = cal.get(cal.WEEK_OF_YEAR)-1;
		int currentYear = cal.get(Calendar.YEAR);
		int currentMonth = cal.get(Calendar.MONTH);		
		
		if(currentWeek==1){
			if(currentMonth==0){
				currentYear = currentYear-1;
			}
			cal.set(currentYear,12,31);			
			previousWeek=cal.getMaximum(cal.WEEK_OF_YEAR);
		}		
			
		for(ViewRow row : result){
			  JsonArray keyArr = (JsonArray)row.key();
			  JsonArray value;
			  String sceCode = "";
			  String channelCode = "";
			  String modifiedTS = "";
			  JsonArray channelCodeArr = null;
			  try{
				  sceCode = (String)keyArr.get(0);
				  channelCode = (String)keyArr.get(1);
				  value = (JsonArray)row.value();					  
				  channelCodeArr = (JsonArray)value.get(0);
				  //JsonObject obj = (JsonObject)channelCodeArr.get(0);
				  modifiedTS = value.getString(1);
				  for(Object yearObj:channelCodeArr){
					  JsonObject obj = (JsonObject)yearObj;
				  logger.debug("Months "+obj.getArray("months"));
				  JsonArray months = obj.getArray("months");
				  int year = obj.getInt("year");				  
				  if(currentYear==year){
					  List monthList = months.toList();
					  for(Object month:monthList){
						  HashMap monthObj = (HashMap)month;						  						  
						  int monthVal = (Integer)monthObj.get("month");						  
						  List weekArray = (ArrayList)monthObj.get("weeks");						  
						  List weekList = new ArrayList();
						  
						  for(Object week:weekArray){
							  try{
								  HashMap weekObj = (HashMap)week;
								  int weekNo = (Integer)weekObj.get("week_no");	
								  String beatPlanValid = (String)weekObj.get("beat_plan_valid");
								  logger.debug("Week No "+weekNo);
								  logger.debug("PreviousWeek "+previousWeek);
								  if(weekNo==previousWeek&&beatPlanValid.equalsIgnoreCase("Y")){									  
									  List visitsList = (ArrayList)weekObj.get("visits");									  
									  int plannedVisits=0;
									  for(Object visit:visitsList){
										  HashMap visitMap = (HashMap)visit;
										  plannedVisits += (Integer)visitMap.get("is_planed_visit");
									  }
									  logger.debug("Planned visits for week "+weekNo+" are "+plannedVisits);
									  int targetVisits = (Integer)weekObj.get("target_visits");
									  logger.debug("Target visits "+targetVisits);
									  String beatPlanAdherence="";
									  if(plannedVisits-targetVisits==0&&!(plannedVisits==0&&targetVisits==0)){
										  beatPlanAdherence="Y";
									  }
									  else{
										  beatPlanAdherence="N";
									  }
									  HashMap beatPlanScore = null;
									  int score = 0;
									  logger.debug(sceCode+"  "+weekNo+" "+row.id()+" "+weekObj.get("beat_plan_points"));
									  if(weekObj.get("beat_plan_points")!=null){
										    beatPlanScore = (HashMap)weekObj.get("beat_plan_points");
									    	if(!(beatPlanScore.isEmpty())){
									    		if(beatPlanScore.get("score")!=null)
									    			score = (Integer)beatPlanScore.get("score");
									    	}
									    		
									  }
									  if(beatPlanScore==null||beatPlanScore.isEmpty()||(!(beatPlanScore.isEmpty())&&score==0)){
										  HttpURLConnection connection ;
										  try{				  
											  URL url = new URL(servlet_url + servlet_name + "?beatPlanAdherence="+beatPlanAdherence);
											  connection =  (HttpURLConnection)url.openConnection(); 
								
											  connection.setRequestMethod("GET");											  			  
											  InputStream response = connection.getInputStream();
										  }
										  catch(Exception e){											  
											  logger.error("Decision execution failed for SCE Code "+sceCode+ " and channel code "+channelCode);
											  continue;
										  }
										  
										  try{
											  String pointsStr = connection.getHeaderField("Points");				  
											  int points = Integer.parseInt(pointsStr);
											  if(points!=0)
												  updateCoverageDoc(sceCode,channelCode,year,weekNo,points,beatPlanScore,modifiedTS);	              
										  }
										  catch(Exception e){												  
											  logger.error("Record for SCE Code "+sceCode+" could not be updated.");
											  continue;
										  }	
									  }
								  }
						      }
							  catch(Exception e){								  
								  logger.error("Beat plan data for SCE code "+sceCode+" and Channel code "+channelCode+" has not been updated "+e.getMessage());
							  }
						  }
					  }
				  }
			  }
			  }
			  catch(Exception e){
				  logger.error("Beat plan data for SCE code "+sceCode+" and Channel code "+channelCode+" has not been updated "+e.getMessage());
				  continue;
			  }			  
		}
		return result;
	}
	
	public void updateCoverageDoc(String sceCode, String channelCode, int year, int weekNo, int value, HashMap beatPlanScore, String modifiedTS) throws Exception{
		List list = new ArrayList();
		list.add(sceCode);
		list.add(channelCode);
		logger.debug("sceCode "+sceCode);
		logger.debug("channelCode "+channelCode);		
		//ViewResult result = bucket.query(ViewQuery.from(docName, updateViewName).key(JsonArray.from(list)).stale(Stale.FALSE));
		ViewResult result = dbOp.executeQuery(bucket, docName, updateViewName, JsonArray.from(list));
		ViewRow cust = result.allRows().get(0);
		JsonObject jObj = (JsonObject) cust.value();		
		JsonArray channelCoverageArr = jObj.getArray("channel_coverage");
		int totalBeatScore;
		if(jObj.getInt("total_beat_score")==null)
			totalBeatScore=0;
		else
			totalBeatScore = jObj.getInt("total_beat_score");
		logger.debug("total beat score "+totalBeatScore);
		List channelCoverageList = channelCoverageArr.toList();
		List updatedChannelCoverageList = new ArrayList();
		int listCount = 0;
		HashMap map=null;
		for(Object obj:channelCoverageList){
			map = (HashMap)obj;
			logger.debug("obj.toString() "+map.toString());
			ArrayList monthsList = (ArrayList)map.get("months");
			
			if(year== (Integer)map.get("year")){
				int monthCount=0;
				HashMap monthMap=null;
				for1:for(Object month:monthsList){
					monthMap =(HashMap)month;
					
					ArrayList weekList = (ArrayList)monthMap.get("weeks");						
					int count = 0;
					for(Object weekObj:weekList){
						HashMap weekMap = (HashMap)weekObj;
						if(weekNo==(Integer)weekMap.get("week_no")){								
								
							Map pointsMap;
							if(beatPlanScore==null||beatPlanScore.isEmpty()){		
								pointsMap = new HashMap();
									
							}
							else{
								pointsMap = beatPlanScore;
							}
							pointsMap.put("score_name", scoreField);
							if(value!=0){
								pointsMap.put("score", value);		
								pointsMap.put("timestamp", modifiedTS);
							}
							pointsMap.put("changed_today", "Y");
							weekMap.put("beat_plan_points", JsonObject.from(pointsMap));									
							weekList.remove(count);
							weekList.add(count,weekMap);								
							monthMap.put("weeks", weekList);					
							totalBeatScore += value;
							break for1;
						}
						count++;
					}						
					monthCount++;					
				}
				if(monthMap!=null){
					monthsList.remove(monthCount);
					monthsList.add(monthCount,monthMap);
				}
				map.put("months", monthsList);				
				break;
			}
			//channelCoverageList.remove(listCount);
			//channelCoverageList.add(listCount,map);	
			updatedChannelCoverageList.add(map);
			listCount++;
		}		
		logger.debug("channelCoverageList "+channelCoverageList);		
		logger.debug("CUSTOMER ID "+cust.id());
		
		
		jObj.put("channel_coverage",JsonArray.from(channelCoverageList));		
		jObj.put("total_beat_score", totalBeatScore);
		String json = jObj.toString();		
		StringDocument doc = StringDocument.create(cust.id(),json );	
		dbOp.upsertDoc(bucket, doc);		
	}
	
	public static void main(String[] args){
		long startTime = System.currentTimeMillis();
		BeatPlanData beatPlanData = new BeatPlanData();		
		try{
			String currentDir = System.getProperty("user.dir");			
			FileInputStream input = new FileInputStream(currentDir+configFile);
			
			Properties prop = new Properties();
			prop.load(input);						
			
			String dbServer = prop.getProperty("DB_SERVER");			
			String bucketName = prop.getProperty("BUCKET");
			password = prop.getProperty("PASSWORD");
			dbOp = new DBOperations();		
			
			bucket = dbOp.getBucket(dbServer, bucketName, prop);
			if (bucket==null){
				logger.error("DB connection failed");
				System.exit(-1);
			}
			docName = prop.getProperty("DOC_NAME");
			String viewName = prop.getProperty("BEAT_PLAN_VIEW");
			updateViewName = prop.getProperty("BEAT_PLAN_UPDATE_VIEW");
			servlet_url = prop.getProperty("SERVLET_URL");
			beatPlanData.getBeatPlanData(docName, viewName);
			long endTime = System.currentTimeMillis();
			logger.debug("Time taken " + (endTime - startTime) + " milliseconds");
						
		}
		catch(Exception e){			
			logger.error("Connection to database failed.");
		}
		
	}
}
