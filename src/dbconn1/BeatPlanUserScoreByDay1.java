package dbconn1;

import java.io.File;
import java.io.FileInputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Properties;
import java.util.Set;
import java.util.Map.Entry;

import org.apache.log4j.Logger;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.StringDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.view.Stale;
import com.couchbase.client.java.view.ViewQuery;
import com.couchbase.client.java.view.ViewResult;
import com.couchbase.client.java.view.ViewRow;
import com.couchbase.client.java.document.*;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;

public class BeatPlanUserScoreByDay1 {
	public static Bucket bucket;	
	public static Bucket userBucket;
	public static String configFile = File.separator+"src"+File.separator+"config.properties";
	private static Logger logger=Logger.getLogger("gSales");
	public static String password;
	public static String docName;
	public static String updateViewName;
	public static String updateUserViewName;
	public static DBOperations dbOp = null;	
	
	
	public void getBeatPlanScore(String docName, String viewName) throws Exception{
		//ViewResult result = bucket.query(ViewQuery.from(docName, viewName).stale(Stale.FALSE));
		ViewResult result = dbOp.executeQuery(bucket, docName, viewName);
		Date currentDate = new Date();		
		Calendar cal = Calendar.getInstance();
		cal.setMinimalDaysInFirstWeek(4);
		cal.setFirstDayOfWeek(Calendar.MONDAY);
		cal.setTime(currentDate);
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
		String prevSceCode = "";
		String sceCode = "";
		int beatPlanScore = 0;
		for(ViewRow row : result){			
			String key = "";	
			JsonArray value;				
			int year=0;			
			JsonArray weeks = null;			
			JsonObject beatPlanPointsObj = null;
			JsonObject monthObj = null;		
			
			try{
				key = (String)row.key();
				sceCode = key;
				value = (JsonArray)row.value();				
				for(Object obj:value){
					JsonObject yearObj = (JsonObject)obj;
					year = yearObj.getInt("year");					
					if(year==cal.get(Calendar.YEAR)){
						JsonArray monthsArr = yearObj.getArray("months");	
						for(Object month:monthsArr){
							monthObj = (JsonObject)month;
							weeks = monthObj.getArray("weeks");
							for(Object week:weeks){
								  try{
									  JsonObject weekObj = (JsonObject)week;
									  int weekNo = (Integer)weekObj.get("week_no");
									  logger.debug("weekObj "+weekObj.toString());
									  String beatPlanValid = (String)weekObj.get("beat_plan_valid");
									  logger.debug("Week No "+weekNo);
									  logger.debug("PreviousWeek "+previousWeek);
									  if(weekNo==previousWeek&&(beatPlanValid!=null&&beatPlanValid.equals("Y"))){
										  beatPlanPointsObj = weekObj.getObject("beat_plan_points");
										  logger.debug("beatPlanPointsObj "+beatPlanPointsObj.toString());
									  
										  if(prevSceCode.equals("")){
												beatPlanScore = 0;
												if(beatPlanPointsObj!=null)
													beatPlanScore += getBeatPlanPoints(beatPlanPointsObj);												
										  }
										  else if(!(prevSceCode.equals(sceCode))){
												
											    logger.debug("SCE CODE "+prevSceCode+" beatPlanScore "+beatPlanScore);
											    HashMap map = new HashMap();
											    map.put("beat_plan_points", beatPlanScore);
												updateUserScoreByDayBeatPlanPoints(prevSceCode, beatPlanScore, updateViewName);
												updateUserDoc(updateUserViewName, prevSceCode, map);
												beatPlanScore = 0;
												if(beatPlanPointsObj!=null)
													beatPlanScore += getBeatPlanPoints(beatPlanPointsObj);
												
										  }
										  else{
												if(beatPlanPointsObj!=null)
													beatPlanScore += getBeatPlanPoints(beatPlanPointsObj);												
										  }
									  }
								  }						 
								  catch(Exception e){
									  e.printStackTrace();
									  logger.error("Beat plan data for SCE code "+sceCode+" has not been updated "+e.getMessage());
								  }
							}
						}
					}
				}
						
			}
			catch(Exception e){	
				e.printStackTrace();
				logger.error("Beat Plan Score for sceCode "+key+" could not be obtained. "+e.getMessage());
				continue;
			}
			prevSceCode = sceCode;
		}
		HashMap map = new HashMap();
	    map.put("beat_plan_points", beatPlanScore);
		updateUserScoreByDayBeatPlanPoints(sceCode, beatPlanScore, updateViewName);
		updateUserDoc(updateUserViewName, sceCode, map);
		logger.debug("SCE CODE "+prevSceCode+" beatPlanScore "+beatPlanScore);
	}
	
	public void updateUserDoc(String updateViewName, String sceId, HashMap map){
		try{						
			logger.debug("MAP "+map);
			//ViewResult result = userBucket.query(ViewQuery.from(docName, updateViewName).key(Integer.parseInt(sceId)).stale(Stale.FALSE));
			ViewResult result = dbOp.executeQuery(userBucket, docName, updateViewName, Integer.parseInt(sceId));
			ViewRow row = null;
			List list = result.allRows();	
			if(list.size()==0){
				throw new Exception("User with SCE ID "+sceId+" does not exist.");
			}else{
				row = (ViewRow)list.get(0);		
			}
			JsonObject value = (JsonObject)row.value();
			JsonArray pointsArr = null;
			if(value.getArray("points")!=null)
				pointsArr = value.getArray("points");
			logger.debug("pointsArr "+pointsArr);
			int userTotalPoints = 0;
			if(value.getInt("total_points")!=null)
				userTotalPoints = value.getInt("total_points");		
			
			int points = 0;
			
			JsonArray updatedPointsArr = JsonArray.empty();			
			if(pointsArr==null||pointsArr.size()==0){				
				Set pointSet = map.entrySet();
				for(Object obj:pointSet){
					Entry entry = (Entry)obj;
					JsonObject updatedPointObj = JsonObject.empty()
							.put("name",entry.getKey())
							.put("value", entry.getValue());
					updatedPointsArr.add(updatedPointObj);
					userTotalPoints += (Integer)entry.getValue();
				}
			}
			else{
				for(Object obj:pointsArr){
					JsonObject pointObj = (JsonObject)obj;
					
					points = pointObj.getInt("value");
					if(map.get(pointObj.getString("name"))!=null)
						points += (Integer)map.get(pointObj.getString("name"));
					JsonObject updatedPointObj = JsonObject.empty()
							.put("name",pointObj.getString("name"))
							.put("value", points);
					logger.debug("updatedPointObj "+updatedPointObj.toString());
					updatedPointsArr.add(updatedPointObj);				
					if(map.get(pointObj.getString("name"))!=null){
						userTotalPoints += (Integer)map.get(pointObj.getString("name"));
						map.remove(pointObj.getString("name"));
					}
				}
				if(map.size()>0){
					Set pointSet = map.entrySet();
					for(Object obj:pointSet){
						Entry entry = (Entry)obj;
						JsonObject updatedPointObj = JsonObject.empty()
								.put("name",entry.getKey())
								.put("value", entry.getValue());
						updatedPointsArr.add(updatedPointObj);
						userTotalPoints += (Integer)entry.getValue();
					}
				}
			}			
			
			JsonObject jObj = (JsonObject) row.value();
			jObj.put("points",updatedPointsArr);		
			jObj.put("total_points",userTotalPoints);
			String json = jObj.toString();			
					
			StringDocument doc = StringDocument.create(row.id(),json );			
			
			logger.debug("ROW ID "+row.id()+doc);			
			dbOp.upsertDoc(userBucket, doc);
		
		}
		catch(Exception e){					
			logger.error("Points for the user with SCE ID "+sceId+" could not be updated.");
		}
	}
	
	public void updateUserScoreByDayBeatPlanPoints(String sceId, int points, String updateView) throws Exception{
		List startKeyList = new ArrayList();
		List endKeyList = new ArrayList();
		startKeyList.add(sceId);
		endKeyList.add(sceId);
		SimpleDateFormat dateFormat = new SimpleDateFormat("MM-dd-yyyy");		
		Date currentDate = new Date();
		String currentDateStr = dateFormat.format(currentDate);
		Calendar cal = Calendar.getInstance();
		cal.setTime(currentDate);		
		List dateList = new ArrayList();
		dateList.add(cal.get(Calendar.YEAR));
		dateList.add(cal.get(Calendar.MONTH)+1);
		dateList.add(cal.get(Calendar.DAY_OF_MONTH));
		dateList.add(0);
		dateList.add(0);
		dateList.add(0);
		startKeyList.add(JsonArray.from(dateList));
		List endDateList = new ArrayList();
		endDateList.add(cal.get(Calendar.YEAR));
		endDateList.add(cal.get(Calendar.MONTH)+1);
		endDateList.add(cal.get(Calendar.DAY_OF_MONTH));
		endDateList.add(23);
		endDateList.add(59);
		endDateList.add(59);
		endKeyList.add(JsonArray.from(endDateList));
		
		//ViewResult result = bucket.query(ViewQuery.from(docName, updateView).startKey(JsonArray.from(startKeyList)).endKey(JsonArray.from(endKeyList)).stale(Stale.FALSE));
		ViewResult result = dbOp.executeQuery(bucket, docName, updateView, JsonArray.from(startKeyList), JsonArray.from(endKeyList));
		SimpleDateFormat df = new SimpleDateFormat("MM-dd-yyyy HH:mm:ss");		
		String date = df.format(new Date());		
		JsonObject userScore;		
		String userId;
		List<ViewRow> rows = result.allRows();
		JsonObject pointsObj = JsonObject.empty()
		.put("name", "beat_plan_points")
		.put("value",points);
		if(rows.size()==0){			 
			JsonArray pointsArr= JsonArray.empty()
			.add(pointsObj);
			userScore = JsonObject.empty()
					  .put("type", "userscorebyday")
					  .put("timestamp", date)
					  .put("sce_code", sceId)
					  .put("points", pointsArr);
			userId = sceId + "_" + currentDateStr+"_USBD";			
			String json = userScore.toString();			
			StringDocument doc = StringDocument.create(userId,json);
			dbOp.insertDoc(bucket, doc);			
		}
		else{
			ViewRow user = (ViewRow)rows.get(0);
			userScore = (JsonObject)user.value();	
			JsonArray pointsArr = userScore.getArray("points");
			JsonArray updatedPointsArr = JsonArray.empty();
			boolean beatPlanExists = false;
			for(Object obj:pointsArr){
				JsonObject pointArrObj = (JsonObject)obj;
				
				if(pointArrObj.getString("name").equals("beat_plan_points")){
					points += pointArrObj.getInt("value");
					pointsObj = JsonObject.empty()
							.put("name", "beat_plan_points")
							.put("value",points);
					updatedPointsArr.add(pointsObj);
					beatPlanExists = true;
				}
				else{
					updatedPointsArr.add(pointArrObj);
				}
			}
			if(!beatPlanExists){
				pointsObj = JsonObject.empty()
						.put("name", "beat_plan_points")
						.put("value",points);
				updatedPointsArr.add(pointsObj);
			}			
			userScore.put("points", updatedPointsArr);
			userId = user.id();			
			logger.debug("Beat points userScore "+userScore.toString());
			
			String json = userScore.toString();			
			StringDocument doc = StringDocument.create(userId,json);
			dbOp.upsertDoc(bucket, doc);			
		}
		logger.debug("USER ID "+userId);
	}
	
	public int getBeatPlanPoints(JsonObject beatPlanObj){
		if(beatPlanObj.getString("changed_today")!=null){
			String changedToday = beatPlanObj.getString("changed_today");		
			if(changedToday.equals("Y"))
				return beatPlanObj.getInt("score");
			else
				return 0;
		}
		else
			return 0;
	}
	
	public static void main(String[] args){
		long startTime = System.currentTimeMillis();
		BeatPlanUserScoreByDay1 userScoreByDay = new BeatPlanUserScoreByDay1();
		
		try{
			String currentDir = System.getProperty("user.dir");			
			FileInputStream input = new FileInputStream(currentDir+configFile);			
			
			Properties prop = new Properties();
			prop.load(input);
			
			String bucketName = prop.getProperty("BUCKET");
			String userBucketName = prop.getProperty("USER_BUCKET");
			String dbServer = prop.getProperty("DB_SERVER");
			password = prop.getProperty("PASSWORD");
			dbOp = new DBOperations();
			bucket = dbOp.getBucket(dbServer, bucketName, prop);			
			
			if (bucket==null){
				logger.error("DB connection failed");
				System.exit(-1);
			}
			
			userBucket = dbOp.getBucket(dbServer, userBucketName, prop);			
			
			if (userBucket==null){
				logger.error("DB connection failed");
				System.exit(-1);
			}
			docName = prop.getProperty("DOC_NAME");			
			String beatPlanScoreView =prop.getProperty("BEAT_PLAN_SCORE_VIEW");			
			updateViewName = prop.getProperty("USER_SCORE_BY_DAY");			
			updateUserViewName = prop.getProperty("USER_SCORE");
			
			userScoreByDay.getBeatPlanScore(docName, beatPlanScoreView);	
			long endTime = System.currentTimeMillis();
			logger.debug("Time taken " + (endTime - startTime) + " milliseconds");
			
		}
		catch(Exception e){			
			logger.error("Connection to database failed.");
		}
		
	}

}
