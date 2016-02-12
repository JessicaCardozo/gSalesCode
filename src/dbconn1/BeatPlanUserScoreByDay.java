package dbconn1;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
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
import com.couchbase.client.java.document.StringDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.view.Stale;
import com.couchbase.client.java.view.ViewResult;
import com.couchbase.client.java.view.ViewRow;

public class BeatPlanUserScoreByDay {
	public static Bucket bucket;	
	public static Bucket userBucket;
	public static String configFile = File.separator+"src"+File.separator+"config.properties";
	private static Logger logger=Logger.getLogger("gSales");
	public static String password;
	public static String docName;
	public static String updateViewName;
	public static String updateUserViewName;
	public static DBOperations dbOp = null;
	public static String servlet_url = "";
	public static String servlet_name = "DecisionBeatPlan";
	
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
			logger.debug("Beat points userScore before updating "+userScore.toString());
			userScore.put("points", updatedPointsArr);
			userId = user.id();			
			logger.debug("Beat points userScore "+userScore.toString());
			
			String json = userScore.toString();			
			StringDocument doc = StringDocument.create(userId,json);
			dbOp.upsertDoc(bucket, doc);			
		}
		logger.debug("USER ID "+userId);
	}
	
	public void updateUserDoc(ViewRow row, HashMap map){
		String sceId = "";
		try{						
			logger.debug("MAP "+map);			
			sceId = ((Integer)row.key()).toString();
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
			logger.debug("before updating ROW ID "+row.id()+row.value());
			
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
	
	public ViewResult getBeatPlanData(String docName, String viewName) throws Exception{
		
		ViewResult result = dbOp.executeQuery(userBucket, docName, updateUserViewName);
		
		try{
			Date date = new Date();				
			
			logger.debug(Locale.getDefault());		
			Calendar cal = Calendar.getInstance();
			cal.setMinimalDaysInFirstWeek(4);
			cal.setFirstDayOfWeek(Calendar.MONDAY);
			cal.setTime(date);		
			int currentWeek = cal.get(cal.WEEK_OF_YEAR);
			int previousWeek = cal.get(cal.WEEK_OF_YEAR)-1;
			int currentYear = cal.get(Calendar.YEAR);
						
			logger.debug("currentWeek "+currentWeek);
			logger.debug("previousWeek "+previousWeek);
			
			String sceCode = "";
			
			for(ViewRow row : result){
				if(row.key()!=null)
				sceCode = ((Integer)row.key()).toString();
				
				logger.debug("--sceCode "+sceCode);
				if(!(sceCode.equals(""))){
					List list = new ArrayList();
					list.add(previousWeek);
					list.add(currentYear);
					list.add(sceCode);
					ViewResult result1 = dbOp.executeQuery(bucket, docName, viewName, JsonArray.from(list));
					ViewRow row1 = null;
					List rowList = result1.allRows();
					if(rowList.size()!=0)
						row1 = (ViewRow)rowList.get(0);
					String beatPlanAdherence="";
					int count = 0;
					if(row1!=null){
						count = (Integer)row1.value();
						logger.debug("count "+count);
						if(count>0){
							beatPlanAdherence="N";
						}
						else{
							beatPlanAdherence="Y";
						}
					}
					else{
						beatPlanAdherence="Y";
					}
					HttpURLConnection connection ;
					try{				  
						URL url = new URL(servlet_url + servlet_name + "?beatPlanAdherence="+beatPlanAdherence);
						connection =  (HttpURLConnection)url.openConnection(); 
			
						connection.setRequestMethod("GET");											  			  
						InputStream response = connection.getInputStream();
					}
					catch(Exception e){		
						logger.error("Decision execution failed for SCE Code "+sceCode);
						continue;
					}
					  
					try{
						String pointsStr = connection.getHeaderField("Points");				  
						int points = Integer.parseInt(pointsStr);	
						HashMap map = new HashMap();
					    map.put("beat_plan_points", points);
					    logger.debug("sceCode "+sceCode+" beat_plan_points "+points);
						updateUserScoreByDayBeatPlanPoints(sceCode, points, updateViewName);
						logger.debug("User doc "+row.value());
						updateUserDoc(row, map);	              
					}
					catch(Exception e){												  
						logger.error("Record for SCE Code "+sceCode+" could not be updated.");
						continue;
					}	
				}
			}
		}
		catch(Exception e){	
			e.printStackTrace();
			logger.error("Error in getBeatPlanData");
		}					  
		
		return result;
	}
	
	public static void main(String[] args){
		long startTime = System.currentTimeMillis();
		BeatPlanUserScoreByDay userScoreByDay = new BeatPlanUserScoreByDay();
		
		try{
			String currentDir = System.getProperty("user.dir");			
			FileInputStream input = new FileInputStream(currentDir+configFile);			
			
			Properties prop = new Properties();
			prop.load(input);
			
			String bucketName = prop.getProperty("BUCKET");
			String userBucketName = prop.getProperty("USER_BUCKET");
			String dbServer = prop.getProperty("DB_SERVER");
			password = prop.getProperty("PASSWORD");
			servlet_url = prop.getProperty("SERVLET_URL");
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
			String beatPlanDataView = prop.getProperty("BEAT_PLAN_DATA_VIEW");							
			updateViewName = prop.getProperty("USER_SCORE_BY_DAY");			
			updateUserViewName = prop.getProperty("USER_SCORE");			
			
			userScoreByDay.getBeatPlanData(docName, beatPlanDataView);	
			long endTime = System.currentTimeMillis();
			logger.debug("Time taken " + (endTime - startTime) + " milliseconds");
			
		}
		catch(Exception e){			
			logger.error("Connection to database failed.");
		}
		
	}
}
