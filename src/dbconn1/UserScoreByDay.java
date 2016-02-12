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
import java.util.Map;
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

public class UserScoreByDay {
	public static Bucket bucket;	
	public static Bucket userBucket;	
	public static String configFile = File.separator+"src"+File.separator+"config.properties";
	private static Logger logger=Logger.getLogger("gSales");
	public static String password;
	public static String docName;
	public static String updateViewName;
	public static String updateUserViewName;
	public static String caseDisbursedScoreField = "case_disbursed_score";
	public static String tatSToDScoreField = "dtat_s_to_d";
	public static String ftrScoreField = "d_ftr";
	public static String pddcScoreField = "pddc_points";
	public static String tatLToSScoreField = "dtat_l_to_s";
	public static String weekOfMonthScoreField = "week_of_month_points";
	public static String loginScoreField = "login_points";
	public static String enquiryField = "enquiry_points";
	public static String beatPlanField = "beat_plan_points";
	public static DBOperations dbOp = null;
	
	public void getScoreData(String docName, String viewName) throws Exception{
		SimpleDateFormat df = new SimpleDateFormat("MM-DD-yyyy");
		String date = df.format(new Date());
		//ViewResult result = bucket.query(ViewQuery.from(docName, viewName).stale(Stale.FALSE));
		ViewResult result = dbOp.executeQuery(bucket, docName, viewName);
		int caseDisbursedScore = 0;
		int tatSToDScore = 0;
		int ftrScore = 0;
		int pddcScore = 0;
		int tatLToSScore = 0;
		int weekOfMonthScore = 0;
		int loginScore = 0;
		String prevSceCode = "";
		String sceCode = "";
		String pointsName = "";
		for(ViewRow row : result){			
			String key;	
			JsonArray value;
			int points=0;			
			
			try{
				key = (String)row.key();
				value = (JsonArray)row.value();
				
				int cnt = 0;
				for(Object obj:value){
					JsonObject scoreObj = (JsonObject)obj;
					if(((String)scoreObj.get("changed_today")).equalsIgnoreCase("Y"))
						points = scoreObj.getInt("points");
						
						sceCode = key;
						if(prevSceCode.equals("")){							
							if(((String)scoreObj.get("type")).equals(caseDisbursedScoreField)){								
								caseDisbursedScore = points;
							}
							else if(((String)scoreObj.get("type")).equals(tatSToDScoreField)){
								tatSToDScore = points;
							}
							else if(((String)scoreObj.get("type")).equals(ftrScoreField)){								
								ftrScore = points;
							}
							else if(((String)scoreObj.get("type")).equals(pddcScoreField)){
								pddcScore = points;
							}
							else if(((String)scoreObj.get("type")).equals(tatLToSScoreField)){
								tatLToSScore = points;
							}
							else if(((String)scoreObj.get("type")).equals(weekOfMonthScoreField)){
								weekOfMonthScore = points;
							}
							else if(((String)scoreObj.get("type")).equals(loginScoreField)){
								loginScore = points;
							}
						}
						else if(!(prevSceCode.equals(sceCode))){							
							if(!(prevSceCode.equals(sceCode))&&cnt==0&&!(prevSceCode.equals(""))){
								    
								    logger.debug("SCE CODE "+prevSceCode+" caseDisbursedScore "+caseDisbursedScore);
								    logger.debug("SCE CODE "+prevSceCode+" tatSToDScore "+tatSToDScore);
								    logger.debug("SCE CODE "+prevSceCode+" ftrScore "+ftrScore);
								    logger.debug("SCE CODE "+prevSceCode+" pddcScore "+pddcScore);
								    logger.debug("SCE CODE "+prevSceCode+" tatLToSScore "+tatLToSScore);
								    logger.debug("SCE CODE "+prevSceCode+" weekOfMonthScore "+weekOfMonthScore);
									JsonArray pointsArr = getPointsArray(caseDisbursedScore, tatSToDScore, ftrScore, pddcScore, tatLToSScore, weekOfMonthScore, loginScore);
									logger.debug("pointsArr "+pointsArr);
									
									HashMap map = new HashMap();														
									map.put(caseDisbursedScoreField, caseDisbursedScore);	
									map.put(tatSToDScoreField, tatSToDScore);
									map.put(ftrScoreField, ftrScore);
									map.put(pddcScoreField, pddcScore);
									map.put(tatLToSScoreField, tatLToSScore);
									map.put(weekOfMonthScoreField, weekOfMonthScore);
									map.put(loginScoreField, loginScore);
									
									caseDisbursedScore = 0;
									tatSToDScore = 0;
									ftrScore = 0;
									pddcScore = 0;
									tatLToSScore = 0;
									weekOfMonthScore = 0;
									loginScore = 0;
									
									updateUserScoreByDayDoc(prevSceCode, pointsArr, updateViewName);
									updateUserDoc(updateUserViewName, prevSceCode, map);
									
							}
							if(((String)scoreObj.get("type")).equals(caseDisbursedScoreField)){								
								caseDisbursedScore = points;
							}
							else if(((String)scoreObj.get("type")).equals(tatSToDScoreField)){
								tatSToDScore = points;
							}
							else if(((String)scoreObj.get("type")).equals(ftrScoreField)){
								ftrScore = points;
							}
							else if(((String)scoreObj.get("type")).equals(pddcScoreField)){
								pddcScore = points;
							}
							else if(((String)scoreObj.get("type")).equals(tatLToSScoreField)){
								tatLToSScore = points;
							}
							else if(((String)scoreObj.get("type")).equals(weekOfMonthScoreField)){
								weekOfMonthScore = points;
							}
							else if(((String)scoreObj.get("type")).equals(loginScoreField)){
								loginScore = points;
							}
						}
						else{
							if(((String)scoreObj.get("type")).equals(caseDisbursedScoreField)){	
								caseDisbursedScore += points;
							}
							else if(((String)scoreObj.get("type")).equals(tatSToDScoreField)){
								tatSToDScore += points;
							}
							else if(((String)scoreObj.get("type")).equals(ftrScoreField)){
								ftrScore += points;
							}
							else if(((String)scoreObj.get("type")).equals(pddcScoreField)){
								pddcScore += points;
							}
							else if(((String)scoreObj.get("type")).equals(tatLToSScoreField)){
								tatLToSScore += points;
							}
							else if(((String)scoreObj.get("type")).equals(weekOfMonthScoreField)){
								weekOfMonthScore += points;
							}
							else if(((String)scoreObj.get("type")).equals(loginScoreField)){
								loginScore += points;
							}
						}
						cnt++;
				}				
				
				prevSceCode = sceCode;
			}
			catch(Exception e){					
				logger.error("Case Disbursed Score for sceCode "+sceCode+" could not be obtained. "+e.getMessage());
				continue;
			}
			
			
		}
		JsonArray pointsArr = getPointsArray(caseDisbursedScore, tatSToDScore, ftrScore, pddcScore, tatLToSScore, weekOfMonthScore, loginScore);
		HashMap map = new HashMap();														
		map.put(caseDisbursedScoreField, caseDisbursedScore);	
		map.put(tatSToDScoreField, tatSToDScore);
		map.put(ftrScoreField, ftrScore);
		map.put(pddcScoreField, pddcScore);
		map.put(tatLToSScoreField, tatLToSScore);
		map.put(weekOfMonthScoreField, weekOfMonthScore);
		map.put(loginScoreField, loginScore);
		
		
		if(!(sceCode.equals(""))){
			try {
				updateUserScoreByDayDoc(sceCode, pointsArr, updateViewName);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				logger.debug("Error updating User Score By Day Doc for User "+sceCode);
			}
			updateUserDoc(updateUserViewName, sceCode, map);
			logger.debug("SCE CODE "+sceCode+" caseDisbursedScore "+caseDisbursedScore);
			logger.debug("SCE CODE "+sceCode+" tatSToDScore "+tatSToDScore);
			logger.debug("SCE CODE "+sceCode+" ftrScore "+ftrScore);
			logger.debug("SCE CODE "+sceCode+" pddcScore "+pddcScore);
			logger.debug("SCE CODE "+sceCode+" tatLToSScore "+tatLToSScore);
			logger.debug("SCE CODE "+sceCode+" weekOfMonthScore "+weekOfMonthScore);
		}
	}
	
	
	
	public void getEnquiryScore(String docName, String viewName) throws Exception{
		//ViewResult result = bucket.query(ViewQuery.from(docName, viewName).stale(Stale.FALSE));
		ViewResult result = dbOp.executeQuery(bucket, docName, viewName);
		String prevSceCode = "";
		String sceCode = "";
		int enquiryScore = 0;
		for(ViewRow row : result){
			String key = "";	
			JsonObject value;
			int points = 0;
			try{
				key = (String)row.key();
				sceCode = key;
				value = (JsonObject)row.value();
				points = Integer.parseInt(value.getString("points"));
				logger.debug("**SCE CODE "+sceCode+" Enquiry score "+value.toString());
				if(prevSceCode.equals("")){												
					enquiryScore = points;					
				}
				else if(!(prevSceCode.equals(sceCode))&&!(prevSceCode.equals(""))){					
					logger.debug("SCE CODE "+prevSceCode+" enquiryScore "+enquiryScore);	
					HashMap map = new HashMap();
					map.put(enquiryField, enquiryScore);
					updateUserScoreByDayPoints(prevSceCode, enquiryField, enquiryScore, updateViewName);	
					updateUserDoc(updateUserViewName, prevSceCode, map);
					enquiryScore = points;					
				}
				else{					
					enquiryScore += points;					
				}
			}
			catch(Exception e){					
				logger.error("Enquiry Score for sceCode "+key+" could not be obtained. "+e.getMessage());
				continue;
			}
			prevSceCode = sceCode;
		}
		HashMap map = new HashMap();
		map.put(enquiryField, enquiryScore);
		if(!(sceCode.equals(""))){
			try {
				updateUserScoreByDayPoints(sceCode, enquiryField, enquiryScore, updateViewName);
			} catch (Exception e) {
				// TODO Auto-generated catch block
				logger.debug("Error updating USBD doc with enquiry points for user "+sceCode);
			}
			updateUserDoc(updateUserViewName, sceCode, map);
		}
	}	
	
	public JsonObject createScoreObject(String fieldName, int score){
		return JsonObject.empty()
				.put("name", fieldName)
				.put("value",score);
	}
	
	public JsonArray getPointsArray(int caseDisbursedScore, int tatSToDScore, int ftrScore, int pddcScore, int tatLToSScore, int weekOfMonthScore, int loginScore){
		JsonObject caseDisbursedScoreObj = createScoreObject(caseDisbursedScoreField,caseDisbursedScore);
		JsonObject tatSToDScoreObj = createScoreObject(tatSToDScoreField,tatSToDScore);
		JsonObject ftrScoreObj = createScoreObject(ftrScoreField,ftrScore);
		JsonObject pddcScoreObj = createScoreObject(pddcScoreField,pddcScore);
		JsonObject tatLToSScoreObj = createScoreObject(tatLToSScoreField,tatLToSScore);
		JsonObject weekOfMonthScoreObj = createScoreObject(weekOfMonthScoreField,weekOfMonthScore);
		JsonObject loginScoreObj = createScoreObject(loginScoreField,loginScore);
		return JsonArray.empty()
				.add(caseDisbursedScoreObj)
				.add(tatSToDScoreObj)
				.add(ftrScoreObj)
				.add(pddcScoreObj)
				.add(tatLToSScoreObj)
				.add(weekOfMonthScoreObj)
				.add(loginScoreObj);
	}
	
	public void updateUserScoreByDayPoints(String sceId, String pointsName, int points, String updateView) throws Exception{
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
		logger.debug(date);
		JsonObject userScore;		
		String userId;
		List<ViewRow> rows = result.allRows();
		JsonObject pointsObj = JsonObject.empty()
		.put("name", pointsName)
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
			JsonArray pointsArr = (JsonArray)userScore.get("points");			
			logger.debug("pointsArr "+pointsArr);			
			JsonArray updatedArr = JsonArray.empty();
			boolean enquiryFieldExists = false;
			for(Object obj:pointsArr){
				JsonObject pointObj = (JsonObject)obj;
				if(((String)pointObj.get("name")).equals(enquiryField)){
					enquiryFieldExists = true;
					updatedArr.add(createScoreObject(enquiryField,pointObj.getInt("value")+points));
				}
				else {
					updatedArr.add(pointObj);
				}
								
			}
			if(!enquiryFieldExists)
				updatedArr.add(createScoreObject(enquiryField,points));
			logger.debug("After adding scores pointsArr "+pointsArr);
			userScore.put("points", updatedArr);
			userId = user.id();				
			String json = userScore.toString();			
			StringDocument doc = StringDocument.create(userId,json);
			dbOp.upsertDoc(bucket, doc);			
		}
		logger.debug("USER ID "+userId);		
	}
	
	
	
	public void updateUserScoreByDayDoc(String sceId, JsonArray pointsArr, String updateView) throws Exception{
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
		if(rows.size()==0){			
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
			JsonArray existingPointsArr = (JsonArray)userScore.get("points");
			JsonArray updatedArr = JsonArray.empty();
			for(Object obj:existingPointsArr){
				JsonObject pointObj = (JsonObject)obj;
				if(((String)pointObj.get("name")).equals(caseDisbursedScoreField)){
					updatedArr.add(createScoreObject(caseDisbursedScoreField,pointObj.getInt("value")+getPoints(caseDisbursedScoreField, pointsArr)));
					pointsArr = deleteObjFromPointsArr(caseDisbursedScoreField,pointsArr);
				}
				else if(((String)pointObj.get("name")).equals(tatSToDScoreField)){
					updatedArr.add(createScoreObject(tatSToDScoreField,pointObj.getInt("value")+getPoints(tatSToDScoreField, pointsArr)));
					pointsArr = deleteObjFromPointsArr(tatSToDScoreField,pointsArr);
				}
				else if(((String)pointObj.get("name")).equals(ftrScoreField)){
					updatedArr.add(createScoreObject(ftrScoreField,pointObj.getInt("value")+getPoints(ftrScoreField, pointsArr)));
					pointsArr = deleteObjFromPointsArr(ftrScoreField,pointsArr);
				}
				else if(((String)pointObj.get("name")).equals(pddcScoreField)){
					updatedArr.add(createScoreObject(pddcScoreField,pointObj.getInt("value")+getPoints(pddcScoreField, pointsArr)));
					pointsArr = deleteObjFromPointsArr(pddcScoreField,pointsArr);
				}
				else if(((String)pointObj.get("name")).equals(tatLToSScoreField)){
					updatedArr.add(createScoreObject(tatLToSScoreField,pointObj.getInt("value")+getPoints(tatLToSScoreField, pointsArr)));
					pointsArr = deleteObjFromPointsArr(tatLToSScoreField,pointsArr);
				}
				else if(((String)pointObj.get("name")).equals(weekOfMonthScoreField)){
					updatedArr.add(createScoreObject(weekOfMonthScoreField,pointObj.getInt("value")+getPoints(weekOfMonthScoreField, pointsArr)));
					pointsArr = deleteObjFromPointsArr(weekOfMonthScoreField,pointsArr);
				}
				else if(((String)pointObj.get("name")).equals(loginScoreField)){
					updatedArr.add(createScoreObject(loginScoreField,pointObj.getInt("value")+getPoints(loginScoreField, pointsArr)));
					pointsArr = deleteObjFromPointsArr(loginScoreField,pointsArr);
				}	
				else if(((String)pointObj.get("name")).equals(enquiryField)){
					updatedArr.add(pointObj);
					pointsArr = deleteObjFromPointsArr(enquiryField,pointsArr);
				}
				else if(((String)pointObj.get("name")).equals(beatPlanField)){
					updatedArr.add(pointObj);
					pointsArr = deleteObjFromPointsArr(beatPlanField,pointsArr);
				}
			}
			
			if(pointsArr!=null&&(pointsArr.size()!=0||!(pointsArr.isEmpty()))){
				for(Object obj:pointsArr){
					JsonObject pointsObj = (JsonObject)obj;
					updatedArr.add(pointsObj);
				}
			}
			
			userScore.put("points", updatedArr);
			userId = user.id();			
			logger.debug("userScore "+userScore.toString());
			String json = userScore.toString();			
			StringDocument doc = StringDocument.create(userId,json);
			dbOp.upsertDoc(bucket, doc);			
		}
		logger.debug("USER ID "+userId);		
	}
	
	public int getPoints(String field,JsonArray pointsArr){
		int points=0;
		for(Object obj:pointsArr){
			JsonObject pointsObj = (JsonObject)obj;
			if(pointsObj.getString("name").equals(field)){
				points = pointsObj.getInt("value");
			}
		}
		return points;
	}
	
	public JsonArray deleteObjFromPointsArr(String field, JsonArray pointsArr){
		JsonArray updatedPointsArr = JsonArray.empty();
		for(Object obj:pointsArr){
			JsonObject pointsObj = (JsonObject)obj;
			if(!(pointsObj.getString("name").equals(field))){
				updatedPointsArr.add(pointsObj);
			}
		}
		return updatedPointsArr;
	}	
	
	public void updateUserDoc(String updateViewName, String sceId, HashMap map){
		try{
			logger.debug("sceId "+sceId);			
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
			e.printStackTrace();
		}
	}
	
	
	
	public static void main(String[] args){
		long startTime = System.currentTimeMillis();
		UserScoreByDay userScoreByDay = new UserScoreByDay();
		
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
			String viewName = prop.getProperty("CASE_DISBURSED_SCORE_VIEW");			
			String enquiryScoreView =prop.getProperty("ENQUIRY_SCORE_VIEW");
			String userViewName = prop.getProperty("USER_SCORE_BY_DAY_POINTS");
			updateViewName = prop.getProperty("USER_SCORE_BY_DAY");		
			updateUserViewName = prop.getProperty("USER_SCORE");
			
			userScoreByDay.getScoreData(docName, viewName);			
			userScoreByDay.getEnquiryScore(docName, enquiryScoreView);			
			
			long endTime = System.currentTimeMillis();
			logger.debug("Time taken " + (endTime - startTime) + " milliseconds");
			
		}
		catch(Exception e){			
			logger.error("Connection to database failed.");
		}
		
	}
}
