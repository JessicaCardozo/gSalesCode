package dbconn1;

import java.io.File;
import java.io.FileInputStream;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import java.util.Properties;
import java.util.Set;

import org.apache.log4j.Logger;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonArrayDocument;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.JsonStringDocument;
import com.couchbase.client.java.document.StringDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.view.Stale;
import com.couchbase.client.java.view.ViewQuery;
import com.couchbase.client.java.view.ViewResult;
import com.couchbase.client.java.view.ViewRow;
import com.google.gson.Gson;
import com.couchbase.client.java.document.*;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;

public class UserScore {
	public static Bucket bucket;
	public static Bucket userBucket;	
	public static String configFile = File.separator+"src"+File.separator+"config.properties";
	private static Logger logger=Logger.getLogger("gSales");
	public static String password;
	public static String docName;
	public static String updateViewName;
	public static String caseDisbursedScoreField = "case_disbursed_score";
	public static DBOperations dbOp = null;	
	
	public void getUserScoreByDayPoints(String docName, String viewName){
		String sceId = "";
		try{
			List startKeyList = new ArrayList();
			List endKeyList = new ArrayList();		
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
			
			//ViewResult result = bucket.query(ViewQuery.from(docName, viewName).startKey(JsonArray.from(startKeyList)).endKey(JsonArray.from(endKeyList)).stale(Stale.FALSE));
			ViewResult result = dbOp.executeQuery(bucket, docName, viewName, JsonArray.from(startKeyList), JsonArray.from(endKeyList));
			
			JsonArray key = null;
			JsonArray value = null;
			//result
			HashMap map = null;			
			for(ViewRow row : result){			
				key = (JsonArray)row.key();
				value = (JsonArray)row.value();
				sceId = key.getString(1);				
				map = new HashMap();
				for(Object obj:value){
					JsonObject pointsObj = (JsonObject)obj;					
					map.put(pointsObj.getString("name"), pointsObj.getInt("value"));				
				}				
				updateUserDoc(updateViewName, sceId, map);
			}
		}
		catch(Exception e){			
			logger.error("Score for User with SCE ID "+sceId+" could not be updated."+e.getMessage());
		}
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
		}
	}
	
	public static void main(String[] args){
		long startTime = System.currentTimeMillis();
		UserScore userScore = new UserScore();
		
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
			String viewName = prop.getProperty("USER_SCORE_BY_DAY_POINTS");			
			updateViewName = prop.getProperty("USER_SCORE");			
			
			userScore.getUserScoreByDayPoints(docName, viewName);	
			long endTime = System.currentTimeMillis();
			logger.debug("Time taken " + (endTime - startTime) + " milliseconds");
			
		}
		catch(Exception e){			
			logger.error("Connection to database failed.");
		}
		
	}
}
