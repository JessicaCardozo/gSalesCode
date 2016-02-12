package dbconn2;

import java.io.File;
import java.io.FileInputStream;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Properties;

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

public class FlagData {
	public static Bucket bucket;	
	public static String configFile = File.separator+"src"+File.separator+"config.properties";
	private static Logger logger=Logger.getLogger("gSales"); 
	public static String password;
	public static String docName;
	public static String updateViewName;
	public static String scoreField = "score";
	public static String processedField = "processed";
	public static String channelCoverageField = "channel_coverage";
	public static DBOperations dbOp = null;	
	public static ErrorLog errorLog = null;
	public static String errorMsg = "";
	
	public void getFlagData(String docName, String viewName){
		try{
			//ViewResult result = bucket.query(ViewQuery.from(docName, viewName).stale(Stale.FALSE));		
			ViewResult result = dbOp.executeQuery(bucket, docName, viewName);
			for(ViewRow row : result){
				JsonObject docObj = (JsonObject)row.value();				
				if(docObj.getString("type").equalsIgnoreCase("customer")){
					updateFlags(row);
				}
				else if(docObj.getString("type").equalsIgnoreCase("coverage")){
					updateBeatPlanFlags(row);
				}
				else if(docObj.getString("type").equalsIgnoreCase("enquiry")){
					updateEnquiryFlags(row);
				}					
			}
		}
		catch(Exception e){			
			logger.error("Connection to database failed.");
			errorMsg = "Connection to database failed.";			
			errorLog.writeError(bucket, "customer", "Reset Flag", errorMsg);
		}
	}
	
	public void updateFlags(ViewRow row) throws Exception{
		JsonObject docObj = (JsonObject)row.value();			
		JsonArray scoreArr = docObj.getArray(scoreField);			
		JsonArray updatedScoreArr = JsonArray.empty();
		for(Object obj:scoreArr){
			JsonObject scoreObj = (JsonObject)obj;
			if(((String)scoreObj.get("changed_today")).equals("Y")){
				scoreObj.put("changed_today","N");										
			}
			updatedScoreArr.add(scoreObj);
		}		
		
		JsonObject jObj = (JsonObject) row.value();
		jObj.put(processedField, "Y");		
		jObj.put(scoreField, updatedScoreArr);
		String json = jObj.toString();
		StringDocument doc = StringDocument.create(row.id(),json);	
		dbOp.upsertDoc(bucket, doc);		
	}
	
	public void updateEnquiryFlags(ViewRow enquiry) throws Exception{
		JsonObject docObj = (JsonObject)enquiry.value();				
		JsonObject scoreObj = docObj.getObject(scoreField);				
		
		if(((String)scoreObj.get("changed_today")).equals("Y")){
			scoreObj.put("changed_today","N");										
		}				
		
		JsonObject jObj = (JsonObject) enquiry.value();
		jObj.put(processedField, "Y");		
		jObj.put(scoreField, scoreObj);
		String json = jObj.toString();
		StringDocument doc = StringDocument.create(enquiry.id(),json);		
		dbOp.upsertDoc(bucket, doc);
	}
	
	public void updateBeatPlanFlags(ViewRow row) throws Exception{			
		String sceCode = (String)row.key();
		logger.debug("ROW ID "+row.id());
		Date date = new Date();		
		Calendar cal = Calendar.getInstance();
		cal.setMinimalDaysInFirstWeek(4);
		cal.setFirstDayOfWeek(Calendar.MONDAY);
		cal.setTime(date);
		int currentWeek = cal.get(Calendar.WEEK_OF_YEAR);
		int previousWeek = cal.get(Calendar.WEEK_OF_YEAR)-1;
		int currentYear = cal.get(Calendar.YEAR);
		int currentMonth = cal.get(Calendar.MONTH);
		
		if(currentWeek==1){
			if(currentMonth==0){
				currentYear = currentYear-1;
			}
			cal.set(currentYear,12,31);			
			previousWeek=cal.getMaximum(Calendar.WEEK_OF_YEAR);
		}
		JsonObject docObj = (JsonObject)row.value();
		JsonArray channelCoverageArr = docObj.getArray(channelCoverageField);
		JsonArray updatedChannelCoverageArr = JsonArray.empty();
		JsonObject updatedYearObj = JsonObject.empty();
		for(Object obj:channelCoverageArr){
			updatedYearObj = JsonObject.empty();
			JsonObject yearObj = (JsonObject)obj;
			
			int year = yearObj.getInt("year");
			JsonArray months = yearObj.getArray("months");
			JsonArray updatedMonths = JsonArray.empty();
			if(currentYear==year){
				  List monthList = months.toList();
				  JsonArray updatedMonthList = JsonArray.empty();
				  HashMap monthObj=null;
				  for(Object month:monthList){					  
					  monthObj = (HashMap)month;					  					  
					  int monthVal = (Integer)monthObj.get("month");
					  logger.debug(" Month "+monthVal);
					  List weekArray = (ArrayList)monthObj.get("weeks");
					  List updatedWeekArray = new ArrayList();
					 
					  int cnt=0;
					  HashMap weekObj = null;
					  for(Object week:weekArray){
						  try{
							  weekObj = (HashMap)week;
							  int weekNo = (Integer)weekObj.get("week_no");
							  String beatPlanValid = (String)weekObj.get("beat_plan_valid");
							  logger.debug("Week No "+weekNo);
							  logger.debug("PreviousWeek "+previousWeek);
							  if(weekNo==previousWeek&&beatPlanValid.equals("Y")){
								  logger.debug("beat_plan_points "+weekObj.get("beat_plan_points"));
								  HashMap beatPlanPoints =(HashMap)weekObj.get("beat_plan_points");
								  beatPlanPoints.put("changed_today","N");									  
								  weekObj.put("beat_plan_points", beatPlanPoints);								  
							  }
							  
						  }					  
						  catch(Exception e){							  
							  logger.error("Beat plan flag for SCE code "+sceCode+" has not been updated "+e.getMessage());
							  errorMsg = "Beat plan flag for SCE code "+sceCode+" has not been updated "+e.getMessage();			
							  errorLog.writeError(bucket, "customer", "Reset Flag", errorMsg);
						  }
						  updatedWeekArray.add(weekObj);
						  cnt++;
					  }					  
					  monthObj.put("weeks", updatedWeekArray);
					  logger.debug("After modifying flags Month OBJ "+monthObj);
					  updatedMonths.add(monthObj);
					  
				  }
				  
			}
			else{
				  updatedMonths = months;
			}
			updatedYearObj.put("year", year);
			updatedYearObj.put("months", updatedMonths);
			logger.debug("updatedYearObj "+updatedYearObj);			
			updatedChannelCoverageArr.add(updatedYearObj);
			logger.debug("after adding to updatedChannelCoverageArr "+updatedChannelCoverageArr);
		}		
		logger.debug("updatedChannelCoverageArr "+updatedChannelCoverageArr);		
		JsonObject jObj = (JsonObject) row.value();
		jObj.put(channelCoverageField,updatedChannelCoverageArr);		
		jObj.put(processedField,"Y");
		String json = jObj.toString();
		StringDocument doc = StringDocument.create(row.id(),json);		
		dbOp.upsertDoc(bucket, doc);
	}
	
	public static void main(String[] args){
		long startTime = System.currentTimeMillis();
		FlagData flagData = new FlagData();
		
		try{
			String currentDir = System.getProperty("user.dir");			
			FileInputStream input = new FileInputStream(currentDir+configFile);
			
			Properties prop = new Properties();
			prop.load(input);
			
			String bucketName = prop.getProperty("BUCKET");
			String dbServer = prop.getProperty("DB_SERVER");
			password = prop.getProperty("PASSWORD");
			dbOp = new DBOperations();
			errorLog = new ErrorLog();
			bucket = dbOp.getBucket(dbServer, bucketName, prop);
			
			if (bucket==null){
				logger.error("DB connection failed");
				System.exit(-1);
			}
			errorLog.getErrorDoc(bucket);
			docName = prop.getProperty("DOC_NAME");
			String viewName = prop.getProperty("FLAG_DATA");			
			updateViewName = prop.getProperty("USER_SCORE");			
			
			flagData.getFlagData(docName, viewName);	
			long endTime = System.currentTimeMillis();
			logger.debug("Time taken " + (endTime - startTime) + " milliseconds");
			
		}
		catch(Exception e){			
			logger.error("Connection to database failed.");
			errorMsg = "Connection to database failed.";			
			errorLog.writeError(bucket, "customer", "Reset Flag", errorMsg);
		}
		
	}

}
