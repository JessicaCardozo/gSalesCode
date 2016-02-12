package dbconn1;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
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

public class SalesLeaderboardData {
	public static Bucket bucket;
	public static Bucket userBucket;	
	public static String configFile = File.separator+"src"+File.separator+"config.properties";
	private static Logger logger=Logger.getLogger("gSales");
	public static String docName;
	public static String servlet_url = "";
	public static String servlet_name = "DecisionSalesLeaderboard";
	public static String updateViewName;
	public static String areaView;
	public static String pointsView;
	public static String leaderboardView;
	public static String scoreCardView;
	public static String scoreField = "week_of_month_points";
	public static String password;
	public static String salesActPointsField = "sales_activity_points";
	public static String salesOutcomePointsField = "sales_outcome_points";	
	public static String tatPointsField = "tat_points";	
	public static String salesLeaderboardField = "sales_leaderboard_points";
	public static String sceIdField = "sce_id";
	public static String sceNameField = "sce_name";
	public static String imageField = "image";
	public static String badgeField = "badge";	
	public static DBOperations dbOp = null;
	
	public void getSalesLeaderboardData(String docName, String viewName) throws Exception{
		List startKeyList = new ArrayList();
		List endKeyList = new ArrayList();		
		SimpleDateFormat dateFormat = new SimpleDateFormat("MM-dd-yyyy");		
		Date currentDate = new Date();
		String currentDateStr = dateFormat.format(currentDate);
		Calendar cal = Calendar.getInstance();
		cal.setTime(currentDate);
		logger.debug("Date "+currentDateStr);
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
		
		for(ViewRow row : result){			
			String sceId = "";			
			JsonArray key;
			JsonArray value;			
			int enquiryPoints = 0;
			int pddcPoints = 0;			
			int loginPoints = 0;
			int weekOfMonthPoints = 0;
			int ftrPoints = 0;
			int caseDisbursedPoints = 0;
			int tatLtoSPoints = 0;
			int tatStoDPoints = 0;
			int beatPlanPoints = 0;					
			JsonArray scoreArr = null;
			JsonObject scoreObj = null;			
			
			try{
				key = (JsonArray)row.key();
				sceId = key.getString(1);
				value = (JsonArray)row.value();	
				
				if(value!=null){
					scoreArr = value;				
					if(!(scoreArr.isEmpty())){								  
							  for(Object obj:scoreArr){
								  logger.debug("OBJ "+obj.toString());
								  scoreObj = (JsonObject)obj;								  
								  if(scoreObj.getString("name").equals("enquiry_points"))
									  if(scoreObj.getInt("value")!=null){
										  enquiryPoints = scoreObj.getInt("value");										  
									  }
								  logger.debug("enquiry points "+loginPoints);
								  if(scoreObj.getString("name").equals("pddc_points"))
									  if(scoreObj.getInt("value")!=null){
										  pddcPoints = scoreObj.getInt("value");										  
									  }
								  if(scoreObj.getString("name").equals("login_points"))
									  if(scoreObj.getInt("value")!=null){
										  loginPoints = scoreObj.getInt("value");										  
									  }
								  if(scoreObj.getString("name").equals("week_of_month_points"))
									  if(scoreObj.getInt("value")!=null){
										  weekOfMonthPoints = scoreObj.getInt("value");										  
									  }
								  if(scoreObj.getString("name").equals("d_ftr"))
									  if(scoreObj.getInt("value")!=null){
										  ftrPoints = scoreObj.getInt("value");										  
									  }
								  if(scoreObj.getString("name").equals("case_disbursed_score"))
									  if(scoreObj.getInt("value")!=null){
										  caseDisbursedPoints = scoreObj.getInt("value");										  
									  }
								  if(scoreObj.getString("name").equals("dtat_l_to_s"))
									  if(scoreObj.getInt("value")!=null){
										  tatLtoSPoints = scoreObj.getInt("value");										  
									  }
								  if(scoreObj.getString("name").equals("dtat_s_to_d"))
									  if(scoreObj.getInt("value")!=null){
										  tatStoDPoints = scoreObj.getInt("value");										  
									  }
								  if(scoreObj.getString("name").equals("beat_plan_points"))
									  if(scoreObj.getInt("value")!=null){
										  beatPlanPoints = scoreObj.getInt("value");										  
									  }
								  
							  }									  
					}				
			        
				}
			    
			}
			catch(Exception e){					
				logger.error("User scores for sce id "+sceId+" could not be obtained.");
				continue;
			}
			
			HttpURLConnection connection ;			
			try{				  
				URL url = new URL(servlet_url + servlet_name+"?scoreArr="+scoreArr);				
				connection =  (HttpURLConnection)url.openConnection(); 		
				connection.setRequestMethod("GET");							  
				InputStream response = connection.getInputStream();
			}
			catch(Exception e){				
				logger.error("Decision execution failed for SCE ID "+sceId);
				continue;
			}
			
			
			try{
				double salesActPoints = Double.parseDouble(connection.getHeaderField("SalesActivityPoints"));
				double salesOutcomePoints = Double.parseDouble(connection.getHeaderField("SalesOutcomePoints"));	
				double tatPoints = Double.parseDouble(connection.getHeaderField("TATPoints"));	
				double salesLeaderboardPoints = Double.parseDouble(connection.getHeaderField("SalesLeaderboardPoints"));	
				logger.debug("salesActPoints "+salesActPoints);
				logger.debug("salesOutcomePoints "+salesOutcomePoints);
				logger.debug("tatPointsStr "+tatPoints);
				logger.debug("salesLeaderboardStr "+salesLeaderboardPoints);				
				
				updateUserDoc(sceId,salesActPoints,salesOutcomePoints,tatPoints,salesLeaderboardPoints);
					              
			}
			catch(Exception e){				
				logger.error(e.getMessage());
				logger.error("Record for SCE ID "+sceId+" could not be updated.");
				continue;
			}
			
		}
	}
	
	public void updateUserDoc(String sceId, Double salesActPoints,Double salesOutcomePoints,Double tatPoints,Double salesLeaderboardPoints){
		try{
			logger.debug("sceId "+sceId);
			logger.debug("updateViewName "+updateViewName);
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
			if(value.getArray("leaderboard_points")!=null)
				pointsArr = value.getArray("leaderboard_points");						
			
			JsonObject salesActPointsObj = null;
			JsonObject salesOutcomePointsObj = null;
			JsonObject tatPointsObj = null;
			JsonObject leaderboardPointObj = null;
			
			JsonArray leaderboardPointsArr = JsonArray.empty();
			if(pointsArr==null||pointsArr.size()==0){		
				logger.debug("in if");
				salesActPointsObj = JsonObject.empty()
							.put("name",salesActPointsField)
							.put("value", ""+salesActPoints);
				salesOutcomePointsObj = JsonObject.empty()
						.put("name",salesOutcomePointsField)
						.put("value", ""+salesOutcomePoints);
				tatPointsObj = JsonObject.empty()
						.put("name",tatPointsField)
						.put("value", ""+tatPoints);
				leaderboardPointObj = JsonObject.empty()
						.put("name",salesLeaderboardField)
						.put("value", ""+salesLeaderboardPoints);
								
			}
			else{
				logger.debug("in else");
				
				for(Object obj:pointsArr){
					JsonObject pointObj = (JsonObject)obj;
					
					if(((String)pointObj.get("name")).equals(salesActPointsField)){
						
						salesActPointsObj = JsonObject.empty()
								.put("name",salesActPointsField)
								.put("value", ""+(Double.parseDouble((String)pointObj.get("value"))+salesActPoints));
					}
					else if(((String)pointObj.get("name")).equals(salesOutcomePointsField)){
						
						salesOutcomePointsObj = JsonObject.empty()
								.put("name",salesOutcomePointsField)
								.put("value",  ""+(Double.parseDouble((String)pointObj.get("value"))+salesOutcomePoints));
					}
					else if(((String)pointObj.get("name")).equals(tatPointsField)){
						
						tatPointsObj = JsonObject.empty()
								.put("name",tatPointsField)
								.put("value", ""+ (Double.parseDouble((String)pointObj.get("value"))+tatPoints));
					}
					else if(((String)pointObj.get("name")).equals(salesLeaderboardField)){
	
						leaderboardPointObj = JsonObject.empty()
								.put("name",salesLeaderboardField)
								.put("value",  ""+(Double.parseDouble((String)pointObj.get("value"))+salesLeaderboardPoints));
					}
					
				}
				
			}
			leaderboardPointsArr.add(salesActPointsObj)
								.add(salesOutcomePointsObj)
								.add(tatPointsObj)
								.add(leaderboardPointObj);
			
			JsonObject jObj = (JsonObject) row.value();
			jObj.put("leaderboard_points",leaderboardPointsArr);				
			String json = jObj.toString();			
			StringDocument doc = StringDocument.create(row.id(),json);	
			dbOp.upsertDoc(userBucket, doc);		
		}
		catch(Exception e){					
			logger.error("Points for the user with SCE ID "+sceId+" could not be updated.");
		}
	}
	
	public List getAreaId(String docName, String areaView){
		List areaList = new ArrayList();
		try{			
			//ViewResult result = userBucket.query(ViewQuery.from(docName, areaView).groupLevel(1).stale(Stale.FALSE));	
			ViewResult result = dbOp.executeQuery(userBucket, docName, 1, areaView);
			
			for(ViewRow row : result){
				if(row.key()!=null)
					areaList.add((String)row.key());				
			}
			logger.debug("areaList "+areaList);
			
		}
		catch(Exception e){				
			logger.error("Area ID could not be obtained.");
		}
		return areaList;
	}
	
	public void updateLeaderboardDoc(String docName, String leaderboardView, String area){
		String scoreCardId = "";
		try{				
			//ViewResult result = userBucket.query(ViewQuery.from(docName, leaderboardView).key(area).stale(Stale.FALSE));
			ViewResult result = dbOp.executeQuery(userBucket, docName, leaderboardView, area);
			String areaId = "";
			List userList = new ArrayList();
			
			for(ViewRow row : result){
				
				try{
					areaId=(String)row.key();
					JsonArray value = null;
					if(row.value()!=null)
						value= (JsonArray)row.value();
					int sceId = value.getInt(0);
					String firstName = value.getString(2);
					String image = value.getString(3);
					JsonArray leaderboardPointsArr = null;
					if(value.getArray(1)!=null){
						leaderboardPointsArr = value.getArray(1);
						User user = new User();
						user.setSceId(Integer.toString(sceId));
						user.setFirstName(firstName);
						user.setImage(image);
						for(Object obj:leaderboardPointsArr){
							JsonObject leaderboardPointsObj = (JsonObject)obj;
							if(leaderboardPointsObj.getString("name").equals("sales_leaderboard_points")){
								user.setSalesLeaderboardPoints(Double.parseDouble(leaderboardPointsObj.getString("value")));
							}
							else if(leaderboardPointsObj.getString("name").equals("sales_activity_points")){
								user.setSalesActPoints(Double.parseDouble(leaderboardPointsObj.getString("value")));
							}
							else if(leaderboardPointsObj.getString("name").equals("sales_outcome_points")){
								user.setSalesOutcomePoints(Double.parseDouble(leaderboardPointsObj.getString("value")));
							}
							else if(leaderboardPointsObj.getString("name").equals("tat_points")){
								user.setTatPoints(Double.parseDouble(leaderboardPointsObj.getString("value")));
							}
						}
						userList.add(user);
					}
				}
				catch(Exception e){					
					logger.error("Area ID could not be obtained.");
					continue;
				}
				
			}			
			Iterator iterator1 = userList.iterator();
		    while(iterator1.hasNext()){
		        User user = (User) iterator1.next();
		        logger.debug(user.getSalesLeaderboardPoints()+":"+user.getSceId()+" ");
		    }
		    Collections.sort(userList,new User());
			Collections.reverse(userList);
		    
			logger.debug("userList after sorting "+userList);
		    iterator1 = userList.iterator();
		    while(iterator1.hasNext()){
		        User user = (User) iterator1.next();
		        logger.debug(user.getSalesLeaderboardPoints()+":"+user.getSceId()+" ");
		    }
		    //ViewResult result1 = bucket.query(ViewQuery.from(docName, scoreCardView).key("scorecard_"+area).stale(Stale.FALSE));
		    System.out.println("scorecard_"+area);
		    ViewResult result1 = dbOp.executeQuery(bucket, docName, scoreCardView, "scorecard_"+area);
		    List rows = result1.allRows();
		    
		    JsonArray userArr = JsonArray.empty();
		    int cnt = 0;
		    for(Object obj : userList){
		    	User user = (User)obj;
		    	JsonObject userObj = JsonObject.empty()
		    			.put(sceIdField, user.getSceId())
		    			.put(sceNameField, user.getFirstName())
		    			.put(imageField, user.getImage())
		    			.put(badgeField,"")
		    			.put(salesActPointsField, user.getSalesActPoints())
		    			.put(salesOutcomePointsField, user.getSalesOutcomePoints())
		    			.put(tatPointsField, user.getTatPoints())
		    			.put(salesLeaderboardField, user.getSalesLeaderboardPoints());
		    			
		    	userArr.add(userObj);
		    	if(cnt==9)
					break;
		    	cnt++;
		    }
		    JsonObject scoreObj = JsonObject.empty();
		    JsonArray sectionArr = JsonArray.empty();
			JsonObject sectionObj = JsonObject.empty()
					.put("users", userArr);
			sectionArr.add(sectionObj);
			JsonObject valueObj = JsonObject.empty()
					.put("section", sectionArr);
			if(rows.size()==0){
				
				scoreObj = JsonObject.empty()
						  .put("type", "scorecard")
						  .put("channels", "ALL")
						  .put("value", valueObj);
						  
				scoreCardId = "scorecard" + "_" + areaId;
											
				String json = scoreObj.toString();				
				StringDocument doc = StringDocument.create(scoreCardId,json);
				dbOp.insertDoc(bucket, doc);				
			}
			else{
				ViewRow scorecard = (ViewRow)rows.get(0);
				scoreObj = (JsonObject)scorecard.value();					
				if(scoreObj.getObject("value")!=null){
					valueObj = scoreObj.getObject("value");
					JsonArray updatedSectionArr = JsonArray.empty();
					sectionArr = valueObj.getArray("section");
					if(sectionArr.getObject(0)!=null){
						sectionObj = sectionArr.getObject(0);
						sectionObj.put("users", userArr);
					}
					updatedSectionArr.add(sectionObj);
					valueObj.put("section", updatedSectionArr);
					scoreObj.put("value", valueObj);
					
				}

				scoreCardId = scorecard.id();				
				logger.debug("userScore "+scoreObj.toString());
				String json = scoreObj.toString();				
				StringDocument doc = StringDocument.create(scoreCardId,json);
				dbOp.upsertDoc(bucket, doc);				
			}
			logger.debug("SCORE CARD ID "+scoreCardId);	
		}
		catch(Exception e){	
			e.printStackTrace();
			logger.error("Leaderboard document could not be updated for "+scoreCardId);
		}
	}
	
	public static void main(String[] args){
		long startTime = System.currentTimeMillis();
		SalesLeaderboardData salesLeaderboardData = new SalesLeaderboardData();
		
		try{
			String currentDir = System.getProperty("user.dir");			
			FileInputStream input = new FileInputStream(currentDir+configFile);
			
			Properties prop = new Properties();
			prop.load(input);
			
			String bucketName = prop.getProperty("BUCKET");
			String userBucketName = prop.getProperty("USER_BUCKET");
			String dbServer = prop.getProperty("DB_SERVER");
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
			password = prop.getProperty("PASSWORD");
			String viewName = prop.getProperty("USER_SCORE_BY_DAY_POINTS");	
			updateViewName = prop.getProperty("SALESLEADERBOARD_VIEW");	
			areaView = prop.getProperty("AREA_VIEW");
			leaderboardView = prop.getProperty("USER_LEADERBOARD");
			scoreCardView = prop.getProperty("SCORECARD_VIEW");
			servlet_url = prop.getProperty("SERVLET_URL");
			
			salesLeaderboardData.getSalesLeaderboardData(docName, viewName);
			List areaList = salesLeaderboardData.getAreaId(docName, areaView);
			for(Object areaObj:areaList){
				String area = (String)areaObj;
				if(!(area.equals(""))){
					salesLeaderboardData.updateLeaderboardDoc(docName, leaderboardView, area);
				}
			}
			long endTime = System.currentTimeMillis();
			logger.debug("Time taken " + (endTime - startTime) + " milliseconds");
			
		}
		catch(Exception e){			
			logger.error("Connection to database failed.");
		}
		
	}
}
