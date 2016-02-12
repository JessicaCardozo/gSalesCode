package dbconn1;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
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

public class CaseDisbursedData {
	public static Bucket bucket;	
	public static String configFile = File.separator+"src"+File.separator+"config.properties";
	private static Logger logger=Logger.getLogger("gSales");
	public static String docName;
	public static String servlet_url = "";
	public static String servlet_name = "DecisionMultiplier";
	public static String updateViewName;
	public static String pointsView;
	public static String scoreField = "case_disbursed_score";
	public static String password;
	public static DBOperations dbOp = null;	
	public static String ftrPointsField = "d_ftr";
	public static String weekOfMonthPointsField = "week_of_month_points";
	public static String loginPointsField = "login_points";
	public static String pointsField = "points";
	
	public int getPoints(JsonArray scoreArr, String field){
		JsonObject scoreObj = null;
		int points = 0;
		if(scoreArr.toString().contains(field))
			  for(Object obj:scoreArr){
				  logger.debug("OBJ "+obj.toString());
				  scoreObj = (JsonObject)obj;
				  if(scoreObj.getString("type").equals(field))
					  if(scoreObj.getInt(pointsField)!=null)
						  points = scoreObj.getInt(pointsField);
				  logger.debug(field+" points "+points);
			  }
		return points;
	}
	
	public void getCaseDisbursedData(String docName, String viewName) throws Exception{
		//ViewResult result = bucket.query(ViewQuery.from(docName, viewName).stale(Stale.FALSE));		
		ViewResult result = dbOp.executeQuery(bucket, docName, viewName);
		for(ViewRow row : result){			
			String proposalId = "";			
			JsonArray value;			
			int ftrPoints = 0;
			int loginPoints = 0;
			int weekOfMonthPoints = 0;
			String modifiedTS = "";			
			JsonArray scoreArr = null;
			JsonObject scoreObj = null;
			int score = 0;
			try{
				proposalId = (String)row.key();
				value = (JsonArray)row.value();	
				
				if(value.getArray(1)!=null){
					scoreArr = value.getArray(1);				
					if(!(scoreArr.isEmpty())){
						ftrPoints = getPoints(scoreArr, ftrPointsField);
						weekOfMonthPoints = getPoints(scoreArr, weekOfMonthPointsField);  
						loginPoints = getPoints(scoreArr, loginPointsField);							  
					}				
			        modifiedTS = value.getString(2);			    
					scoreArr = value.getArray(1);
					if(!(scoreArr.isEmpty())){
						score = getPoints(scoreArr, scoreField);							  
					}
				}
			    
			}
			catch(Exception e){					
				logger.error("Multiplier for proposal id "+proposalId+" could not be updated.");				
				continue;
			}
			if(scoreArr==null||scoreArr.isEmpty()||(!(scoreArr.isEmpty())&&score==0)){
				HttpURLConnection connection ;			
				try{				  
					URL url = new URL(servlet_url + servlet_name + "?caseDisbursed="+value.getString(0).toUpperCase());
					connection =  (HttpURLConnection)url.openConnection(); 
		
					connection.setRequestMethod("GET");									  
					InputStream response = connection.getInputStream();
				}
				catch(Exception e){					
					logger.error("Decision execution failed for Proposal ID "+proposalId);					
					continue;
				}
			
			
				try{
					String pointsStr = connection.getHeaderField("Points");				  
					logger.debug("pointsStr "+pointsStr);
					int multiplier = (ftrPoints+loginPoints+weekOfMonthPoints)*Integer.parseInt(pointsStr);
					if(multiplier!=0)
						updateCustomerDoc((String)row.key(), multiplier, modifiedTS);	              
				}
				catch(Exception e){						
					logger.error("Record for Proposal ID "+proposalId+" could not be updated.");					
					continue;
				}	
			}
			
		}
	}
	
	public void updateCustomerDoc(String proposalId, int multiplier, String modifiedTS) throws Exception{		
		//ViewResult result = bucket.query(ViewQuery.from(docName, updateViewName).key(proposalId).stale(Stale.FALSE));
		ViewResult result = dbOp.executeQuery(bucket, docName, updateViewName, proposalId);
		ViewRow cust = result.allRows().get(0);
		JsonObject jObj = (JsonObject) cust.value();				
		JsonArray pointsArr = jObj.getArray("score");
		JsonObject scoreObj = JsonObject.empty()
				.put("type", scoreField)
				.put("points", multiplier)
				.put("timestamp", modifiedTS)
				.put("changed_today", "Y");
		if(pointsArr==null){
			pointsArr = JsonArray.from(scoreObj);			
		}
		else{
			pointsArr.add(scoreObj);			
		}
		logger.debug("pointsArr "+pointsArr);
				
		jObj.put("score", pointsArr);
		String json = jObj.toString();
		StringDocument doc = StringDocument.create(cust.id(),json );		
		dbOp.upsertDoc(bucket, doc);		
	}
	
	public static void main(String[] args){
		long startTime = System.currentTimeMillis();
		CaseDisbursedData caseDisbursedData = new CaseDisbursedData();
		
		try{
			String currentDir = System.getProperty("user.dir");			
			FileInputStream input = new FileInputStream(currentDir+configFile);
			
			Properties prop = new Properties();
			prop.load(input);
			
			String bucketName = prop.getProperty("BUCKET");
			password = prop.getProperty("PASSWORD");
			String dbServer = prop.getProperty("DB_SERVER");
			dbOp = new DBOperations();
			
			bucket = dbOp.getBucket(dbServer, bucketName, prop);
			
			if (bucket==null){
				logger.error("DB connection failed");
				System.exit(-1);
			}
			docName = prop.getProperty("DOC_NAME");
			String viewName = prop.getProperty("MULTIPLIER_VIEW");
			updateViewName = prop.getProperty("MULTIPLIER_UPDATE_VIEW");
			pointsView = prop.getProperty("POINTS_VIEW");
			servlet_url = prop.getProperty("SERVLET_URL");
			
			caseDisbursedData.getCaseDisbursedData(docName, viewName);
			long endTime = System.currentTimeMillis();
			logger.debug("Time taken " + (endTime - startTime) + " milliseconds");
			
		}
		catch(Exception e){			
			logger.error("Connection to database failed.");			
		}
		
	}

}
