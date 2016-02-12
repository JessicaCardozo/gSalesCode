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

public class WeekOfMonthData {
	public static Bucket bucket;	
	public static String configFile = File.separator+"src"+File.separator+"config.properties";
	private static Logger logger=Logger.getLogger("gSales");
	public static String docName;
	public static String servlet_url = "";
	public static String servlet_name = "DecisionWeekOfMonth";
	public static String updateViewName;
	public static String pointsView;
	public static String scoreField = "week_of_month_points";
	public static String password;
	public static DBOperations dbOp = null;
	
	public void getWeekOfMonthData(String docName, String viewName) throws Exception{
		//ViewResult result = bucket.query(ViewQuery.from(docName, viewName).stale(Stale.FALSE));	
		ViewResult result = dbOp.executeQuery(bucket, docName, viewName);
		
		for(ViewRow row : result){			
			String proposalId = "";	
			JsonArray value;
			JsonObject weekOfMonthScore = null;
			String modifiedTS = "";
			String loginBeforeTenth = "";
			int loginDay = 0;
			JsonArray scoreArr = null;
			JsonObject scoreObj = null;
			int score = 0;
			try{
				proposalId = (String)row.key();
				value = (JsonArray)row.value();
				loginBeforeTenth = (String)value.get(0);
				logger.debug("loginBeforeTenth "+loginBeforeTenth);
				//added since login before 10th is a flag
				if(loginBeforeTenth.equalsIgnoreCase("Y")){
					loginDay = 1;
				}
				else{
					loginDay = 11;
				}
				
				if(value.getArray(1)!=null){
					  scoreArr = value.getArray(1);
					  if(!(scoreArr.isEmpty())){
						  if(scoreArr.toString().contains(scoreField))
							  for(Object obj:scoreArr){
								  logger.debug("OBJ "+obj.toString());
								  scoreObj = (JsonObject)obj;
								  if(scoreObj.getString("type").equals(scoreField))
									  if(scoreObj.getInt("points")!=null)
										  score = scoreObj.getInt("points");
								  logger.debug("score "+score);
							  }
							  
					  }
				}
				modifiedTS = value.getString(2);
			}
			catch(Exception e){				
				logger.error("Week of Month Points for proposal id "+proposalId+" could not be updated.");				
				continue;
			}
			if(scoreArr==null||scoreArr.isEmpty()||(!(scoreArr.isEmpty())&&score==0)){
				HttpURLConnection connection ;			
				try{				  
					URL url = new URL(servlet_url + servlet_name + "?loginDay="+loginDay);
					connection =  (HttpURLConnection)url.openConnection(); 
		
					connection.setRequestMethod("GET");									  
					InputStream response = connection.getInputStream();
				}
				catch(Exception e){
					e.printStackTrace();
					logger.error("Decision execution failed for Proposal ID "+proposalId);					
					continue;
				}
				
				try{
					String pointsStr = connection.getHeaderField("Points");				  
					logger.debug("pointsSTr "+pointsStr);	
					int points = Integer.parseInt(pointsStr);
					if(points!=0)
						updateCustomerDoc(proposalId, points, weekOfMonthScore, modifiedTS);	              
				}
				catch(Exception e){
					e.printStackTrace();
					logger.error(e.getMessage());
					logger.error("Record for Proposal ID "+proposalId+" could not be updated.");					
					continue;
				  }	
			}
			
		}
	}
	
	public void updateCustomerDoc(String proposalId, int points, JsonObject weekOfMonthScore, String modifiedTS) throws Exception{		
		//ViewResult result = bucket.query(ViewQuery.from(docName, updateViewName).key(proposalId).stale(Stale.FALSE));
		ViewResult result = dbOp.executeQuery(bucket, docName, updateViewName, proposalId);
		ViewRow cust = result.allRows().get(0);
		JsonDocument storedCust  = JsonDocument.create(cust.id(),(JsonObject)cust.value());
		
		JsonArray pointsArr = storedCust.content().getArray("score");
		JsonObject scoreObj = JsonObject.empty()
				.put("type", scoreField)
				.put("points", points)
				.put("timestamp", modifiedTS)
				.put("changed_today", "Y");
		if(pointsArr==null){
			pointsArr = JsonArray.from(scoreObj);			
		}
		else{
			pointsArr.add(scoreObj);			
		}				
		logger.debug("pointsArr "+pointsArr);
		JsonObject jObj = (JsonObject) cust.value();
		jObj.put("score", pointsArr);			
		String json = jObj.toString();		
		StringDocument doc = StringDocument.create(cust.id(),json);
		dbOp.upsertDoc(bucket, doc);		
	}
	
	public static void main(String[] args){
		long startTime = System.currentTimeMillis();
		WeekOfMonthData weekOfMonthData = new WeekOfMonthData();
		
		try{
			String currentDir = System.getProperty("user.dir");			
			FileInputStream input = new FileInputStream(currentDir+configFile);
			
			Properties prop = new Properties();
			prop.load(input);
			
			String bucketName = prop.getProperty("BUCKET");
			String dbServer = prop.getProperty("DB_SERVER");
			password = prop.getProperty("PASSWORD");
			dbOp = new DBOperations();
			bucket = dbOp.getBucket(dbServer, bucketName, prop);			
			
			if (bucket==null){
				logger.error("DB connection failed");
				System.exit(-1);
			}
			docName = prop.getProperty("DOC_NAME");
			String viewName = prop.getProperty("WEEK_OF_MONTH_VIEW");
			updateViewName = prop.getProperty("WEEK_OF_MONTH_UPDATE_VIEW");
			servlet_url = prop.getProperty("SERVLET_URL");
			
			weekOfMonthData.getWeekOfMonthData(docName, viewName);
			long endTime = System.currentTimeMillis();
			logger.debug("Time taken " + (endTime - startTime) + " milliseconds");
			
		}
		catch(Exception e){			
			logger.error("Connection to database failed.");			
		}
		
	}
}
