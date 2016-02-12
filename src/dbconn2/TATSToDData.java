package dbconn2;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.io.ObjectInputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.net.URLEncoder;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
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

public class TATSToDData {
	public static Bucket bucket;
	private static Logger logger=Logger.getLogger("gSales"); 	
	public static String configFile = File.separator+"src"+File.separator+"config.properties";
	public static String servlet_url = "";
	public static String servlet_name = "DecisionTATSToD";
	public static String updateViewName;
	public static String docName;
	public static String scoreField = "dtat_s_to_d";
	public static String password;
	public static DBOperations dbOp = null;	
	public static ErrorLog errorLog = null;
	public static String errorMsg = "";
		
	public void getTATSToDData(String docName, String viewName) throws Exception{		
		//ViewResult result = bucket.query(ViewQuery.from(docName, viewName).stale(Stale.FALSE));
		ViewResult result = dbOp.executeQuery(bucket, docName, viewName);
		for(ViewRow row : result){
			  String key = (String)row.key();
			  logger.debug("Key "+key);
			  JsonArray value;
			  double tatSToD = 0;				  
			  String modifiedTS = "";
			  int score = 0;
			  JsonArray scoreArr = null;
			  JsonObject scoreObj = null;
			  String tatSToDStr = null;
			  try{
				  value = (JsonArray)row.value();
				  tatSToDStr = (String)value.get(0);
				  if(!tatSToDStr.equals(""))
					  tatSToD = Double.parseDouble((String)value.get(0));				  
				  modifiedTS = value.getString(2);
				  logger.debug("tatSToD "+tatSToD);				  
				  
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
			  }
			  catch(Exception e){				  
				  logger.error("Record for Proposal ID "+key+" could not be updated.");
				  errorMsg = "Record for Proposal ID "+key+" could not be updated.";			
				  errorLog.writeError(bucket, "customer", "TAT S To D Data", errorMsg);
				  continue;
			  }
			  if(!tatSToDStr.equals("")){
				  if(scoreArr==null||scoreArr.isEmpty()||(!(scoreArr.isEmpty())&&score==0)){
					  HttpURLConnection connection ;
					  logger.debug("in if");
					  try{				  
						  URL url = new URL(servlet_url + servlet_name + "?TATStoD="+tatSToD);
						  connection =  (HttpURLConnection)url.openConnection(); 
			
						  connection.setRequestMethod("GET");					  			  
						  InputStream response = connection.getInputStream();
					  }
					  catch(Exception e){						  
						  logger.error("Decision execution failed for Proposal ID "+key);
						  errorMsg = "Decision execution failed for Proposal ID "+key;			
						  errorLog.writeError(bucket, "customer", "TAT S To D Data", errorMsg);
						  continue;
					  }
					  try{
						  String pointsStr = connection.getHeaderField("Points");	
						  int points = Integer.parseInt(pointsStr);
						  if(points!=0)
							  updateCustomerDoc(key,points,modifiedTS);	              
					  }
					  catch(Exception e){						  
						  logger.error(e.getMessage());
						  logger.error("Record for Proposal ID "+key+" could not be updated.");
						  errorMsg = "Record for Proposal ID "+key+" could not be updated.";			
						  errorLog.writeError(bucket, "customer", "TAT S To D Data", errorMsg);
						  continue;
					  }			  
				  }
			  }
		}
		
	}
	
	public void updateCustomerDoc(String proposalId, int value, String modifiedTS) throws Exception{		
		//ViewResult result = bucket.query(ViewQuery.from(docName, updateViewName).key(proposalId).stale(Stale.FALSE));
		ViewResult result = dbOp.executeQuery(bucket, docName, updateViewName, proposalId);
		ViewRow cust = result.allRows().get(0);
		JsonObject jObj = (JsonObject) cust.value();		
		
		JsonArray pointsArr = jObj.getArray("score");
		JsonObject scoreObj = JsonObject.empty()
				.put("type", scoreField)
				.put("points", value)
				.put("timestamp", modifiedTS)
				.put("changed_today", "Y");
		if(pointsArr==null){
			pointsArr = JsonArray.from(scoreObj);			
		}
		else{
			pointsArr.add(scoreObj);			
		}	
		logger.debug("pointsArr "+pointsArr);
		logger.debug("id "+cust.id());		
		
		jObj.put("score", pointsArr);			
		String json = jObj.toString();
		StringDocument doc = StringDocument.create(cust.id(),json);	
		dbOp.upsertDoc(bucket, doc);		
	}
	
	public static void main(String[] args){
		long startTime = System.currentTimeMillis();
		TATSToDData tatStoDData = new TATSToDData();
		
		try{			
			String currentDir = System.getProperty("user.dir");			
			FileInputStream input = new FileInputStream(currentDir+configFile);
		
			Properties prop = new Properties();
			prop.load(input);
			
			String dbServer = prop.getProperty("DB_SERVER");
			password = prop.getProperty("PASSWORD");
			logger.debug("dbServer "+dbServer);
			String bucketName = prop.getProperty("BUCKET");
			dbOp = new DBOperations();
			errorLog = new ErrorLog();
			bucket = dbOp.getBucket(dbServer, bucketName, prop);
			
			if (bucket==null){
				logger.error("DB connection failed");
				System.exit(-1);
			}
			errorLog.getErrorDoc(bucket);
			docName = prop.getProperty("DOC_NAME");
			String viewName = prop.getProperty("TAT_VIEW_NAME");
			updateViewName = prop.getProperty("TAT_UPDATE_VIEW_NAME");
			servlet_url = prop.getProperty("SERVLET_URL");
			
			tatStoDData.getTATSToDData(docName, viewName);			
			long endTime = System.currentTimeMillis();
			logger.debug("Time taken " + (endTime - startTime) + " milliseconds");
		}
		catch(Exception e){
			e.printStackTrace();
			logger.error("Connection to database failed.");
			errorMsg = "Connection to database failed.";			
			errorLog.writeError(bucket, "customer", "TAT S To D Data", errorMsg);
		}
		
	}
}
