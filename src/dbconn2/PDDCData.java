package dbconn2;

import java.io.File;
import java.io.FileInputStream;
import java.io.InputStream;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
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

public class PDDCData {
	public static Bucket bucket;	
	public static String configFile = File.separator+"src"+File.separator+"config.properties";
	private static Logger logger=Logger.getLogger("gSales");
	public static String docName;
	public static String servlet_url = "";
	public static String servlet_name = "DecisionPDDC";
	public static String updateViewName;
	public static String pddcPointsField = "pddc_points";
	public static String password;
	public static DBOperations dbOp = null;	
	public static ErrorLog errorLog = null;
	public static String errorMsg = "";
	
	public void getPDDCData(String docName, String viewName) throws Exception{
		//ViewResult result = bucket.query(ViewQuery.from(docName, viewName).stale(Stale.FALSE));		
		ViewResult result = dbOp.executeQuery(bucket, docName, viewName);
		
		for(ViewRow row : result){			
			JsonArray value;
			String proposalId = "";
			double pddc = 0;
			String modifiedTS = "";
			JsonObject pddcScore = null;
			JsonArray scoreArr = null;
			JsonObject scoreObj = null;
			int score = 0;
			String pddcStr = "";
			try{
				proposalId = (String)row.key();
			    value = (JsonArray)row.value();
			    pddcStr = (String)value.get(0);
			    if(!pddcStr.equals(""))
			    	pddc = Double.parseDouble(pddcStr);			    
			    modifiedTS = value.getString(2);
			    logger.debug("value "+value);
			    
			    if(value.getArray(1)!=null){
					  scoreArr = value.getArray(1);
					  if(!(scoreArr.isEmpty())){
						  if(scoreArr.toString().contains(pddcPointsField))
							  for(Object obj:scoreArr){
								  logger.debug("OBJ "+obj.toString());
								  scoreObj = (JsonObject)obj;
								  if(scoreObj.getString("type").equals(pddcPointsField))
									  if(scoreObj.getInt("points")!=null)
										  score = scoreObj.getInt("points");
								  logger.debug("score "+score);
							  }
							  
					  }
				  }
			  
			}
			catch(Exception e){				
				logger.error("Record for Proposal ID "+proposalId+" could not be updated.");
				errorMsg = "Record for Proposal ID "+proposalId+" could not be updated.";			
				errorLog.writeError(bucket, "customer", "PDDC Data", errorMsg);
				continue;
			}
			if(!pddcStr.equals("")){
				if(scoreArr==null||scoreArr.isEmpty()||(!(scoreArr.isEmpty())&&score==0)){
					HttpURLConnection connection ;			
					try{				  
						URL url = new URL(servlet_url + servlet_name + "?pddc="+pddc);
						connection =  (HttpURLConnection)url.openConnection(); 
			
						connection.setRequestMethod("GET");									  
						InputStream response = connection.getInputStream();
					}
					catch(Exception e){						
						logger.error("Decision execution failed for Proposal ID "+proposalId);
						errorMsg = "Decision execution failed for Proposal ID "+proposalId;			
						errorLog.writeError(bucket, "customer", "PDDC Data", errorMsg);
						continue;
					}
					
					try{
						String pointsStr = connection.getHeaderField("Points");				  
						int points = Integer.parseInt(pointsStr);
						if(points!=0)   
							updateCustomerDoc((String)row.key(), points, pddcScore, modifiedTS);	              
					}
					catch(Exception e){							
						logger.error("Record for Proposal ID "+proposalId+" could not be updated.");
						errorMsg = "Record for Proposal ID "+proposalId+" could not be updated.";			
						errorLog.writeError(bucket, "customer", "PDDC Data", errorMsg);
						continue;
					  }	
				}
			}
			
		}
	}
	
	public void updateCustomerDoc(String proposalId, int value, JsonObject pddcScore, String modifiedTS) throws Exception{		
		//ViewResult result = bucket.query(ViewQuery.from(docName, updateViewName).key(proposalId).stale(Stale.FALSE));
		ViewResult result = dbOp.executeQuery(bucket, docName, updateViewName, proposalId);
		ViewRow cust = result.allRows().get(0);
		JsonObject jObj = (JsonObject) cust.value();				
		
		JsonArray pointsArr = jObj.getArray("score");
		JsonObject scoreObj = JsonObject.empty()
				.put("type", pddcPointsField)
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
		
		jObj.put("score", pointsArr);				
		String json = jObj.toString();		
		StringDocument doc = StringDocument.create(cust.id(),json);
		dbOp.upsertDoc(bucket, doc);		
	}
	
	public static void main(String[] args){
		long startTime = System.currentTimeMillis();
		PDDCData pddcData = new PDDCData();
		
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
			String viewName = prop.getProperty("PDDC_VIEW");
			updateViewName = prop.getProperty("PDDC_UPDATE_VIEW");
			servlet_url = prop.getProperty("SERVLET_URL");
			
			pddcData.getPDDCData(docName, viewName);
			long endTime = System.currentTimeMillis();
			logger.debug("Time taken " + (endTime - startTime) + " milliseconds");
		}
		catch(Exception e){			
			logger.error("Connection to database failed.");
			errorMsg = "Connection to database failed.";			
			errorLog.writeError(bucket, "customer", "PDDC Data", errorMsg);
		}
		
	}
}
