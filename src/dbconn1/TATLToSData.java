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


public class TATLToSData {
	public static Bucket bucket;	
	public static String configFile = File.separator+"src"+File.separator+"config.properties";
	private static Logger logger=Logger.getLogger("gSales");
	public static String docName;
	public static String servlet_url = "";
	public static String servlet_name = "DecisionTATLToS";
	public static String updateViewName;
	public static String tatltosPointsField = "dtat_l_to_s";
	public static String password;
	public static DBOperations dbOp = null;	
	
	public void getTATLToSData(String docName, String viewName) throws Exception{
		//ViewResult result = bucket.query(ViewQuery.from(docName, viewName).stale(Stale.FALSE));	
		ViewResult result = dbOp.executeQuery(bucket, docName, viewName);
		
		for(ViewRow row : result){
			JsonArray value;
			String proposalId = "";
			double tatLToS = 0;
			String tatLToSStr = "";
			String caseDisbursed = "";
			JsonObject lToSScore = null;
			String modifiedTS = "";
			JsonArray scoreArr = null;
			JsonObject scoreObj = null;
			int score = 0;
			try{
				proposalId = (String)row.key();				
			    value = (JsonArray)row.value();
			    tatLToSStr = (String)value.get(0);
			    if(!tatLToSStr.equals(""))
			    	tatLToS = Double.parseDouble((String)value.get(0));			    
			    caseDisbursed = value.getString(1);
			    modifiedTS = value.getString(3);
			    
			    if(value.getArray(2)!=null){
					  scoreArr = value.getArray(2);
					  if(!(scoreArr.isEmpty())){
						  if(scoreArr.toString().contains(tatltosPointsField))
							  for(Object obj:scoreArr){
								  logger.debug("OBJ "+obj.toString());
								  scoreObj = (JsonObject)obj;
								  if(scoreObj.getString("type").equals(tatltosPointsField))
									  if(scoreObj.getInt("points")!=null)
										  score = scoreObj.getInt("points");
								  logger.debug("score "+score);
							  }							  
					  }
				}			  
			}
			catch(Exception e){				
				logger.error("TAT L To S Points for proposal id"+row.key()+" could not be updated.");
				continue;
			}
			if(!tatLToSStr.equals("")){
				if(scoreArr==null||scoreArr.isEmpty()||(!(scoreArr.isEmpty())&&score==0)){
					HttpURLConnection connection ;			
					try{				  
						URL url = new URL(servlet_url + servlet_name + "?tatltos="+tatLToS+"&caseDisbursed="+caseDisbursed);
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
						logger.debug("pointsSTr "+pointsStr);
						int points = Integer.parseInt(pointsStr);
						if(points!=0)
							updateCustomerDoc((String)row.key(), points, lToSScore, modifiedTS);	              
					}
					catch(Exception e){							
						logger.error("Record for Proposal ID "+proposalId+" could not be updated.");
						continue;
					  }	
				}
			}
		}
	}
	
	public void updateCustomerDoc(String proposalId, int value, JsonObject lToSScore, String modifiedTS) throws Exception{		
		//ViewResult result = bucket.query(ViewQuery.from(docName, updateViewName).key(proposalId).stale(Stale.FALSE));
		ViewResult result = dbOp.executeQuery(bucket, docName, updateViewName, proposalId);
		ViewRow cust = result.allRows().get(0);
		JsonObject jObj = (JsonObject) cust.value();		
		
		JsonArray pointsArr = jObj.getArray("score");
		JsonObject scoreObj = JsonObject.empty()
				.put("type", tatltosPointsField)
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
		TATLToSData tatLToSData = new TATLToSData();
		
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
			String viewName = prop.getProperty("TAT_LTOS_VIEW");
			updateViewName = prop.getProperty("TAT_LTOS_UPDATE_VIEW");
			servlet_url = prop.getProperty("SERVLET_URL");
			
			tatLToSData.getTATLToSData(docName, viewName);
			long endTime = System.currentTimeMillis();
			logger.debug("Time taken " + (endTime - startTime) + " milliseconds");
		}
		catch(Exception e){			
			logger.error("Connection to database failed.");
		}
		
	}
}
