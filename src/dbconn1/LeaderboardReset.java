package dbconn1;

import java.io.File;
import java.io.FileInputStream;
import java.util.Properties;

import org.apache.log4j.Logger;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.StringDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.view.Stale;
import com.couchbase.client.java.view.ViewQuery;
import com.couchbase.client.java.view.ViewResult;
import com.couchbase.client.java.view.ViewRow;

public class LeaderboardReset {
	public static Bucket userBucket;	
	public static String configFile = File.separator+"src"+File.separator+"config.properties";
	private static Logger logger=Logger.getLogger("gSales"); 
	public static String password;
	public static String docName;
	public static String updateViewName;	
	public static DBOperations dbOp = null;	
		
	public void resetLeaderboardPoints(String docName, String viewName) throws Exception{		
		//ViewResult result = userBucket.query(ViewQuery.from(docName, viewName).stale(Stale.FALSE));	
		ViewResult result = dbOp.executeQuery(userBucket, docName, viewName);
		for(ViewRow row : result){
			
			try{				
				JsonObject value = null;
				if(row.value()!=null)
					value= (JsonObject)row.value();								
				JsonObject jObj = (JsonObject) value;
				jObj.put("leaderboard_points",JsonArray.empty());				
				String json = jObj.toString();				
				StringDocument doc = StringDocument.create(row.id(),json);	
				dbOp.upsertDoc(userBucket, doc);				
			}
			catch(Exception e){				
				logger.error("Leaderboard points for User "+row.key()+" could not be reset.");
				continue;
			}
			
		}		
		
	}
	
	public static void main(String[] args){
		long startTime = System.currentTimeMillis();
		LeaderboardReset leaderboardReset = new LeaderboardReset();
		
		try{
			String currentDir = System.getProperty("user.dir");			
			FileInputStream input = new FileInputStream(currentDir+configFile);
			
			Properties prop = new Properties();
			prop.load(input);
			
			String bucketName = prop.getProperty("USER_BUCKET");
			String dbServer = prop.getProperty("DB_SERVER");
			password = prop.getProperty("PASSWORD");
			dbOp = new DBOperations();	
			
			userBucket = dbOp.getBucket(dbServer, bucketName, prop);
			
			if (userBucket==null){
				logger.error("DB connection failed");
				System.exit(-1);
			}
			docName = prop.getProperty("DOC_NAME");						
			updateViewName = prop.getProperty("USER_SCORE");			
			
			leaderboardReset.resetLeaderboardPoints(docName, updateViewName);	
			long endTime = System.currentTimeMillis();
			logger.debug("Time taken " + (endTime - startTime) + " milliseconds");
			
		}
		catch(Exception e){			
			logger.error("Connection to database failed.");
		}
		
	}
}
