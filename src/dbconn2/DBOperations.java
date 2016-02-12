package dbconn2;

import java.util.Properties;

import org.apache.log4j.Logger;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.Cluster;
import com.couchbase.client.java.CouchbaseCluster;
import com.couchbase.client.java.document.StringDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.env.CouchbaseEnvironment;
import com.couchbase.client.java.env.DefaultCouchbaseEnvironment;
import com.couchbase.client.java.view.Stale;
import com.couchbase.client.java.view.ViewQuery;
import com.couchbase.client.java.view.ViewResult;

public class DBOperations {
	private static Logger logger=Logger.getLogger("gSales");
	public static int loopCount=0;	
	
	public Bucket getBucket(String dbServer,String bucketName, Properties prop) throws Exception{	
		Cluster cluster = null;
		Bucket bucket = null;
			
		loopCount = Integer.parseInt(prop.getProperty("LOOP_COUNT"));
		int connectTimeout = Integer.parseInt(prop.getProperty("CONNECT_TIMEOUT"));		
		int i=0;
		while(bucket==null&&i<loopCount){
			try{
				CouchbaseEnvironment env = DefaultCouchbaseEnvironment.builder()
			        .connectTimeout(connectTimeout) //10000ms = 10s, default is 5s
			        .build();
					cluster = CouchbaseCluster.create(env,dbServer);			
					bucket = cluster.openBucket(bucketName);					
			}
			catch(Exception e){
				logger.debug("In catch "+e.getMessage());	
				if (e.getMessage().equals("Could not open bucket.")||e.getMessage().equals("java.util.concurrent.TimeoutException")){
					
					try{
						Thread.sleep(5000);
					} 
					catch(InterruptedException ex) {
						Thread.currentThread().interrupt();
					}
				}
				else{ 
					throw e;
				}
			}
			i++;
		}
		return bucket;
	}
	
	public void upsertDoc(Bucket bucket, StringDocument doc) throws Exception{
		StringDocument doc1 =null;	
		int i = 0;
		while(doc1==null&&i<loopCount){
			logger.debug("in while "+i);
			try {
				doc1 = bucket.upsert(doc);
			}
			catch (Exception te){				
				te.printStackTrace();
				if (te.getMessage().equals("java.util.concurrent.TimeoutException")){
					logger.debug("Caught TE exception");
					logger.debug("I : " + i);
					try{
						Thread.sleep(5000);
					} catch(InterruptedException ex) {
						logger.error("Interrupted Exception "+ex.getMessage());
						Thread.currentThread().interrupt();
					}
				}
				else{ 
					throw te;
				}
			}
			i++;
		}
	}
	
	public void insertDoc(Bucket bucket, StringDocument doc) throws Exception{
		StringDocument doc1 =null;
		int i=0;
		while(doc1==null&&i<loopCount){
			logger.debug("in while "+i);
			try {
				doc1 = bucket.insert(doc);
			}
			catch (Exception te){				
				te.printStackTrace();
				if (te.getMessage().equals("java.util.concurrent.TimeoutException")){
					logger.debug("Caught TE exception");
					logger.debug("I : " + i);
					try{
						Thread.sleep(5000);
					} catch(InterruptedException ex) {
						logger.error("Interrupted Exception "+ex.getMessage());
						Thread.currentThread().interrupt();
					}
				}
				else{ 
					throw te;
				}
			}
			i++;
		}
	}
	
	public ViewResult executeQuery(Bucket bucket, String docName, String viewName) throws Exception{
		ViewResult result =null;
		int i=0;
		while(result==null&&i<loopCount){
			logger.debug("in while "+i);
			try {
				result = bucket.query(ViewQuery.from(docName, viewName).stale(Stale.FALSE));
			}
			catch (Exception te){				
				te.printStackTrace();
				if (te.getMessage().equals("java.util.concurrent.TimeoutException")){
					logger.debug("Caught TE exception");
					logger.debug("I : " + i);
					try{
						Thread.sleep(5000);
					} catch(InterruptedException ex) {
						Thread.currentThread().interrupt();
					}
				}
				else{ 
					throw te;
				}
			}
			i++;
		}
		return result;
	}
	
	public ViewResult executeQuery(Bucket bucket, String docName, String viewName, String key) throws Exception{
		ViewResult result =null;
		int i=0;
		while(result==null&&i<loopCount){
			logger.debug("in while "+i);
			try {
				result = bucket.query(ViewQuery.from(docName, viewName).key(key).stale(Stale.FALSE));
			}
			catch (Exception te){				
				te.printStackTrace();
				if (te.getMessage().equals("java.util.concurrent.TimeoutException")){
					logger.debug("Caught TE exception");
					logger.debug("I : " + i);
					try{
						Thread.sleep(5000);
					} catch(InterruptedException ex) {
						Thread.currentThread().interrupt();
					}
				}
				else{ 
					throw te;
				}
			}
			i++;
		}
		return result;
	}
	
	public ViewResult executeQuery(Bucket bucket, String docName, String viewName, JsonArray key) throws Exception{
		ViewResult result =null;
		int i=0;
		while(result==null&&i<loopCount){
			logger.debug("in while "+i);
			try {
				result = bucket.query(ViewQuery.from(docName, viewName).key(key).stale(Stale.FALSE));
			}
			catch (Exception te){				
				te.printStackTrace();
				if (te.getMessage().equals("java.util.concurrent.TimeoutException")){
					logger.debug("Caught TE exception");
					logger.debug("I : " + i);
					try{
						Thread.sleep(5000);
					} catch(InterruptedException ex) {
						Thread.currentThread().interrupt();
					}
				}
				else{ 
					throw te;
				}
			}
			i++;
		}
		return result;
	}
	
	public ViewResult executeQuery(Bucket bucket, String docName, String viewName, JsonArray startKey, JsonArray endKey) throws Exception{
		ViewResult result =null;
		int i=0;
		while(result==null&&i<loopCount){
			logger.debug("in while "+i);
			try {
				result = bucket.query(ViewQuery.from(docName, viewName).startKey(startKey).endKey(endKey).stale(Stale.FALSE));
			}
			catch (Exception te){				
				te.printStackTrace();
				if (te.getMessage().equals("java.util.concurrent.TimeoutException")){
					logger.debug("Caught TE exception");
					logger.debug("I : " + i);
					try{
						Thread.sleep(5000);
					} catch(InterruptedException ex) {
						Thread.currentThread().interrupt();
					}
				}
				else{ 
					throw te;
				}
			}
			i++;
		}
		return result;
	}
	
	public ViewResult executeQuery(Bucket bucket, String docName, String viewName, int key) throws Exception{
		ViewResult result =null;
		int i=0;
		while(result==null&&i<loopCount){
			logger.debug("in while "+i);
			try {
				result = bucket.query(ViewQuery.from(docName, viewName).key(key).stale(Stale.FALSE));
			}
			catch (Exception te){				
				te.printStackTrace();
				if (te.getMessage().equals("java.util.concurrent.TimeoutException")){
					logger.debug("Caught TE exception");
					logger.debug("I : " + i);
					try{
						Thread.sleep(5000);
					} catch(InterruptedException ex) {
						Thread.currentThread().interrupt();
					}
				}
				else{ 
					throw te;
				}
			}
			i++;
		}
		return result;
	}
	
	public ViewResult executeQuery(Bucket bucket, String docName, int groupLevel, String viewName) throws Exception{
		ViewResult result =null;
		int i=0;
		while(result==null&&i<loopCount){
			logger.debug("in while "+i);
			try {
				result = bucket.query(ViewQuery.from(docName, viewName).groupLevel(groupLevel).stale(Stale.FALSE));
			}
			catch (Exception te){				
				te.printStackTrace();
				if (te.getMessage().equals("java.util.concurrent.TimeoutException")){
					logger.debug("Caught TE exception");
					logger.debug("I : " + i);
					try{
						Thread.sleep(5000);
					} catch(InterruptedException ex) {
						Thread.currentThread().interrupt();
					}
				}
				else{ 
					throw te;
				}
			}
			i++;
		}
		return result;
	}

}
