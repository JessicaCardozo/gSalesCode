package dbconn2;

import java.io.UnsupportedEncodingException;
import java.text.SimpleDateFormat;
import java.util.Date;

import com.couchbase.client.java.Bucket;
import com.couchbase.client.java.document.JsonDocument;
import com.couchbase.client.java.document.StringDocument;
import com.couchbase.client.java.document.json.JsonArray;
import com.couchbase.client.java.document.json.JsonObject;

public class ErrorLog {
	StringDocument errorDoc;
	String errorDocName;
	
	public void getErrorDoc(Bucket bucket){
		try{
			Date date = new Date();
			SimpleDateFormat sdf = new SimpleDateFormat("dd_MM_yyyy");
			errorDocName = "error_summary_"+sdf.format(date);
			errorDoc = bucket.get("error_summary_"+sdf.format(date),StringDocument.class);	
		}
		catch(Exception e){			
			e.printStackTrace();
		}
	}
	
	public void writeError(Bucket bucket, String docName, String data, String errorMsg){		
		Date date = new Date();
		SimpleDateFormat sdf = new SimpleDateFormat("dd/MM/yyyy hh:mm:ss");
		JsonObject obj = JsonObject.empty().put("date",sdf.format(date))
				.put("document", docName)
				.put("data", data)
				.put("error_msg", errorMsg);
		JsonObject errorObj = null;
		StringDocument doc = null;
		if(errorDoc==null){			
			errorObj = JsonObject.empty()
			  .put("error_log", JsonArray.empty().add(obj));			
			doc = StringDocument.create(errorDocName,errorObj.toString());	
			bucket.insert(doc);
		}
		else{				
			JsonArray errLog = JsonObject.empty().fromJson(errorDoc.content()).getArray("error_log");
			errorObj = JsonObject.empty().fromJson(errorDoc.content());
			errLog.add(obj);
			errorObj.put("error_log", errLog);			
			
			doc = StringDocument.create(errorDocName,errorObj.toString());
			bucket.upsert(doc);
		}
	}
	
}
