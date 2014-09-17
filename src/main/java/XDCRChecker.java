import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.TapClient;

import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.tapmessage.ResponseMessage;


public class XDCRChecker 
{
	
	public static final int MAX_RETRIES = 5;
	public static final int RETRY_DELAY = 5000;
	public static TapClient tapClient;
	public static CouchbaseClient source_client;
	public static CouchbaseClient dest_client;
	
	public void run() throws Exception
	{
		  // (Subset) of nodes in the cluster to establish a connection
        List<URI> source_hosts = Arrays.asList(   new URI("http://192.168.21.101:8091/pools"), new URI("http://192.168.21.102:8091/pools")   );
        List<URI> dest_hosts= Arrays.asList(   new URI("http://192.168.21.103:8091/pools")  , new URI("http://192.168.21.104:8091/pools")  );

        // Name of the Bucket to connect to
        String source_bucket = "charlie";
        String dest_bucket   = "charlie_backup";
          
        // Password of the bucket (empty) string if none
        String password = "";
       
        // Connect to the Cluster
         tapClient = new TapClient(source_hosts, source_bucket, password);
         source_client = new CouchbaseClient(source_hosts, source_bucket, password);
         dest_client = new CouchbaseClient(dest_hosts, dest_bucket, password);	
    	tapClient.tapBackfill("jimmy18", 0,0, TimeUnit.MILLISECONDS);
        	
    	//TapStream testing =	tapClient.tapDump("zz");

    	JobDispatcher dispatcher = new JobDispatcher(dest_client);
    	dispatcher.start();
    	
        while (tapClient.hasMoreMessages())
        { 
        	ResponseMessage responseMsg = tapClient.getNextMessage();   
        	if (responseMsg != null) 
        	{ 
	        	//System.out.println( "key:" + responseMsg.getKey() + "\nCAS:" + responseMsg.getCas() + "\nvbucket:" + responseMsg.getVbucket() ); 
	        	
	        	
	        	Mutation mutation = new Mutation(responseMsg.getKey(), responseMsg.getCas());
	        	JobDispatcher.add(mutation);

        	}

        	
        	
        }
        
        // Shutting down properly
        tapClient.shutdown();

    	
    }	//run
	
	
	
    public static void main( String[] args ) throws Exception
    {

    	  	XDCRChecker myApp = new XDCRChecker();
    	  	myApp.run();
    	
    } //main

} //class