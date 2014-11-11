import com.couchbase.client.CouchbaseClient;
import com.couchbase.client.CouchbaseConnectionFactoryBuilder;
import com.couchbase.client.TapClient;

import java.net.URI;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import net.spy.memcached.FailureMode;
import net.spy.memcached.tapmessage.ResponseMessage;


/*
 * XDCR Checker
 * -------------
 *  Application to help verify a uni-directional XDCR relationship between 2
 *  clusters.
 *  Performs a check that each mutation on the source cluster makes it
 *  successfully to the destination cluster.
 *  Works by opening a TAP stream on the source cluster to receive all of the
 *  mutations and checks:
 *  (1) The key is available on the destination AND
 *  (2) The key on the destination has the same CAS value
 *
 *  This class provides the entry point (main) function, and creates the
 *  couchbase client connection to both clusters.
 */
public class XDCRChecker {

    // Some constants to help tune the application behaviour
    public static final int MAX_RETRIES = 5;       // Number of attempts before failing a given key
    public static final int RETRY_DELAY = 5000;    // Number of milliseconds in between retries

    // Source cluster details
    private static final String source_bucket = "charlie";
    private static final String source_password = "";
    private static final String source_host_1 = "http://192.168.21.101:8091/pools";
    private static final String source_host_2 = "http://192.168.21.101:8091/pools";

    // Destination cluster details
    private static final String dest_bucket   = "charlie_backup";
    private static final String dest_password = "";
    private static final String dest_host_1 = "http://192.168.21.103:8091/pools";
    private static final String dest_host_2 = "http://192.168.21.104:8091/pools";

    // Three client objects
    public static TapClient tapClient;                              // Source TAP to receive incoming mutations
    public static CouchbaseClient sourceClient;     // Source client to validate CAS
    public static CouchbaseClient destClient;               // Destination client to validate CAS

    public void run() throws Exception {
        // Create the host lists ready for the client constructors
        List<URI> source_hosts = Arrays.asList(new URI(source_host_1), new URI(source_host_2));
        List<URI> dest_hosts   = Arrays.asList(new URI(dest_host_1), new URI(dest_host_2));

        // Setup
        CouchbaseConnectionFactoryBuilder cfb = new CouchbaseConnectionFactoryBuilder();
        cfb.setFailureMode(FailureMode.Cancel);
        // The following parameters are included for further tuning where necessary
        // cfb.setOpTimeout(2500);  // milliseconds for an operation to succeed
        // cfb.setOpQueueMaxBlockTime(500); // milliseconds to allow to enqueue an operation
        // cfb.setMaxReconnectDelay(500);
        // cfb.setTimeoutExceptionThreshold(10);
        // cfb.setUseNagleAlgorithm(true);

        // Connect to the Clusters
        tapClient = new TapClient(source_hosts, source_bucket, source_password);
        sourceClient = new CouchbaseClient(source_hosts, source_bucket, source_password);
        destClient = new CouchbaseClient(cfb.buildCouchbaseConnection(dest_hosts, dest_bucket, dest_password));

        // Set up a TAP stream to retrieve ALL documents AND future updates
        tapClient.tapBackfill("TAP:" + System.currentTimeMillis(), 0,0, TimeUnit.MILLISECONDS);

        // Invoke the thread to dispatch a check for each mutation
        JobDispatcher dispatcher = new JobDispatcher(destClient);
        dispatcher.start();

        // Loop (forever) receiving mutations. Each mutation gets added to the job queue.
        while (tapClient.hasMoreMessages()) {
            ResponseMessage responseMsg = tapClient.getNextMessage();
            if (responseMsg != null) {
                // DEBUG
                //System.out.println( "key:" + responseMsg.getKey() + "\nCAS:" + responseMsg.getCas() + "\nvbucket:" + responseMsg.getVbucket() );

                Mutation mutation = new Mutation(responseMsg.getKey(), responseMsg.getCas());
                JobDispatcher.add(mutation);
            }
        }

        // Shutting down properly
        tapClient.shutdown();
        sourceClient.shutdown();
        destClient.shutdown();
    }   //run

        // Simple main function to kick things off.
    public static void main( String[] args ) throws Exception {
        XDCRChecker myApp = new XDCRChecker();
        myApp.run();
    } //main

} //class
