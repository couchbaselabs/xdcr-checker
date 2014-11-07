import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.couchbase.client.CouchbaseClient;

import net.spy.memcached.internal.OperationFuture;


/*
 * Job Dispatcher
 * --------------
 * For each mutation received on the tap client, we add it to this dispatcher's queue
 * We loop forever walking this queue looking for jobs to trigger. Jobs are triggered 
 * when their RETRY_DELAY time has expired. This gives mutations some grace period to 
 * arrive at the destination. When a job is dispatched we request the CAS value from 
 * the destination cluster and compare it to the CAS we received from the source TAP.
 * We make this call asynchronously and add a listener to check for the result.
 * 
 */
public class JobDispatcher extends Thread {

	static long mutationCount=0, matchCount=0, mismatchCount  = 0;
	static private Queue<Mutation> mutationQueue = new ConcurrentLinkedQueue<Mutation>();
	static private CouchbaseClient cbclient;
	
	
	// Constructor takes a connection to the destination cluster
	// where we will perform the CAS check
	public JobDispatcher(CouchbaseClient c){
		cbclient = c;
	}
	
	// Queue accessor for adding new jobs or retries
	public static void add(Mutation m){ 
    	mutationCount++;
		mutationQueue.add(m);
	}

	
	// Main function where we loop through all items on our queue,
	// looking for jobs that are ready to be dispatched.
	public void run()
	{
		while (true)
		{
			long currentTime = System.currentTimeMillis();
			Iterator<Mutation> iterator = mutationQueue.iterator();
		
			// Loop through all items of the queue
			while(iterator.hasNext()){
				
				// Take the mutation at the head of the queue and check
				// if we are ready to dispatch the job
				Mutation mutation = (Mutation) iterator.next();
				long scheduledTime = mutation.getCreationTime() + (XDCRChecker.RETRY_DELAY * (1 + mutation.getRetryCount()));
				if (currentTime > scheduledTime) // wait 5 secs for each attempt
				{
					// This mutation is ready to be checked. 
					// Dispatch the operation, register a listener to await the result and remove from the queue
					OperationFuture<Map<String,String>> myFuture = cbclient.getKeyStats(mutation.getKey());
					myFuture.addListener(new MutationChecker(mutation));
					mutationQueue.remove(mutation);
				}
			}// while (iterator)
			
			// Take a short pause so we don't burn CPU
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			
			
		} // while (true)
	} //run
}

