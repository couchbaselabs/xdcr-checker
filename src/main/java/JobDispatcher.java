import java.util.Iterator;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.ConcurrentLinkedQueue;

import com.couchbase.client.CouchbaseClient;

import net.spy.memcached.internal.OperationFuture;

public class JobDispatcher extends Thread {

	static long mutationCount=0, matchCount=0, mismatchCount  = 0;
	static private Queue<Mutation> mutationQueue = new ConcurrentLinkedQueue<Mutation>();
	static private CouchbaseClient cbclient;
	
	public JobDispatcher(CouchbaseClient c){
		cbclient = c;
	}
	
	public static void add(Mutation m){ 
    	mutationCount++;
		mutationQueue.add(m);
	}

	public void run()
	{
		//access via Iterator
		while (true){
			long currentTime = System.currentTimeMillis();
			Iterator<Mutation> iterator = mutationQueue.iterator();
			try {
				Thread.sleep(500);
			} catch (InterruptedException e) {
				e.printStackTrace();
			}
			while(iterator.hasNext()){
				Mutation mutation = (Mutation) iterator.next();
				long scheduledTime = mutation.getCreationTime() + (XDCRChecker.RETRY_DELAY * (1 + mutation.getRetryCount()));
				if (currentTime > scheduledTime) // wait 5 secs for each attempt
				{
					OperationFuture<Map<String,String>> myFuture = cbclient.getKeyStats(mutation.getKey());
					myFuture.addListener(new MutationChecker(mutation));
					mutationQueue.remove(mutation);

				}
			}// while
		} // while true
	} //run
}

