import java.util.Map;
import net.spy.memcached.internal.OperationCompletionListener;
import net.spy.memcached.internal.OperationFuture;



/*
 * Mutation Checker
 * ----------------
 * Check that results from the destination cluster match the expected value 
 * we received from the source cluster. 
 * We have a simple class that represents each mutation and a listener which 
 * does the actual comparison.
 */

class Mutation
{
	// Object that records
	private long creationTime;  // When this update was first received
	private String key;         // The document's key 
	private long expectedCAS;   // Our "version-match" check
	private int retryCount;     // Number of attempts we've made

	
	// Constructor takes a key and a CAS
	public Mutation (String k, long CAS){
		key          = k;
		expectedCAS  = CAS;
		creationTime = System.currentTimeMillis();
		retryCount   = 0;
	}

	// Accessors
	public String getKey(){return key;}
	public long getCreationTime(){return creationTime;}
	public long getExpectedCAS(){return expectedCAS;}
	public void incRetryCount(){retryCount++;}
	public int getRetryCount() { return retryCount;}
}



/*
 * Listener class responsible for checking the results.
 * Results check takes into account the following
 * (1) Did the operation complete successfully ?
 * (2) Did we match this version in the target cluster (CAS value) ?
 * (3) If the comparison fails, was this for a legitimate reason? (see below)
 * (4) Has this operation exceeded its number of lives?
 * Legitimate reasons for failing can include:
 * (i)  The document was updated again more recently
 * (ii) The document on the source cluster was deleted
 * On the final retry, before registering a failure, we go back to the source cluster
 * to verify the version we are looking for remains valid.
 */
class MutationChecker implements OperationCompletionListener{

	private Mutation mutation;

	MutationChecker( Mutation m)
	{
		mutation = m;
	}

	
	// Function to perform appropriate logic if we have not been able to successfully
	// detect a source mutation at the destination cluster
	public void handleXDCRDiscrepancy()
	{
		System.out.println( "** Possible XDCR Discrepancy on KEY: " + mutation.getKey() + "\tCAS: " + mutation.getExpectedCAS());
	}
	
	
	private boolean keyChangedAtSource()
	{
		OperationFuture<Map<String, String>> res = XDCRChecker.sourceClient.getKeyStats(mutation.getKey());
		Map<String, String> myMap = null;
		try {
			myMap = res.get();
		} catch (Exception e){
			System.out.println( "Exception getting final retry future");
		}
		if (res.getStatus().isSuccess())
		{
			String dest_CAS = myMap.get("key_cas");
			long compare_CAS = Long.parseLong(dest_CAS);
			return (compare_CAS != mutation.getExpectedCAS());
		}
		else
		{
			System.out.println( "Couldn't validate key: " + mutation.getKey() + " from source. Received: " + res.getStatus().getStatusCode()); 
			return true;
		}
	}
	
	
	// Allow a certain number of retries before taking further action
	// We must check for legitimate failure reasons and if none
	// are found, declare a potential XDCR discrepancy
	// reasons for missing the mutation
	public void rescheduleOrComplete()
	{
		if (mutation.getRetryCount() < XDCRChecker.MAX_RETRIES) 
		{
			this.mutation.incRetryCount();
			System.out.println(" - issuing retry number " + this.mutation.getRetryCount());
			JobDispatcher.add(this.mutation);
		} 
		else if (keyChangedAtSource())	
		{
			// A Royal Pardon - the CAS now differs at source.
			System.out.println(mutation.getKey() + "\tCAS differed at source"); 
		}
		else
		{ 
			this.handleXDCRDiscrepancy();
		}
	}

	
	// Function called when operation to get destination's CAS 
	// value has completed
	public void onComplete(OperationFuture<?> future) 
	{
		Object res = null;
		// First check operation completed successfully
		if (future.getStatus().isSuccess()) 
		{

			try {
				res = future.get();
			} catch (Exception e) {
				System.out.println( "Exception getting future!:" + mutation.getKey() + "\tCAS: " + mutation.getExpectedCAS() + "\tNum retries: " + mutation.getRetryCount()); 
			}
			if (res != null)
			{
				// Operation completed successfully - check if we got the CAS we expected
				Map<String, String> myMap = (Map<String, String>)res;
				String dest_CAS = myMap.get("key_cas");
				long compare_CAS = Long.parseLong(dest_CAS);
				if (compare_CAS != mutation.getExpectedCAS())
				{
					System.out.print ("CAS Mismatch for Key: " + mutation.getKey() + " Source: " + mutation.getExpectedCAS() + " Dest: " + compare_CAS);
					this.rescheduleOrComplete();
				}
				else
				{
					System.out.println ("Matched Key: " + mutation.getKey()+ " with CAS: " + mutation.getExpectedCAS());
				} 
			}
			else // (res == null)
			{
				// Error-case. Typically don't expect to arrive here.
				System.out.println( "Future returned null for key:" + mutation.getKey() + "\tCAS: " + mutation.getExpectedCAS() + "\tNum retries: " + mutation.getRetryCount() ); 
			}
		}
		else // operation to get the CAS was unsuccessful - check why.
		{
			switch(future.getStatus().getStatusCode())
			{
				case ERR_NOT_FOUND:
					System.out.print("Key: " + mutation.getKey() + " Not Found");
					break;
				case CANCELLED:
					System.out.print ("Key: " + mutation.getKey() + " Op Cancelled");
					break;
				case TIMEDOUT:
					System.out.print("Key: " + mutation.getKey() + " Timed Out");
					break;
				default:
					System.out.println( "Unsuccessful future (" + future.getStatus().getStatusCode()+  ")for key:" + mutation.getKey() + "\tCAS: " + mutation.getExpectedCAS() + "\tNum retries: " + mutation.getRetryCount() ); 
					break;		
			}
			this.rescheduleOrComplete();
		}
	}
}