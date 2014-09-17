import java.util.Map;
import java.util.concurrent.ExecutionException;

import net.spy.memcached.internal.OperationCompletionListener;
import net.spy.memcached.internal.OperationFuture;
import net.spy.memcached.ops.StatusCode;

// class used to record mutations to be checked
class Mutation{
	private long creationTime;
	private String key;
	private long expectedCAS;
	private int retryCount;

	public Mutation (String k, long CAS){
		key = k;
		expectedCAS = CAS;
		creationTime = System.currentTimeMillis();
	}

	public String getKey(){return key;}
	public long getCreationTime(){return creationTime;}
	public long getExpectedCAS(){return expectedCAS;}
	public void incRetryCount(){retryCount++;}
	public int getRetryCount() { return retryCount;}
}


class MutationChecker implements OperationCompletionListener{

	private Mutation mutation;

	MutationChecker( Mutation m)
	{
		mutation = m;
	}

	// When we have exhausted all retries, check other possible legitimate 
	// reasons for missing the mutation
	public void lastChanceSaloon(){
		OperationFuture<Map<String, String>> res = XDCRChecker.source_client.getKeyStats(mutation.getKey());
		Map<String, String> myMap = null;
		try {
			myMap = res.get();
		} catch (Exception e){
			System.out.println( "Exception getting last chance future!!:");
		}
		if (res.getStatus().isSuccess())
		{
			String dest_CAS = myMap.get("key_cas");
			long compare_CAS = Long.parseLong(dest_CAS);
			if (compare_CAS != mutation.getExpectedCAS())
			{
				System.out.println( "Royal Pardon 1: " + mutation.getKey() + "\tCAS differed at source"); 
			}
			else{
				System.out.println( "!!!!! XDCR Failure !!!!!: " + mutation.getKey() + "\tCAS: " + mutation.getExpectedCAS());
			}
		}
		else {
			System.out.println( "Royal Pardon 2: " + mutation.getKey() + "\treceived " + res.getStatus().getStatusCode()); 
		}
		
	}
	
	
	public void rescheduleOrFail(){
		// 
		if (mutation.getRetryCount() < XDCRChecker.MAX_RETRIES) {

			this.mutation.incRetryCount();
			JobDispatcher.add(this.mutation);
		} 
		else { // last chance saloon
	
			lastChanceSaloon();
		}
	}

	public void onComplete(OperationFuture<?> future) {
		Object res = null;
		if (future.getStatus().isSuccess()) 
		{

			try {
				res = future.get();
			} catch (Exception e) {
				System.out.println( "Exception getting future!!:" + mutation.getKey() + "\tCAS: " + mutation.getExpectedCAS() + "\tNum retries: " + mutation.getRetryCount()); 
			}
			if (res != null){
				Map<String, String> myMap = (Map<String, String>)res;
				String dest_CAS = myMap.get("key_cas");
				long compare_CAS = Long.parseLong(dest_CAS);
				if (compare_CAS != mutation.getExpectedCAS())
				{
					System.out.println ("CAS Mismatch for Key: " + mutation.getKey() + "Expected: " + mutation.getExpectedCAS() + " Got:" + compare_CAS + "...rescheduling ");
					this.rescheduleOrFail();
				}
				else
				{
					System.out.println ("Matched Key: " + mutation.getKey()+ " with CAS: " + mutation.getExpectedCAS());
				} 
			}
			else
			{
				System.out.println( "Future returned null for key:" + mutation.getKey() + "\tCAS: " + mutation.getExpectedCAS() + "\tNum retries: " + mutation.getRetryCount() ); 
			}
		}
		else
		{
			if (future.getStatus().getStatusCode() == StatusCode.ERR_NOT_FOUND){
				System.out.println ("Key not found: " + mutation.getKey() + "...rescheduling");
				this.rescheduleOrFail();

			}
			else {
				System.out.println( "Unsuccessful future (" + future.getStatus().getStatusCode()+  ")for key:" + mutation.getKey() + "\tCAS: " + mutation.getExpectedCAS() + "\tNum retries: " + mutation.getRetryCount() ); 
			}
		}
	}
}