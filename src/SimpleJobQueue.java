
/**
 * A very simple job queue, used for queuing work in a divide and conquer algorithm.
 * 
 * This queue operates on the concept that each entry has a method (a job to do) and
 * a unit of work (the data on which to perform the job).
 * 
 * As a general rule, each unit of work has a sequence number (starting at 1).  Queue
 * implementations that use directed communications (e.g. the target processor
 * is identified by the enqueue method) can rely on this sequence number to help
 * distribute the work.  Queue implementations that use collective/broadcast comms,
 * shared storage, etc must still consider the number, as some algorithms may require
 * that one thread/process/worker aggregate data to proceed (as an example, consider
 * MergeSort.  The merge step requires each merging process to receive 2 pieces of
 * data to merge together).
 * 
 * The methods in this interface are pared down to the bare essentials, to allow for
 * diverse implementations (MPI/multi processor, etc).
 * 
 * @author Jesse Rosalia
 *
 */
public interface SimpleJobQueue {

	/**
	 * Broadcast a message to all waiting job processors.
	 * 
	 * @param method
	 */
	public void broadcast(int method);

	public void broadcast(int method, UnitOfWork work);

	public UnitOfWork dequeue();
	
	/**
	 * Dequeue a specific message from the queue.  This method will block until the requested message is available.
	 * 
	 * @param method
	 * @return
	 */
	public UnitOfWork dequeue(int method);
	
	/**
	 * Enqueue a unit of work for processing.
	 * 
	 * @param method
	 * @param work
	 */
	public void enqueue(int method, UnitOfWork work);
		
	public UnitOfWork poll();
	
	/**
	 * Poll the entire range of ranks (0 based) for a new message.  This method is nonblocking, and will return null
	 * if a message is not available.
	 * 
	 * @param method
	 * @return
	 */
	public UnitOfWork poll(int method);
	
}
