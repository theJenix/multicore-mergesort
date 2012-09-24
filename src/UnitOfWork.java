
/**
 * A unit of work for the Queue.  This object defines the way in which an algorithm defines a job for a queue process/thread/listener to do.
 * 
 * @author Jesse Rosalia
 *
 */
public class UnitOfWork {
	int num;
	int [] array;
	int start;
	int count;
	int method;
	
	public UnitOfWork(int [] array, int start, int count, int num, int method) {
		this.array  = array;
		this.start  = start;
		this.count  = count;
		this.num    = num;
		this.method = method;
	}
	
	public UnitOfWork repurpose(int [] array, int start, int count, int num, int method) {
		this.array  = array;
		this.start  = start;
		this.count  = count;
		this.num    = num;
		this.method = method;
		return this;
	}
}
