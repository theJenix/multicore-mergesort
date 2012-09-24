import mpi.MPI;
import mpi.Status;


/**
 * An implementation of the SimpleJobQueue that uses MPI point to point communications
 * to spread the work across multiple processors/cores.
 * 
 * The enqueue method uses the UnitOfWork's number is used to determine what processor
 * to send messages to, and the poll/dequeue is implemented to receive messages from any process.
 * 
 * TODO: Currently this queue works only on arrays of integers, and makes some algorithm implementation
 * assumptions.  Next steps would be to extract those details to the algorithm, and modify UnitOfWork
 * to be able to handle any data types.
 * 
 * @author Jesse Rosalia
 *
 */
public class MulticoreDistributedQueue implements SimpleJobQueue {

	private int rankLimit;
	private int[] buffer;
	
	public MulticoreDistributedQueue() {
		this.rankLimit = MPI.COMM_WORLD.Size();
	}
	
	public void setBuffer(int [] buffer) {
		this.buffer = buffer;
	}
//	
//	private int getLastRankLinear(int rank) {
//		return (rank - 1) < this.minRank ? this.maxRank : (rank - 1);
//		
//	}
//	
//	private int getNextRankLinear(int rank) {
//		return (rank + 1) > this.maxRank ? this.minRank : (rank + 1);
//	}
//	
//	private int getParentWorkerRank(int rank) {
//		int parentRank = rank / 2;
//		if (parentRank < this.minRank) {
//			parentRank = (minRank + rank) / 2;
//		}
//		return parentRank;
//	}
//	
//	private int getNextWorkerRank(int rank, int num) {
//		int childRank = (2 * rank + num) % (this.maxRank - this.minRank + 1);
//		//NOTE: just a heads up... += minRank doesnt work if minRank is 0, but if minRAnk is 0,
//		// childRank will never be less
//		while (childRank < this.minRank) {
//			childRank += this.minRank;
//		}
//		return childRank;
//	}
//	
//	private synchronized void getToken() {
//		int rank = MPI.COMM_WORLD.Rank();
//		MPI.COMM_WORLD.Recv(mutex, 0, mutex.length, MPI.INT, getLastRankLinear(rank), TOKEN_MSG);
//	}
//	
//	private void passToken() {
//		int rank = MPI.COMM_WORLD.Rank();
//		MPI.COMM_WORLD.Isend(mutex, 0, mutex.length, MPI.INT, getNextRankLinear(rank), TOKEN_MSG);
//		
//	}
	
	public void enqueue(int method, UnitOfWork work) {
		//keep track of the work number in the message we send...this is important for allowing the caller to algorithmically backtrack
		this.buffer[0] = work.num;
		System.arraycopy(work.array, work.start, this.buffer, 1, work.count);
		//NOTE: IMPLEMENTATION DETAIL: only send to the 0th rank on directed messages...when spreading the work
		// based on the #, we want to eschew the 0th rank.
		//TODO: this implementation detail should be extracted to the actual algorithm rather than this queue implementation.
		int tgt = work.num == 0 ? 0 : Math.max(1, work.num % (this.rankLimit));
		MPI.COMM_WORLD.Isend(this.buffer, 0, work.count + 1, MPI.INT, tgt, method);
	}
	

	@Override
	public UnitOfWork dequeue() {
		return dequeue(MPI.ANY_TAG);
	}
	
	public UnitOfWork dequeue(int method) {
		UnitOfWork u = null;
		//loop until we get a message, with a 10ms delay in between polls (after the first poll).
		//NOTE: there may be an easier way to do this, but the blocking methods (Recv and its variants)
		// require a source rank...this queue is written to expect messages from any rank, so that may not
		// work well.
		boolean firstTime = true;
		while (u == null) {
			if (!firstTime) {
				try {
					Thread.sleep(10);
				} catch (InterruptedException e) {
					e.printStackTrace();
				}
			}
			firstTime = false;
			u = poll(method);
		}
		
		return u;
//		UnitOfWork u = null;
//		Status status = null;
//		//Iprobe tests if a message is available
//		status = MPI.COMM_WORLD.Probe(MPI.ANY_SOURCE, method);
//		if (status != null) {
//			//if so, allocate space for it, and stuff into a UnitOfWork object.
////			int [] array = new int[status.count];
//			//NOTE: use the tag from the status, just in case method is MPI.ANY_TAG
//			MPI.COMM_WORLD.Recv(buffer, 0, status.count, MPI.INT, status.source, status.tag);
//			u = new UnitOfWork(buffer, 1, status.count - 1, buffer[0], status.tag);
//		}
		//return u, which may be null if no messages exists.
//		return u;

	}

	@Override
	public UnitOfWork poll() {
		return poll(MPI.ANY_TAG);
//		UnitOfWork work = null;
//		Status status = MPI.COMM_WORLD.Iprobe(MPI.ANY_SOURCE, MPI.ANY_TAG);
//		if (status != null) {
//			int [] array = new int[status.count];
//			MPI.COMM_WORLD.Recv(array, 0, status.count, MPI.INT, status.source, status.tag);
//			work = new UnitOfWork(array, 1, status.count - 1, array[0], status.tag);
//		}
//		
//		return work;
	}
	
	public UnitOfWork poll(int method) {
		UnitOfWork u = null;
		Status status = null;
		//Iprobe tests if a message is available
		status = MPI.COMM_WORLD.Iprobe(MPI.ANY_SOURCE, method);
		if (status != null) {
			//if so, allocate space for it, and stuff into a UnitOfWork object.
//			int [] array = new int[status.count];
			//NOTE: use the tag from the status, just in case method is MPI.ANY_TAG
			MPI.COMM_WORLD.Recv(buffer, 0, status.count, MPI.INT, status.source, status.tag);
			u = new UnitOfWork(buffer, 1, status.count - 1, buffer[0], status.tag);
		}
		//return u, which may be null if no messages exists.
		return u;
	}

	public void broadcast(int method) {
		this.broadcast(method, new UnitOfWork(new int[] {}, 0, 0, 0, method));
	}
	
	@Override
	public void broadcast(int method, UnitOfWork work) {
		//send a message with no data to all processes
		for (int ii = 0; ii < this.rankLimit; ii++) {
			work.num = ii;
			this.enqueue(method, work);
		}
	}
}
