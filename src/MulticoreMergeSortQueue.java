import java.util.HashMap;
import java.util.Map;

import mpi.MPI;

/**
 * An implementation of the Merge Sort algorithm, using a distributed multiprocess Queue to accomplish the work.
 * 
 * Currently, this code uses processor 0 as the "main process", that waits for a results, and all other
 *  processors as the worker processor.  The worker processors split and merge until an array of the target
 *  length 
 * 
 * TODO: There are a few things to do here to make this more reusable:
 * 	The length is assumed to be available in all processors (as the stopping point), but that will not be the case
 *  in a real implementation.  Need to either pass the length around with the data, OR
 *  
 *  Modify the main thread to be a worker, and remove the use of RESULT_MSG.  Because of the "binary tree"
 *  # system, there should be as many splits as there are merges, and the result should always roll up to the 0th processor.
 *  	This of course assumes that the 0 ranked processor kicks off the algorithm, which could be mitigated by always
 *  	sending the message to the 0th process first to kick off the sort, or by carrying the calling rank around
 *  	in the messages.
 * 	
 * @author Jesse Rosalia
 *
 */
public class MulticoreMergeSortQueue {

	/** A definition of the messages used in merge sort: split, merge, result and quit */
	/** Message to split the array in half, and enqueue the results */
	private static final int SPLIT_MSG = 1;
	/** Message to quit a worker processe and exit the program.  Most commonly used with the broadcast queue method */ 
	private static final int QUIT_MSG = 3;
	/** Message to merge the data elements in order.  This must be received in pairs to work */
	private static final int MERGE_MSG = 2;
	/** Message to send the result once the sort is complete */
	private static final int RESULT_MSG = 5;
	
	private static final int SIZE_MSG = 6;
	
	/** Test data */
//	private static final int array[] = {5,2,6,2,8,2,34,5,2,7,3,63,4,5,2,6,2,8,2,34,5,2,7,3,63,4,5,2,6,2,8,2,34,5,2,7,3,63,4,5,2,6,2,8,2,34,5,2,7,3,63,4,5,2,6,2,8,2,34,5,2,7,3,63,4,5,2,6,2,8,2,34,5,2,7,3,63,4,5,2,6,2,8,2,34,5,2,7,3,63,4,5,2,6,2,8,2,34,5,2,7,3,63,4,5,2,6,2,8,2,34,5,2,7,3,63,4,5,2,6,2,8,2,34,5,2,7,3,63,4,5,2,6,2,8,2,34,5,2,7,3,63,4,5,2,6,2,8,2,34,5,2,7,3,63,4,5,2,6,2,8,2,34,5,2,7,3,63,4,5,2,6,2,8,2,34,5,2,7,3,63,4,5,2,6,2,8,2,34,5,2,7,3,63,4,5,2,6,2,8,2,34,5,2,7,3,63,4,5,2,6,2,8,2,34,5,2,7,3,63,4,5,2,6,2,8,2,34,5,2,7,3,63,4,5,2,6,2,8,2,34,5,2,7,3,63,4,5,2,6,2,8,2,34,5,2,7,3,63,4,5,2,6,2,8,2,34,5,2,7,3,63,4,5,2,6,2,8,2,34,5,2,7,3,63,4,5,2,6,2,8,2,34,5,2,7,3,63,4,5,2,6,2,8,2,34,5,2,7,3,63,4,5,2,6,2,8,2,34,5,2,7,3,63,4,5,2,6,2,8,2,34,5,2,7,3,63,4,5,2,6,2,8,2,34,5,2,7,3,63,4,5,2,6,2,8,2,34,5,2,7,3,63,4,5,2,6,2,8,2,34,5,2,7,3,63,4,5,2,6,2,8,2,34,5,2,7,3,63,4,5,2,6,2,8,2,34,5,2,7,3,63,4,5,2,6,2,8,2,34,5,2,7,3,63,4,5,2,6,2,8,2,34,5,2,7,3,63,4,5,2,6,2,8,2,34,5,2,7,3,63,4,5,2,6,2,8,2,34,5,2,7,3,63,4};
//	private static final int array[] = {5,2,6,2,4,1};
	
	//NOTE: to run, you must run this class, but use the following command line arg to kick off MPI:
	//	-jar ${MPJ_HOME}/lib/starter.jar
	// (MPI provides the runner(s) for the class)
	//  NOTE that you MUST have MPJ_HOME defined in your environment (for eclipse, your terminal, etc)...the core MPJ Express libraries
	//  require this to find a specific config file, and will crash without this variable.
	//
	//...to debug, add the following:
	//	-agentlib:jdwp=transport=dt_socket,server=y,suspend=y,address=8000
	//
	//...to limit the number of processors it can use:
	//	-np x (x=integer, # of processors)
	//
	public static void main(String[] args) throws Exception {

		MPI.Init(args);

		boolean debug = false;
		boolean profile = true;
				
		int rank = MPI.COMM_WORLD.Rank();
		SimpleJobQueue queue = new MulticoreDistributedQueue();
		int threshold = 4000000;
		//int threshold = 1000;
		UnitOfWork result;
		Profiler prof = new Profiler();

		if (rank == 0) {
			int size = 16000000;
			int [] data = generateData(size);
			long start = System.currentTimeMillis();

			int [] scratch = new int[size + 1];
			((MulticoreDistributedQueue)queue).setBuffer(scratch);
			
			//broadcast the size to all job processors, so we can preallocate space
			queue.broadcast(SIZE_MSG, new UnitOfWork(new int[] {size}, 0, 1, 0, SIZE_MSG));
			
			queue.enqueue(SPLIT_MSG, new UnitOfWork(data, 0, data.length, 1 ,SPLIT_MSG));
			data = null;
			result = queue.dequeue(MERGE_MSG);

			long stop = System.currentTimeMillis();
			queue.broadcast(QUIT_MSG);
			printResults(stop - start, result);
		} else {

			/**
			 * General strategy:
			 * 
			 * Loop until a QUIT_MSG is received:
			 * 
			 * 	Poll for a SPLIT_MSG...if encountered and the data is > 1 element long, we need to split the array and enqueue the split
			 * 		arrays as 2 SPLIT_MSG jobs.
			 * 
			 * 	If the data has only 1 element, we can start the merge...enqueue the 1 element data as a MERGE_MSG job and loop around.
			 * 
			 * 	If there isn't a SPLIT_MSG available, poll for a MERGE_MSG...if encountered, check to see if we have another message waiting
			 * 	with the same num (more on that later).  If so, merge the parts together and enqueue either a MERGE_MSG, or a RESULT_MSG (if we're done).
			 *  
			 * A note on job numbering:
			 * 	The job numbering follows a scheme similar to how you would index a binary tree in an array:
			 * 		Given a message with num = X:
			 * 			LeftSplit (X) = 2X,
			 * 			RightSplit(X) = 2X + 1,
			 * 			Merge	  (X) = X/2
			 * 
			 * 	This accomplishes 2 important goals:
			 * 		It provides a good spread of the work without duplicates (process 1 splits into 2 and 3, process 2 into 4 and 5,
			 * 			3 into 6 and 7, etc...no overlap)
			 * 		It allows for a deterministic way of sending 2 pieces of data to one process for merging (especially important
			 * 			with MPI, since it uses directed messages)
			 */
			Map<Integer, UnitOfWork> workMap = new HashMap<Integer, UnitOfWork>();
			UnitOfWork u1 = null, u2 = null;
			int [] scratch = new int[5];
			((MulticoreDistributedQueue)queue).setBuffer(scratch);

			UnitOfWork work = queue.dequeue(SIZE_MSG);
			int size = work.array[1];
			scratch = new int[size + 1];
			((MulticoreDistributedQueue)queue).setBuffer(scratch);
			boolean finished = false;
			do {
				//poll for the next piece of work for this process
				int t = 0;
				if (profile) t = prof.start("dequeue");
				work = queue.dequeue();
				if (profile) prof.stop("dequeue", t);
				//switch to determine how to process
				switch(work.method) {
				case SPLIT_MSG:
					if (work.count > threshold) {
						if (profile) t = prof.start("split");
						//split the data array in half 
						int half = work.count/2;
						
						//Left Split
						if (debug) System.out.println(String.format("Process %d (rank %d) enqueing split %d, size %d", work.num, rank, work.num*2, half));
						queue.enqueue(SPLIT_MSG, new UnitOfWork(work.array, work.start,    half,              work.num * 2, SPLIT_MSG));
						
						//Right Split
						if (debug) System.out.println(String.format("Process %d (rank %d) enqueing split %d, size %d", work.num, rank, work.num*2 + 1, work.count - half));
						queue.enqueue(SPLIT_MSG, work.repurpose(work.array, work.start + half, work.count - half, work.num * 2 + 1, SPLIT_MSG));
						if (profile) prof.stop("split", t);
					} else {
						if (profile) t = prof.start("base_case");
						//1 element, nothing to split...Merge
						if (debug) System.out.println(String.format("Process %d (rank %d) enqueing merge %d, size %d", work.num, rank, work.num/2, work.count));
						System.arraycopy(work.array, work.start, scratch, 0, work.count);
						new ReferenceMergeSort().go(scratch, work.count);
						queue.enqueue(MERGE_MSG, work.repurpose(scratch, 0, work.count, work.num / 2, MERGE_MSG));
						if (profile) prof.stop("base_case", t);
					}
					break;
				case MERGE_MSG:
					if (profile) t = prof.start("merge");

					//merge message is available
					if (debug) System.out.println(String.format("Process %d (rank %d) grabbing merge %d, size %d", work.num, rank, work.num, work.count));
					//this is the second unit grabbed...we need to process
					//NOTE: we need a map here, because a process will be receiving merge messages intended for various
					// multiples of its rank, and we need to make sure we keep the numbers straight.
					//..e.g. process 5 may receive work directed to 5 and 12 (mod 7), and we need to make sure
					// the 2 pieces of work for 12 are matched up.
					if (workMap.containsKey(work.num)) {
						u1 = workMap.remove(work.num);
						u2 = work;
						int newSize = u1.count + u2.count;
						//merge the 2 unit's arrays into newArray
						merge(u1.array, u1.start, u1.count, u2.array, u2.start, u2.count, scratch);
						
						//if we're done, send the results back to processor 0
						//NOTE: no longer necessary...the way we spread out the processes will ensure it all
						// comes back to 0
						if (false /*newArray.length == array.length*/) {
							if (debug) System.out.println(String.format("Process %d (rank %d) enqueing result %d, size %d", work.num, rank, work.num/2, newSize));
							queue.enqueue(RESULT_MSG, new UnitOfWork(scratch, 0, scratch.length, 0, RESULT_MSG));
						} else {
							if (work.num == 1) {
								@SuppressWarnings("unused")
								int ii = 0;
							}
							//otherwise enqueue this block to be merged by the parent process
							if (debug) System.out.println(String.format("Process %d (rank %d) enqueing merge %d, size %d", work.num, rank, work.num/2, newSize));
							int s = 0;
							if (profile) s = prof.start("merge_enqueue");
							queue.enqueue(MERGE_MSG, work.repurpose(scratch, 0, newSize, work.num / 2, MERGE_MSG));
							if (profile) prof.stop("merge_enqueue", s);
						}
						//clear u1 and u2;
						u1 = null;
						u2 = null;
					} else {
						workMap.put(work.num, work);
					}
					if (profile) prof.stop("merge", t);

					break;
				case QUIT_MSG:
					finished = true;
					break;
				}
			} while (!finished);
		}
		
		if (profile) {
			System.out.println("(" + rank + ") Profiler");
			System.out.println("(" + rank + ") " + prof.toString());
		}
		MPI.Finalize();
	}
	private static int[] generateData(int count) {
		int [] data = new int[count];
		for (int ii = 0; ii < count; ii++) {
			data[ii] = (int)(Math.random() * count);
		}
		
		return data;
	}

	private static void printResults(long elapsedMS, UnitOfWork result) {
		System.out.println(String.format("Finished sorting %d numbers in %d milliseconds", result.count, elapsedMS));
		if (result.count < 10000) {
			for (int ii = result.start; ii < result.count; ii++) {
				System.out.print(" " + result.array[ii]);
			}
			System.out.println();
		}
	}

	/**
	 * Merge the integers from newArr and newArr2 into newArr3, in order.  This method
	 * assumes that newArr3 is large enough to hold ((count - start) + (count2 - start2)) integers.
	 * 
	 * @param newArr
	 * @param start
	 * @param count
	 * @param newArr2
	 * @param start2
	 * @param count2
	 * @param newArr3
	 */
	private static void merge(int[] newArr, int start, int count, int[] newArr2,
			int start2, int count2, int[] newArr3) {
		int tgt = 0;
		for (int ii = 0, jj = 0; ii < count || jj < count2; ) {
			if (jj == count2 || (ii < count && newArr[ii + start] < newArr2[jj + start2])) {
				newArr3[tgt] = newArr[ii + start];
				ii++;
				tgt++;
			} else {
				newArr3[tgt] = newArr2[jj + start2];
				jj++;
				tgt++;
			}
		}
	}
}
