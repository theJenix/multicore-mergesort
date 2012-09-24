import mpi.MPI;
import mpi.Status;

/**
 * IGNORE ME: This is my prototype code, used when I was trying to figure out the MPI stuff.
 * 
 * @author Jesse Rosalia
 *
 */
public class MulticoreMergeSortOld {
	
	private static final int QUIT_MSG = 3;
	private static final int MERGE_MSG = 2;
	private static final int SPLIT_MSG = 1;
	private static final int array[] = {5,2,6,2,8,2,34,5,2,7,3,63,4};
	private static final int TOKEN_MSG = 4;
	private static final int RESULT_MSG = 5;
	
	public static void main(String[] args) throws Exception {
//		String path = System.getenv("MPJ_HOME")+"/conf/wrapper.conf";
		MPI.Init(args);
		int rank = MPI.COMM_WORLD.Rank();
		int size = MPI.COMM_WORLD.Size();
		
		if (rank == 0) {
			handleController();
		} else {
			if (rank == (size - 1)) {
				//kick off the token
				int []token = {1};
				MPI.COMM_WORLD.Send(token, 0, 1, MPI.INT, 1, TOKEN_MSG);
			}
			handleWorker();
		}
		MPI.Finalize();
	}
	
	private static void handleController() {
		int [] newArr = new int[array.length];
		MPI.COMM_WORLD.Send(array, 0, array.length, MPI.INT, 1, 1);
		MPI.COMM_WORLD.Recv(newArr, 0, newArr.length, MPI.INT, 1, 5);
		int ii = 0;
	}
	
	private static class UnitOfWork {
		int rank;
		int tag;
		int count;
	}
	
	private static void handleWorker() {
		int newArr[] = new int[array.length];
		int msgWaiting = 0;
		do {

			try {
				Thread.sleep(10);
			} catch (InterruptedException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			UnitOfWork unit = waitForMessage(newArr);
			msgWaiting = unit.tag;
			if (msgWaiting == SPLIT_MSG) {
				if (unit.count > 1) {
					MPI.COMM_WORLD.Isend(newArr, 0, 				  unit.count/2, MPI.INT, 1, SPLIT_MSG);	
					int left = unit.count/2;
					MPI.COMM_WORLD.Isend(newArr, left, unit.count - left,   MPI.INT, 1, SPLIT_MSG);
				} else {
					MPI.COMM_WORLD.Isend(newArr, 0, unit.count,   MPI.INT, 1, MERGE_MSG);
				}
			} else if (msgWaiting == MERGE_MSG) {
				int newArr2[] = new int[array.length];
				UnitOfWork unit2 = waitForMessage(MERGE_MSG, newArr2);
				int newCnt = unit.count + unit2.count;
				int newArr3[] = new int[newCnt];
				merge(newArr, unit.count, newArr2, unit2.count, newArr3);
				if (newCnt == array.length) {
					MPI.COMM_WORLD.Isend(newArr3, 0, newCnt, MPI.INT, 0, RESULT_MSG);
				} else {
					MPI.COMM_WORLD.Isend(newArr3, 0, newCnt, MPI.INT, 1, MERGE_MSG);
				}
			}
			
		} while (msgWaiting != QUIT_MSG);
	}

	private static void merge(int[] newArr, int count, int[] newArr2,
			int count2, int[] newArr3) {
		int tgt = 0;
		for (int ii = 0, jj = 0; ii < count || jj < count2; ) {
			if ((ii < count && newArr[ii] < newArr2[jj]) || jj == count2) {
				newArr3[tgt] = newArr[ii];
				ii++;
				tgt++;
			} else {
				newArr3[tgt] = newArr2[jj];
				jj++;
				tgt++;
			}
		}
	}

	private static UnitOfWork waitForMessage(int [] array) {
		return waitForMessage(-1, array);
	}
	
	private static UnitOfWork waitForMessage(int msgNo, int[] array) {
		int size = MPI.COMM_WORLD.Size();
		int rank = MPI.COMM_WORLD.Rank();
		int lastRank = (rank - 1) < 1 ? (size - 1) : (rank - 1);
		int nextRank = (rank + 1) >= size ? 1 : (rank + 1);
		int tag = 0;
		int src = -1;
		int []token = {1};
		Status status = null;
		do {
			MPI.COMM_WORLD.Recv(token, 0, 1, MPI.INT, lastRank, TOKEN_MSG);
			for (int ii = 0; ii < size; ii++) {
				if (msgNo != -1) {
					status = MPI.COMM_WORLD.Iprobe(ii, msgNo);
					if (status != null) {
						src = ii;
						tag = msgNo;
						break;
					}
				} else {
					for (int jj = 1; jj < 3; jj++) {
						status = MPI.COMM_WORLD.Iprobe(ii, jj);
						if (status != null) {
							src = ii;
							tag = jj;
							break;
						}
					}
					if (src >= 0) {
						break;
					}
				}
			}
			if (src >= 0) {
				break;
			}
			MPI.COMM_WORLD.Isend(token, 0, 1, MPI.INT, nextRank, TOKEN_MSG);
		} while (true);
		MPI.COMM_WORLD.Recv(array, 0, status.count, MPI.INT, src, tag);
		MPI.COMM_WORLD.Isend(token, 0, 1, MPI.INT, nextRank, TOKEN_MSG);
		UnitOfWork unit = new UnitOfWork();
		unit.rank = src;
		unit.tag = tag;
		unit.count = status.count;
		return unit;
	}
	
}
