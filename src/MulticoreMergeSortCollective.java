import mpi.MPI;

public class MulticoreMergeSortCollective {

	private static int[] generateData(int count) {
		int [] data = new int[count];
		for (int ii = 0; ii < count; ii++) {
			data[ii] = (int)(Math.random() * count);
		}
		
		return data;
	}

	private static void printResults(long elapsedMS, int[] array) {
		System.out.println(String.format("Finished sorting %d numbers in %d milliseconds", array.length, elapsedMS));
		if (array.length < 10000) {
			for (int ii = 0; ii < array.length; ii++) {
				System.out.print(" " + array[ii]);
			}
			System.out.println();
		}
	}

	public static void main(String[] args) throws Exception {
		MPI.Init(args);
		int rank = MPI.COMM_WORLD.Rank();
		int size = MPI.COMM_WORLD.Size();

		int origSize = 200000000;
		int [] array;
		int [] newArray;
		boolean profile = true;
		Profiler prof = new Profiler();
		int mainToken = 0;
		int newSize;
		if (rank == 0) {
			array = generateData(origSize);
			newArray = array;
			newSize = array.length;
			if (array.length % size > 0) {
				newSize += (size - (array.length % size));
		
				newArray = new int[newSize];
				int ii = 0;
				for (; ii < array.length; ii++) {
					newArray[ii] = array[ii];
				}
				for (; ii < newSize; ii++) {
					newArray[ii] = Integer.MAX_VALUE;
				}
			}
			mainToken = prof.start("main");
		} else {
			newSize = origSize;
			if (newSize % size > 0) {
				newSize += (size - (newSize % size));
			}
			//not used outside rank 0, but we need to set to null to avoid compiler errors
			array = null;
			newArray = null;//array;
		}
		long startTimeMS = System.currentTimeMillis();
		int token = 0;
		if (profile) prof.start("one step");
		int chunkSize = newSize/size;
		//allocate the new array in which to contain the chunks of data
		int [] newArray2 = new int[chunkSize];
		//scatter the array across all processes
		MPI.COMM_WORLD.Scatter(newArray, 0, chunkSize, MPI.INT, newArray2, 0, chunkSize, MPI.INT, 0);
		//sort the chunks on each process
		new ReferenceMergeSort().go(newArray2, chunkSize); //(newArray2, 0, half, newArray2, half, chunkSize - half, newArray3);
		//and gather the data back together
		MPI.COMM_WORLD.Gather(newArray2, 0, chunkSize, MPI.INT, newArray, 0, chunkSize, MPI.INT, 0);
		//back at rank 0, do an n-way merge of all of the chunks
		if (rank == 0) {
			int mt = 0;
			if (profile) mt = prof.start("merge");
			nWayMerge(newArray, size, chunkSize, array);
			if (profile) prof.stop("merge", mt);
		}
		//debugging info
		long elapsedMS = System.currentTimeMillis() - startTimeMS;
		if (profile) prof.stop("one step", token);
		//for rank 0, print results and verify the array is in fact sorted
		if (rank == 0) {
			if (profile) prof.stop("main", mainToken);
			printResults(elapsedMS, array);
		
			for (int ii = 1; ii < array.length; ii++) {
				if (array[ii] < array[ii - 1]) {
					System.out.println("Array out of order");
					break;
				}
			}
		}
		if (profile) {
			System.out.println("(" + rank + ") Profiler");
			System.out.println("(" + rank + ") " + prof.toString());
		}

		MPI.Finalize();
	}

	/**
	 * Merge n chunks (each of chunkSize size) of an array into the new array
	 * 
	 * @param array
	 * @param chunks
	 * @param chunkSize
	 * @param newArray
	 */
	private static void nWayMerge(int[] array, int chunks, int chunkSize,
			int[] newArray) {
		//keep track of the current index of each chunk in an array
		int [] indices = new int[chunks];
		for (int ii = 0; ii < newArray.length; ii++) {
			int minInx = 0; //the # of the chunk with the next smallest value...will also be the index into indices
			//find which chunk has the next smallest value
			for (int jj = 1; jj < chunks; jj++) {
				//if we havent gotten to the last index of the chunk, check if the value at this chunk is less than
				// the last min value we found...if so, update minInx
				if (indices[jj] < chunkSize && array[chunkSize * jj + indices[jj]] < array[chunkSize * minInx + indices[minInx]]) {
					minInx = jj;
				}
			}
			//copy that value into the new array
			newArray[ii] = array[chunkSize * minInx + indices[minInx]];
			//and update that chunk's index
			indices[minInx]++;
		}
	}
}
