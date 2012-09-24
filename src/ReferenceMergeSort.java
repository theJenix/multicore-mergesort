
/**
 * A reference implementation of Merge sort, against which we can compare the MPI version.
 * 
 * This is written to be space/memory efficient, at the expense of a little code complexity
 * 
 * @author Jesse Rosalia
 *
 */
public class ReferenceMergeSort {
	public static void main(String[] args) throws Exception {
		int [] data = generateData(200000000);

		long startTime = System.currentTimeMillis();
		new ReferenceMergeSort().go(data, data.length);
		long endTime = System.currentTimeMillis();
		printResults(endTime - startTime, data);
	}

	private static final int threshold = 20;

	/**
	 * Run the sort on the specified number of elements in the data array.
	 * 
	 * @param data
	 * @param count
	 */
	public void go(int[] data, int count) {
		int [] newArray = new int[count];
		mergeSort(data, 0, count - 1, newArray);
	}

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

	/**
	 * Run the merge sort by recursively calling itself with smaller and smaller chunks, then
	 * recombining the chunks in sorted order.
	 * 
	 * This performs an in-place sort by using a temporary buffer
	 * 
	 * @param array
	 * @param start
	 * @param end
	 * @param tempBuffer
	 */
	private void mergeSort(int[] array, int start, int end, int[] tempBuffer) {
		int half = start + (end - start)/2;
		
		if (end - start > threshold) {
			mergeSort(array, start, half, tempBuffer);
			mergeSort(array, half+1, end, tempBuffer);
			//merge into the temporary buffer
			merge(array, start, half - start + 1, array, half + 1, end - half, tempBuffer);
			//then copy the merged data over the appropriate unmerged elements of the array 
			System.arraycopy(tempBuffer, 0, array, start, end - start + 1);
		} else {
			selectionSort(array, start, end);
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

	
	protected static void selectionSort(int [] array, int start, int end) {
		int minPos = 0;
		//for each character position, find the correct character that belongs in that position (min in the 0th
		// next min in the 1st, etc)
//		int end = (start + count);
		for (int ii = start; ii <= end; ii++) {
			minPos = ii;
			for (int jj = ii; jj <= end; jj++) {
				if (array[minPos] > array[jj]) {
					minPos = jj;
				}
			}
			//perform the swap
			int tmp = array[ii];
			array[ii] = array[minPos];
			array[minPos] = tmp;
//			swap(ii, minPos);
		}
	}

}
