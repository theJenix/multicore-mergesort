
/**
 * An implementation of SimpleJobQueue to use JavaSpaces tuplespace implementation.  A tuplespace is effectively a distributed
 * queue: first in, first out, no directed messages.  Processes that enqueue work to a tuplespace assume any available process
 * can complete the work, and processes that poll from a tuplespace can get messages from anyone: this makes it very handy
 * when used with divide and conquer algorithms (such as mergesort) where source of the message matters less than "is there something
 * to split?" and "is there 2 things to merge?" 
 * 
 * @author Jesse Rosalia
 *
 */
public class JavaSpacesDistributedQueue implements SimpleJobQueue {

	@Override
	public void broadcast(int method) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public UnitOfWork dequeue() {
		// TODO Auto-generated method stub
		return null;
	}
	
	@Override
	public UnitOfWork dequeue(int method) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void enqueue(int method, UnitOfWork work) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public UnitOfWork poll(int method) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void broadcast(int method, UnitOfWork work) {
		// TODO Auto-generated method stub
		
	}

	@Override
	public UnitOfWork poll() {
		// TODO Auto-generated method stub
		return null;
	}

}
