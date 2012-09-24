import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;


public class Profiler {

	
	private class ProfilerEntry {
		private long startTimeMS = System.currentTimeMillis();
		private long stopTimeMS;
	}
	
	private List<String> keysInOrder = new ArrayList<String>();
	private Map<String, List<ProfilerEntry>> entries = new HashMap<String, List<ProfilerEntry>>();
	
	public int start(String name) {
		List<ProfilerEntry> list = entries.get(name);
		if (list == null) {
			list = new ArrayList<ProfilerEntry>();
			entries.put(name, list);
			keysInOrder.add(name);
		}
		
		list.add(new ProfilerEntry());
		return list.size() - 1;
	}
	
	public void stop(String name, int token) {
		List<ProfilerEntry> list = entries.get(name);
		if (list == null) {
			list = new ArrayList<ProfilerEntry>();
			entries.put(name, list);
		}
		
		ProfilerEntry pe = list.get(token);
		pe.stopTimeMS = System.currentTimeMillis();
	}
	
	@Override
	public String toString() {
		StringBuilder builder = new StringBuilder();
		builder.append("All times in microseconds").append("\n");
		for (String key : keysInOrder) {
			List<ProfilerEntry> list = entries.get(key);
			double avg = 0;
			double max = Double.MIN_VALUE;
			double min = Double.MAX_VALUE;
			double total = 0;
			for (ProfilerEntry e : list) {
				long elapsedUS = 1000 * (e.stopTimeMS - e.startTimeMS);
				avg += elapsedUS;
				max = Math.max(max, elapsedUS);
				min = Math.min(min, elapsedUS);
				total += elapsedUS;
			}
			avg /= list.size();
			
			builder.append(key).append(":").append("\tcount:").append(list.size())
			.append("\ttotal: ").append((int)total).append("\tavg: ").append((int)avg).append("\tmax: ").append((int)max).append("\tmin: ").append((int)min).append("\n");
		}
		return builder.toString();
	}
}
