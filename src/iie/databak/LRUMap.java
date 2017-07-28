package iie.databak;

import java.util.LinkedHashMap;

public class LRUMap<K, V> extends LinkedHashMap<K, V> {

	private int maxEntrys;
	
	public LRUMap(int maxEntrys) {
		super(maxEntrys,0.75f,true);
		this.maxEntrys = maxEntrys;
	}

	@Override
	protected boolean removeEldestEntry(java.util.Map.Entry<K, V> eldest) {
		return size() > maxEntrys;
	}

}
