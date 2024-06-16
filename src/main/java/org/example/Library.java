package org.example;

import org.apache.zookeeper.*;
import org.apache.zookeeper.data.Stat;

import java.io.IOException;
import java.util.*;
import java.util.stream.Collectors;

public class Library implements Map<String, String> {

	private final String address;
	private final int timeout;
	private final ZooKeeper zooKeeper;

	private int size;

	public Library(String address, int timeout) throws IOException {
		this.address = address;
		this.timeout = timeout;
		this.zooKeeper = new ZooKeeper(this.address, this.timeout, watchedEvent -> {});
	}

	@Override
	public int size() {
		return size;
	}

	@Override
	public boolean isEmpty() {
		return size == 0;
	}

	@Override
	public boolean containsKey(Object key) {
		try {
			return zooKeeper.exists(String.valueOf(key), false) != null;
		} catch (KeeperException | InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public boolean containsValue(Object value) {
		try {
			Deque<String> znodes = (Deque<String>) zooKeeper.getChildren("/", false);
			String valueString = String.valueOf(value);
			while (!znodes.isEmpty()) {
				String znode = znodes.removeFirst();
				byte[] data = zooKeeper.getData(znode, false, new Stat());
				String dataString = new String(data);
				if (dataString.equals(valueString)) {
					return true;
				}

				List<String> children = zooKeeper.getChildren(znode, false);
				znodes.addAll(children);
			}
		} catch (KeeperException | InterruptedException e) {
			throw new RuntimeException(e);
		}

		return false;
	}

	@Override
	public String get(Object key) {
		try {
			if (this.containsKey(key)) {
				return new String(zooKeeper.getData(String.valueOf(key), false, new Stat()));
			}
		} catch (KeeperException | InterruptedException e) {
			throw new RuntimeException(e);
		}
		return null;
	}

	@Override
	public String put(String key, String value) {
		try {
			String prevValue = null;
			if (this.containsKey(key)) {
				prevValue = new String(zooKeeper.getData(key, false, new Stat()));
				zooKeeper.setData(key, value.getBytes(), -1);
			} else {
				zooKeeper.create(key, value.getBytes(), ZooDefs.Ids.OPEN_ACL_UNSAFE, CreateMode.PERSISTENT);
				size++;
			}
			return prevValue;
		} catch (KeeperException | InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public String remove(Object key) {
		try {
			String stringKey = String.valueOf(key);
			String prevValue = null;
			if (this.containsKey(key)) {
				prevValue = new String(zooKeeper.getData(stringKey, false, new Stat()));
				zooKeeper.delete(stringKey, -1);
				size--;
			}
			return prevValue;
		} catch (KeeperException | InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	@Override
	public void putAll(Map<? extends String, ? extends String> m) {
		for (Entry<? extends String, ? extends String> entry : m.entrySet()) {
			this.put(entry.getKey(), entry.getValue());
		}
	}

	@Override
	public void clear() {
		for (Entry<String, String> entry : this.entrySet()) {
			this.remove(entry.getKey());
		}
	}

	@Override
	public Set<String> keySet() {
		Set<Entry<String, String>> entrySet = this.entrySet();
		return entrySet.stream().map(Entry::getKey).collect(Collectors.toSet());
	}

	@Override
	public Collection<String> values() {
		Set<Entry<String, String>> entrySet = this.entrySet();
		return entrySet.stream().map(Entry::getValue).collect(Collectors.toSet());
	}

	@Override
	public Set<Entry<String, String>> entrySet() {
		try {
			Set<Entry<String, String>> entrySet = new HashSet<>();
			Deque<String> znodes = (Deque<String>) zooKeeper.getChildren("/", false);
			while (!znodes.isEmpty()) {
				String znode = znodes.removeFirst();
				byte[] data = zooKeeper.getData(znode, false, new Stat());
				String dataString = new String(data);
				entrySet.add(new LibraryEntry(znode, dataString));
				List<String> children = zooKeeper.getChildren(znode, false);
				znodes.addAll(children);
			}
			return entrySet;
		} catch (KeeperException | InterruptedException e) {
			throw new RuntimeException(e);
		}
	}

	private static final class LibraryEntry implements Entry<String, String> {
		private final String key;
		private String value;

		public LibraryEntry(String key, String value) {
			this.key = key;
			this.value = value;
		}

		@Override
		public String getKey() {
			return key;
		}

		@Override
		public String getValue() {
			return value;
		}

		@Override
		public String setValue(String value) {
			String old = this.value;
			this.value = value;
			return old;
		}
	}

}