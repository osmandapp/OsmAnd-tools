package net.osmand.router;

import java.util.Collection;
import java.util.Map;

import gnu.trove.function.TObjectFunction;
import gnu.trove.iterator.TLongObjectIterator;
import gnu.trove.map.TLongObjectMap;
import gnu.trove.procedure.TLongObjectProcedure;
import gnu.trove.procedure.TLongProcedure;
import gnu.trove.procedure.TObjectProcedure;
import gnu.trove.set.TLongSet;

public class ExcludeTLongObjectMap<T> implements TLongObjectMap<T> {

	int size = 0;
	long[] keys;
	TLongObjectMap<T> map;

	public ExcludeTLongObjectMap(TLongObjectMap<T> map, long... keys) {
		this.map = map;
		this.keys = keys;
	}

	@Override
	public long getNoEntryKey() {
		throw new UnsupportedOperationException();
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
	public boolean containsKey(long key) {
		if (checkException(key)) {
			return false;
		}
		return map.containsKey(key);
	}

	private boolean checkException(long key) {
		for(long k : keys) {
			if(key == k) {
				return true;
			}
		}
		return false;
	}

	@Override
	public boolean containsValue(Object value) {
		throw new UnsupportedOperationException();
	}

	@Override
	public T get(long key) {
		if(checkException(key)) {
			return null;
		}
		return map.get(key);
	}

	@Override
	public T put(long key, T value) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public T putIfAbsent(long key, T value) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public T remove(long key) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public void putAll(Map<? extends Long, ? extends T> m) {
		// TODO Auto-generated method stub

	}

	@Override
	public void putAll(TLongObjectMap<? extends T> map) {
		// TODO Auto-generated method stub

	}

	@Override
	public void clear() {
		// TODO Auto-generated method stub

	}

	@Override
	public TLongSet keySet() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long[] keys() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public long[] keys(long[] array) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Collection<T> valueCollection() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public Object[] values() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public T[] values(T[] array) {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public TLongObjectIterator<T> iterator() {
		// TODO Auto-generated method stub
		return null;
	}

	@Override
	public boolean forEachKey(TLongProcedure procedure) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean forEachValue(TObjectProcedure<? super T> procedure) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public boolean forEachEntry(TLongObjectProcedure<? super T> procedure) {
		// TODO Auto-generated method stub
		return false;
	}

	@Override
	public void transformValues(TObjectFunction<T, T> function) {
		// TODO Auto-generated method stub

	}

	@Override
	public boolean retainEntries(TLongObjectProcedure<? super T> procedure) {
		// TODO Auto-generated method stub
		return false;
	}

}