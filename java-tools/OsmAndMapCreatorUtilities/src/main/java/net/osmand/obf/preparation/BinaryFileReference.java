package net.osmand.obf.preparation;


import java.io.IOException;
import java.io.RandomAccessFile;

/**
 * Utility to keep references between binary blocks
 * while generating binary file
 */
public class BinaryFileReference {

	private final long pointerToWrite;
	private final long pointerToCalculateShiftFrom;
	private final boolean _8bit;
	private long pointerToCalculateShiftTo;

	public BinaryFileReference(long pointerToWrite, long pointerToCalculateShiftFrom, boolean _8bit) {
		this.pointerToWrite = pointerToWrite;
		this.pointerToCalculateShiftFrom = pointerToCalculateShiftFrom;
		this._8bit = _8bit;
	}

	public long getStartPointer() {
		return pointerToCalculateShiftFrom;
	}

	public long writeReference(RandomAccessFile raf, long pointerToCalculateShifTo) throws IOException {
		this.pointerToCalculateShiftTo = pointerToCalculateShifTo;
		long currentPosition = raf.getFilePointer();
		raf.seek(pointerToWrite);
		long val = pointerToCalculateShiftTo - pointerToCalculateShiftFrom;
		if (_8bit) {
			raf.writeLong(val | (1L << 63)); // mark highest bit to 1 as long
		} else if (val < Integer.MAX_VALUE) {
			raf.writeInt((int) val);
		} else {
			throw new IllegalStateException("Out of bounds value: " + val);
		}
		raf.seek(currentPosition);
		return val;
	}

	public static BinaryFileReference createLongSizeReference(long pointerToWrite){
		return new BinaryFileReference(pointerToWrite, pointerToWrite + 8, true);
	}
	
	public static BinaryFileReference createSizeReference(long pointerToWrite){
		return new BinaryFileReference(pointerToWrite, pointerToWrite + 4, false);
	}

	public static BinaryFileReference createShiftReference(long pointerToWrite, long pointerShiftFrom){
		return new BinaryFileReference(pointerToWrite, pointerShiftFrom, false);
	}


}
