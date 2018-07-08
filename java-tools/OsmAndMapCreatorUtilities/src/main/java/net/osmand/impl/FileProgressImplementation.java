package net.osmand.impl;

import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;


public class FileProgressImplementation extends ConsoleProgressImplementation {
	
	private long length;
	private long read = 0;
	private File file;
	private long lastPrintedTime;
	private long lastPrintedBytes;
	
	public FileProgressImplementation(String taskName, File toRead) {
		file = toRead;
		length = toRead.length();
		startTaskLong(taskName, length);
	}
	
	
	
	public void update() {
		remainingLong(length - read);
	}
	
	protected String getPrintMessage() {
		long now = System.currentTimeMillis();
		double spd = 0;
		if(lastPrintedTime != 0 && now - lastPrintedTime > 0) {
			spd = (((double)read - lastPrintedBytes) / (1024.0 * 1024.0)) / ((now - lastPrintedTime) / 1000.0) ;
		}
		String msg = String.format("Done %.2f%% %.2f MBs", getCurrentPercent(), spd);
		lastPrintedTime = now;
		lastPrintedBytes = read;
		return msg;
	}
	
	
	public InputStream openFileInputStream() throws IOException{
		FileInputStream is = new FileInputStream(file);
		return new InputStream() {

			@Override
			public int read() throws IOException {
				int r = is.read();
				if(r >= 0) {
					read ++;
				}
				return r;
			}
			
			@Override
			public int read(byte[] b, int off, int len) throws IOException {
				int rd = is.read(b, off, len);
				if(rd >= 0) {
					read += rd;
				}
				return rd;
			}
			
			@Override
			public long skip(long n) throws IOException {
				long skip = is.skip(n);
				if(skip >= 0) {
					read += skip;
				}
				return skip;
			}
			
			@Override
			public int read(byte[] b) throws IOException {
				int rd = is.read(b);
				if(rd >= 0) {
					read += rd;
				}
				return rd;
			}
			
			@Override
			public void close() throws IOException {
				is.close();
			}
			
		};
	}
}
