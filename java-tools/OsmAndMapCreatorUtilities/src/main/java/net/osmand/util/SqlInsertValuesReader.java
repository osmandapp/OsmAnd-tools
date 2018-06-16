package net.osmand.util;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.util.ArrayList;
import java.util.List;
import java.util.zip.GZIPInputStream;

public class SqlInsertValuesReader {

	public interface InsertValueProcessor {
		public void process(List<String> vs);

	}

	public static void readInsertValuesFile(final String fileName, InsertValueProcessor p) throws IOException {
		InputStream fis = new FileInputStream(fileName);
		if (fileName.endsWith("gz")) {
			fis = new GZIPInputStream(fis);
		}
		readInsertValuesFile(fis, p);
	}
	public static void readInsertValuesFile(final InputStream fis, InsertValueProcessor p) throws IOException {
		InputStreamReader read = new InputStreamReader(fis, "UTF-8");
		char[] cbuf = new char[1000];
		int cnt;

		String buf = "";
		List<String> insValues = new ArrayList<String>();
		boolean values = false;
		boolean openInsValues = false;
		boolean openWord = false;
		while ((cnt = read.read(cbuf)) >= 0) {
			String str = new String(cbuf, 0, cnt);
			buf += str;
			boolean processed = true;
			while (processed) {
				processed = false;
				if (!values) {
					int ind = buf.indexOf("VALUES");
					if (ind != -1) {
						buf = buf.substring(ind + "VALUES".length());
						values = true;
						processed = true;
					}
				} else if (!openInsValues) {
					int ind = buf.indexOf("(");
					if (ind != -1) {
						buf = buf.substring(ind + 1);
						openInsValues = true;
						insValues.clear();
						processed = true;
					}
				} else if (!openWord) {
					StringBuilder number = new StringBuilder(100);
					for (int k = 0; k < buf.length(); k++) {
						char ch = buf.charAt(k);
						if (ch == '\'') {
							openWord = true;
							processed = true;
						} else if (ch == ')') {
							if (number.toString().trim().length() > 0) {
								insValues.add(number.toString().trim());
							}

							try {
								p.process(insValues);
							} catch (Exception e) {
								System.err.println(e.getMessage() + " " + insValues);
							}
							openInsValues = false;
							processed = true;
						} else if (ch == ',') {
							if (number.toString().trim().length() > 0) {
								insValues.add(number.toString().trim());
							}
							processed = true;
						} else {
							number.append(ch);
						}
						if (processed) {
							buf = buf.substring(k + 1);
							break;
						}
					}
				} else if (openWord) {
					StringBuilder word = new StringBuilder(100);
					boolean escape = false;
					for (int k = 0; k < buf.length(); k++) {
						char ch = buf.charAt(k);
						if (escape) {
							word.append(ch);
							escape = false;
						} else {
							if (ch == '\'') {
								insValues.add(word.toString());
								processed = true;
								openWord = false;
								buf = buf.substring(k + 1);
								break;
							} else if (ch == '\\') {
								escape = true;
							} else {
								word.append(ch);
							}
						}
					}
				}
			}

		}
		read.close();
	}

}
