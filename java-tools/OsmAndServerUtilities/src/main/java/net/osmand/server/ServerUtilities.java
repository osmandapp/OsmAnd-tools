package net.osmand.server;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import net.osmand.bitcoinsender.TransactionAnalyzer;

public class ServerUtilities {

	public static void main(String[] args) throws IOException {
		String utl = args[0];
		List<String> subArgs = new ArrayList<String>(Arrays.asList(args).subList(1, args.length));
		String[] subArgsArray = subArgs.toArray(new String[args.length - 1]);
		if (utl.equals("generate-report-underpaid")) {
			TransactionAnalyzer.main(subArgsArray);
		}
	}
}
