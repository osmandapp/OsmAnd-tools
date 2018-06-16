package net.osmand.server;

import java.io.IOException;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

import org.json.JSONException;

import net.osmand.bitcoinsender.TransactionAnalyzer;
import net.osmand.live.subscriptions.UpdateSubscription;

public class ServerUtilities {

	public static void main(String[] args) throws IOException, JSONException, ClassNotFoundException, SQLException {
		String utl = args[0];
		List<String> subArgs = new ArrayList<String>(Arrays.asList(args).subList(1, args.length));
		String[] subArgsArray = subArgs.toArray(new String[args.length - 1]);
		if (utl.equals("generate-report-underpaid")) {
			TransactionAnalyzer.main(subArgsArray);
		} else if (utl.equals("update-subscriptions")) {
			UpdateSubscription.main(subArgsArray);
		}
	}
}
