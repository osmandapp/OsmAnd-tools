package net.osmand.mailsender;

import net.osmand.mailsender.authorization.DataProvider;

import java.io.IOException;
import java.security.GeneralSecurityException;
import java.util.List;

public class EmailSenderMain {

    public static void main(String[] args) throws IOException, GeneralSecurityException {
        String id = null;
        String range = null;
        if (args.length > 1) {
            id = args[0];
            range = args[1];
        } else {
            System.out.println("Usage: <sheet_id> <data_range>");
            System.exit(1);
        }
        List<List<Object>> data = DataProvider.getValues(id, range);
        if (data != null)
            System.out.print(data.toString());
    }
}
