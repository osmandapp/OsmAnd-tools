package net.osmand.exceptionanalyzer;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.repackaged.org.apache.commons.codec.binary.Base64;
import com.google.api.client.util.store.FileDataStoreFactory;
import com.google.api.services.gmail.GmailScopes;
import com.google.api.services.gmail.model.*;
import com.google.api.services.gmail.Gmail;

import java.io.*;
import java.text.SimpleDateFormat;
import java.util.*;

import net.osmand.exceptionanalyzer.data.ExceptionText;

public class ExceptionAnalyzerMain {
    private static final String LABEL = "OsmAnd Bug/May";
    private static final String VERSION_FILTER = "2.6.3";

	/** Application name. */
    private static final String APPLICATION_NAME = "ExceptionAnalyzer";
    
    private static final File FOLDER_WITH_LOGS =  new File(System.getProperty("user.home") + "/"+"attachments_logs");

    /** Directory to store user credentials for this application. */
    private static final java.io.File DATA_STORE_DIR = new java.io.File(
            System.getProperty("user.home"), ".credentials/gmail-java-quickstart");

    /** Global instance of the {@link FileDataStoreFactory}. */
    private static FileDataStoreFactory DATA_STORE_FACTORY;

    /** Global instance of the JSON factory. */
    private static final JsonFactory JSON_FACTORY =
            JacksonFactory.getDefaultInstance();

    /** Global instance of the HTTP transport. */
    private static HttpTransport HTTP_TRANSPORT;

    /** Global instance of the scopes required by this quickstart.
     *
     * If modifying these scopes, delete your previously saved credentials
     * at ~/.credentials/gmail-java-quickstart
     */
    private static final List<String> SCOPES = Arrays.asList(GmailScopes.GMAIL_READONLY);

    static {
        try {
            HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
            DATA_STORE_FACTORY = new FileDataStoreFactory(DATA_STORE_DIR);
        } catch (Throwable t) {
            t.printStackTrace();
            System.exit(1);
        }
    }

    /**
     * Creates an authorized Credential object.
     * @return an authorized Credential object.
     * @throws IOException
     */
    public static Credential authorize() throws IOException {
        // Load client secrets.
        InputStream in =
                ExceptionAnalyzerMain.class.getResourceAsStream("/client_secret.json");
        GoogleClientSecrets clientSecrets =
                GoogleClientSecrets.load(JSON_FACTORY, new InputStreamReader(in));

        // Build flow and trigger user authorization request.
        GoogleAuthorizationCodeFlow flow =
                new GoogleAuthorizationCodeFlow.Builder(
                        HTTP_TRANSPORT, JSON_FACTORY, clientSecrets, SCOPES)
                        .setDataStoreFactory(DATA_STORE_FACTORY)
                        .setAccessType("offline")
                        .build();
        Credential credential = new AuthorizationCodeInstalledApp(
                flow, new LocalServerReceiver()).authorize("user");
        System.out.println(
                "Credentials saved to " + DATA_STORE_DIR.getAbsolutePath());
        return credential;
    }

    /**
     * Build and return an authorized Gmail client service.
     * @return an authorized Gmail client service
     * @throws IOException
     */
    public static Gmail getGmailService() throws IOException {
        Credential credential = authorize();
        return new Gmail.Builder(HTTP_TRANSPORT, JSON_FACTORY, credential).setApplicationName(APPLICATION_NAME)
                .build();
    }

    public static void main(String[] args) throws IOException {
//        downloadAttachments();
        makeReport();
    }

	private static void makeReport() {
		System.out.println("Analyzing the exceptions...");
        Map<String, List<ExceptionText>> result = analyzeExceptions(VERSION_FILTER);
        writeResultToFile(result);
	}

	private static void downloadAttachments() throws IOException {
		// Build a new authorized API client service.
        Gmail service = getGmailService();

        // Print the labels in the user's account.
        String user = "me";
        ListLabelsResponse listResponse =
                service.users().labels().list(user).execute();
        List<Label> labels = listResponse.getLabels();
        if (labels.size() == 0) {
            System.out.println("No labels found.");
        }
        System.out.println("Labels:");
        for (Label label : labels) {
            if (label.getName().toUpperCase().equals(LABEL.toUpperCase())) {
                List<String> trash = new ArrayList<>();
                trash.add(label.getId());
                List<Message> result = listMessagesWithLabels(service, user, trash);
				System.out.println("Messages in " + LABEL + ": " + result.size());
                getAttachments(result, user, service);
            }
        }
	}

    /**
     * List all Messages of the user's mailbox with labelIds applied.
     *
     * @param service Authorized Gmail API instance.
     * @param userId User's email address. The special value "me"
     * can be used to indicate the authenticated user.
     * @param labelIds Only return Messages with these labelIds applied.
     * @throws IOException
     */
    public static List<Message> listMessagesWithLabels(Gmail service, String userId,
                                                       List<String> labelIds) throws IOException {
        ListMessagesResponse response = service.users().messages().list(userId)
                .setLabelIds(labelIds).execute();

        List<Message> messages = new ArrayList<>();
        while (response.getMessages() != null) {
            messages.addAll(response.getMessages());
            if (response.getNextPageToken() != null) {
                String pageToken = response.getNextPageToken();
                response = service.users().messages().list(userId).setLabelIds(labelIds)
                        .setPageToken(pageToken).execute();
            } else {
                break;
            }
        }

        return messages;
    }

    public static void getAttachments(List<Message> messages, String userId, Gmail service)
            throws IOException {
        int count = 0;
        SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd HH_mm_ss");
        String prev = null;
        for (Message messageRef : messages) {
            String messageId = messageRef.getId();
            Message message = service.users().messages().get(userId, messageId).execute();
            List<MessagePart> parts = message.getPayload().getParts();
            if (parts != null) {
            	String msgId = sdfDate.format(new Date(message.getInternalDate())) ;
            	if(msgId.equals(prev)) {
            		msgId += "_2";
            	}
            	prev = msgId;
            	count++;
                for (MessagePart part : parts) {
                    if (part.getFilename() != null && part.getFilename().length() > 0) {
                        String filename = part.getFilename();
                        File exception = new File(FOLDER_WITH_LOGS, msgId + "." + filename);
                        if(exception.exists()) {
                        	System.out.println("Attachment already downloaded!");
                        } else {
                            System.out.println("Downloading attachment: " + msgId + "." + filename);
                            String attId = part.getBody().getAttachmentId();
                            MessagePartBody attachPart = service.users().messages().attachments().
                                    get(userId, messageId, attId).execute();
                            Base64 base64Url = new Base64(true);
                            byte[] fileByteArray = base64Url.decodeBase64(attachPart.getData());
                            if (!FOLDER_WITH_LOGS.exists()) {
                                FOLDER_WITH_LOGS.mkdirs();
                            }
                            exception.createNewFile();
                            FileOutputStream fileOutFile = new FileOutputStream(exception);
                            fileOutFile.write(fileByteArray);
                            fileOutFile.close();
                            System.out.println("Attachment saved!");
                            exception.setLastModified(message.getInternalDate());
                        }
                    }
                }
            }
        }
        System.out.println("Number of files: " + count);
    }

    public static Map<String, List<ExceptionText>> analyzeExceptions(String versionFilter) {
        Map<String, List<ExceptionText>> result = new HashMap<>();
        if (FOLDER_WITH_LOGS.exists()) {
            for(File currLog : FOLDER_WITH_LOGS.listFiles()) {
            	if(!currLog.getName().endsWith(".exception.log")) {
            		continue;
            	}
                try{
                    FileInputStream fstream = new FileInputStream(currLog);
                    BufferedReader br = new BufferedReader(new InputStreamReader(fstream));
                    String strLine;

                    /* read log line by line */
                    while ((strLine = br.readLine()) != null)   {
                        String currDate = "";
                        String currApkVersion = "";
                        String currBody = "";
                        String currName = "";
                        if (strLine.matches("[0-9]+.[0-9]+.[0-9]+ [0-9]+:[0-9]+:[0-9]+")) {
                            currDate = strLine;
                            strLine = br.readLine();
                            int indexOfColumn = strLine.indexOf(":");
                            currApkVersion = strLine.substring(indexOfColumn + 1, strLine.length());
                            br.readLine();
                            strLine = br.readLine();
                            if (strLine.contains(":")) {
                                int columnIndex = strLine.indexOf(":");
                                strLine = strLine.substring(0, columnIndex);
                            }
                            currName = strLine;
                            while (!strLine.contains("Version  OsmAnd") && (strLine = br.readLine()) != null) {
                                currBody += strLine + "\n";
                            }
                            ExceptionText exception = new ExceptionText(currDate, currName, currBody, currApkVersion);
                            if(versionFilter != null && !currApkVersion.contains(versionFilter)) {
                            	continue;
                            }
                            String hsh = exception.getExceptionHash();
                            if (!result.containsKey(hsh)) {
                            	List<ExceptionText> exceptions = new ArrayList<>();
                            	result.put(hsh, exceptions);
                            }
                            result.get(hsh).add(exception);
                        }
                    }
                    br.close();
                    fstream.close();
                } catch (Exception e) {
                    System.err.println("Error: " + e.getMessage());
                }
            }
        }
        return result;
    }

    public static void writeResultToFile(Map<String, List<ExceptionText>> mapToAnalyze) {
        File output = new File(FOLDER_WITH_LOGS, "results.csv");
        if (output.exists()) {
            output.delete();
            try {
                output.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        else {
            try {
                output.createNewFile();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        PrintWriter pw = null;
        try {
            pw = new PrintWriter(output);
        } catch (FileNotFoundException e) {
            e.printStackTrace();
        }
        final Map<String, List<ExceptionText>> map = mapToAnalyze;
        List<String> st = new ArrayList<>(map.keySet());
        Collections.sort(st,new Comparator<String>() {

			@Override
			public int compare(String o1, String o2) {
				return -Integer.compare(map.get(o1).size(), map.get(o2).size());
			}
		});
        pw.write("Name,Version,Date,Count,Stacktrace\n");

        for (String key : st) {
            StringBuilder sb = new StringBuilder();
            List<ExceptionText> exceptions = map.get(key);
            ExceptionText exception = exceptions.get(0);
            //for (ExceptionText exception: exceptions) {
                String info = exception.getAllInfo().toString()
                        .replace("[", "")
                        .replace("]", "");
                sb.append(info);
                sb.append(',');
                sb.append(exceptions.size());
                sb.append(',');
                String body = exception.getStackTrace().replaceAll("\n", "").replaceAll("\tat", " ");
                sb.append(body);
                sb.append('\n');
            //}

            pw.write(sb.toString());
        }


        pw.close();

        System.out.println("done!");
    }

}