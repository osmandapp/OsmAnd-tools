package net.osmand.exceptionanalyzer;

import com.google.api.client.auth.oauth2.Credential;
import com.google.api.client.extensions.java6.auth.oauth2.AuthorizationCodeInstalledApp;
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver;
import com.google.api.client.extensions.jetty.auth.oauth2.LocalServerReceiver.Builder;
import com.google.api.client.googleapis.auth.oauth2.GoogleAuthorizationCodeFlow;
import com.google.api.client.googleapis.auth.oauth2.GoogleClientSecrets;
import com.google.api.client.googleapis.javanet.GoogleNetHttpTransport;
import com.google.api.client.http.HttpTransport;
import com.google.api.client.json.jackson2.JacksonFactory;
import com.google.api.client.json.JsonFactory;
import com.google.api.client.util.store.FileDataStoreFactory;
import com.google.api.services.gmail.GmailScopes;
import com.google.api.services.gmail.model.*;
import com.google.api.services.gmail.Gmail;

import java.io.*;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;

import net.osmand.exceptionanalyzer.data.ExceptionText;
import net.osmand.util.Algorithms;

public class ExceptionAnalyzerMain {
	
	private static final String APPLICATION_NAME = "ExceptionAnalyzer";
	
	private static final List<String> SCOPES = Arrays.asList(GmailScopes.GMAIL_READONLY);
	
	private static class Variables {
	
		private String LABEL= "Crash";
		private String VERSION = "3.1";
	    public int LIMIT = 1000;
		private boolean DOWNLOAD_MESSAGES = true;
	    private File FOLDER_WITH_LOGS;
	    private File HOME_FILE;
	    
	    private JsonFactory JSON_FACTORY = JacksonFactory.getDefaultInstance();
	    
	    /** Global instance of the scopes required by this quickstart.
	     *
	     * If modifying these scopes, delete your previously saved credentials
	     * at ~/.credentials/gmail-java-quickstart
	     */
	    private FileDataStoreFactory DATA_STORE_FACTORY;
	    /** Directory to store user credentials for this application. */
	    private File DATA_STORE_DIR;
	    /** Global instance of the HTTP transport. */
	    private HttpTransport HTTP_TRANSPORT;
		
	    
	    public Variables(String home) throws Exception {
	    	HOME_FILE = new File(home);
			FOLDER_WITH_LOGS = new File(HOME_FILE, "attachments_logs");
	    	DATA_STORE_DIR = new File(HOME_FILE, ".credentials/gmail-java-quickstart");
	        HTTP_TRANSPORT = GoogleNetHttpTransport.newTrustedTransport();
	        DATA_STORE_FACTORY = new FileDataStoreFactory(DATA_STORE_DIR);
	    }
	    
	}
	
	public static void main(String[] args) throws Exception {

		String version = null;
		String label = null;
		String clientSecretJson = "";
		String home = System.getProperty("user.home");
		boolean clean = false;
		int limit = -1;
		for (String s : args) {
			String[] sk = s.split("=");
			if (sk[0].equals("--label")) {
				label = sk[1];
			} else if (sk[0].equals("--version")) {
				version = sk[1];
			} else if (sk[0].equals("--clean")) {
				clean = true;
			} else if (sk[0].equals("--home")) {
				home = sk[1];
			} else if (sk[0].equals("--limit")) {
				limit = Integer.parseInt(sk[1]);
			} else if (sk[0].equals("--clientSecretJson")) {
				clientSecretJson = sk[1];
			}
		}
		Variables vars = new Variables(home);
		vars.FOLDER_WITH_LOGS.mkdirs();
		if (clean) {
			Algorithms.removeAllFiles(vars.FOLDER_WITH_LOGS);
			vars.FOLDER_WITH_LOGS.mkdirs();
		}
		vars.DATA_STORE_DIR.mkdirs();
		if (label != null) {
			vars.LABEL = label;
		}
		if (limit != -1) {
			vars.LIMIT = limit;
		}
		if (version != null) {
			vars.VERSION = version;
		}

		System.out.println(String.format(
				"Utility to download exceptions."
						+ "\nDownload emails with label='%s' (change with --label=, leave empty to skip) to %s. "
						+ "\nMake report with version='%s' (change with --version=, leave empty to skip).",
				label, vars.FOLDER_WITH_LOGS.getAbsolutePath(), version));
		if (vars.DOWNLOAD_MESSAGES && !Algorithms.isEmpty(vars.LABEL)) {
			downloadAttachments(clientSecretJson, vars);
		}
		if (!Algorithms.isEmpty(vars.VERSION)) {
			makeReport(vars);
		}
	}
	
    /**
     * Creates an authorized Credential object.
     * @param vars 
     * @return an authorized Credential object.
     * @throws IOException
     */
    public static Credential authorize(String clientSecretJson, Variables vars) throws IOException {
        // Load client secrets.
        InputStream in = Algorithms.isEmpty(clientSecretJson)  ?
                ExceptionAnalyzerMain.class.getResourceAsStream("/client_secret.json") :
                new FileInputStream(clientSecretJson);
        GoogleClientSecrets clientSecrets =
                GoogleClientSecrets.load(vars.JSON_FACTORY, new InputStreamReader(in));
        in.close();
        // Build flow and trigger user authorization request.
        GoogleAuthorizationCodeFlow flow =
                new GoogleAuthorizationCodeFlow.Builder(
                		vars.HTTP_TRANSPORT, vars.JSON_FACTORY, clientSecrets, SCOPES)
                        .setDataStoreFactory(vars.DATA_STORE_FACTORY)
                        .setAccessType("offline")
                        .build();
        Builder bld = new LocalServerReceiver.Builder();
        bld.setPort(5000);
        Credential credential = new AuthorizationCodeInstalledApp(
                flow, bld.build()).authorize("user");
        System.out.println(
                "Credentials saved to " + vars.DATA_STORE_DIR.getAbsolutePath());
        return credential;
    }

    /**
     * Build and return an authorized Gmail client service.
     * @param vars 
     * @return an authorized Gmail client service
     * @throws IOException
     */
    public static Gmail getGmailService(String clientSecretFile, Variables vars) throws IOException {
        Credential credential = authorize(clientSecretFile, vars);
        return new Gmail.Builder(vars.HTTP_TRANSPORT, vars.JSON_FACTORY, credential).setApplicationName(APPLICATION_NAME)
                .build();
    }

    

	public static void makeReport(Variables vars) {
		System.out.println("Analyzing the exceptions...");
        Map<String, List<ExceptionText>> result = analyzeExceptions(vars);
        writeResultToFile(result, vars);
	}

	private static void downloadAttachments(String clientSecretFile, Variables vars) throws IOException {
		// Build a new authorized API client service.
        Gmail service = getGmailService(clientSecretFile, vars);

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
            if (label.getName().toUpperCase().equals(vars.LABEL.toUpperCase())) {
                List<String> labelIds = new ArrayList<>();
                labelIds.add(label.getId());
                List<Message> result = listMessagesWithLabels(service, user, labelIds, vars.LIMIT);
				System.out.println("Messages in " + vars.LABEL + ": " + result.size());
                getAttachments(result, user, service, vars);
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
                                                       List<String> labelIds, int limit) throws IOException {
        ListMessagesResponse response = service.users().messages().list(userId)
                .setLabelIds(labelIds).execute();

        List<Message> messages = new ArrayList<>();
		while (messages.size() < limit && response.getMessages() != null) {
			messages.addAll(response.getMessages());
			if (response.getNextPageToken() != null) {
				String pageToken = response.getNextPageToken();
				response = service.users().messages().list(userId).setLabelIds(labelIds).setPageToken(pageToken)
						.execute();
			} else {
				break;
			}
		}
        return messages;
    }

    public static void getAttachments(List<Message> messages, String userId, Gmail service, Variables vars)
            throws IOException {
        int count = 0;
        SimpleDateFormat sdfDate = new SimpleDateFormat("yyyy-MM-dd");
        String prev = null;
        Set<String> users = new TreeSet<>();
        Set<String> messageIdsLoaded = new TreeSet<>();
        
        for(File fld : vars.FOLDER_WITH_LOGS.listFiles()) {
        	if(fld.getName().endsWith(".exception.log")) {
        		String nm = fld.getName();
        		String mid = nm.substring(0, nm.length() - ".exception.log".length());
        		mid = mid.substring(nm.indexOf('.') + 1);
        		messageIdsLoaded.add(mid);
        	}
        }
        
        long lastSaved = System.currentTimeMillis();
        for (Message messageRef : messages) {
            String messageId = messageRef.getId();
            if(messageIdsLoaded.contains(messageId)) {
            	System.out.println("Attachment already downloaded " + messageId + " " + (System.currentTimeMillis() - lastSaved) + " ms !");
            	lastSaved = System.currentTimeMillis();
            	continue;
            }
            Message message = service.users().messages().get(userId, messageId).execute();
            String from = "";
            for(MessagePartHeader hdr: message.getPayload().getHeaders()) {
            	if(hdr.getName().equals("From")) {
            		from = hdr.getValue();
            		break;
            	}
            }
            
            List<MessagePart> parts = message.getPayload().getParts();
            if (parts != null) {
            	String msgId = sdfDate.format(new Date(message.getInternalDate())) + "." + messageId ;
            	if(msgId.equals(prev)) {
            		msgId += "_2";
            	}
            	prev = msgId;
            	count++;
            	users.add(from);
                for (MessagePart part : parts) {
                    if (part.getFilename() != null && part.getFilename().length() > 0) {
                        String filename = part.getFilename();
                        File exception = new File(vars.FOLDER_WITH_LOGS, msgId + "." + filename);
                        File uid = new File(vars.FOLDER_WITH_LOGS, msgId + ".uid.txt");
                        if(!uid.exists()) {
                        	FileOutputStream fileOutFile = new FileOutputStream(uid);
                            fileOutFile.write(from.getBytes());
                            fileOutFile.close();
                            uid.setLastModified(message.getInternalDate());
                        }
						if (exception.exists()) {
							System.out.println("Attachment already downloaded " + msgId + " "+ (System.currentTimeMillis() - lastSaved) + " ms !");
							lastSaved = System.currentTimeMillis();
						} else {
                        	MessagePartBody attachPart = null;
                            try {
								System.out.println("Downloading attachment: " + msgId + "." + filename);
								String attId = part.getBody().getAttachmentId();
								attachPart = service.users().messages().attachments()
										.get(userId, messageId, attId).execute();
								byte[] fileByteArray = Base64.getDecoder().decode(attachPart.getData());
								exception.createNewFile();
								FileOutputStream fileOutFile = new FileOutputStream(exception);
								fileOutFile.write(fileByteArray);
								fileOutFile.close();
								System.out.println("Attachment saved " + (System.currentTimeMillis() - lastSaved) +"ms !");
								lastSaved = System.currentTimeMillis();
								exception.setLastModified(message.getInternalDate());
							} catch (Exception e) {
								if (attachPart != null) {
									System.out.println("---:" + attachPart.getData() + ":---");
								}
								e.printStackTrace();
							}
                        }
                    }
                }
            }
        }
        System.out.println("Processed emails " + count + " from " + users.size() + " users");
    }

    public static Map<String, List<ExceptionText>> analyzeExceptions(Variables vars) {
        Map<String, List<ExceptionText>> result = new HashMap<>();
        TreeSet<String> usrs  = new TreeSet<>();
        int count = 0;
        int exceptionCount = 0;
        if (vars.FOLDER_WITH_LOGS.exists()) {
            for(File currLog : vars.FOLDER_WITH_LOGS.listFiles()) {
            	if(!currLog.getName().endsWith(".exception.log")) {
            		continue;
            	}
            	count++;
                try{
                	File usr = new File(vars.FOLDER_WITH_LOGS, currLog.getName().substring(0, currLog.getName().length() - ".exception.log".length())+".uid.txt");
                	String user = "";
                	if(usr.exists()) {
                		BufferedReader br = new BufferedReader(new FileReader(usr));
                		user = br.readLine() ;
                		br.close();
                	}
                	usrs.add(user);
                	
                    FileInputStream fstream = new FileInputStream(currLog);
                    BufferedReader br = new BufferedReader(new InputStreamReader(fstream));
                    String strLine;

                    /* read log line by line */
                    while ((strLine = br.readLine()) != null) {
                        String currDate = "";
                        String currApkVersion = "";
                        String currBody = "";
                        String currName = "";
                        String currText = "";

                        if (strLine.matches("[0-9]+.[0-9]+.[0-9]+ [0-9]+:[0-9]+:[0-9]+")) {
                            currDate = strLine;
                            while (!strLine.contains("Version  OsmAnd") && (strLine = br.readLine()) != null) {
                                currText += strLine + "\n";
                            }
                            String[] lines = currText.split("\n");
                            for (String line : lines) {

                                if (line.contains("Apk Version")) {
                                    int indexOfColumn = line.indexOf(":");
                                    currApkVersion = line.substring(indexOfColumn + 1, line.length());
                                }
                                else if ((line.contains("Exception:") || line.contains("Error:")) && currName.equals("")) {
                                    int columnIndex = line.indexOf(":");
                                    currName = line.substring(0, columnIndex);
                                }
                                else {
                                    currBody += line + "\n";
                                }
                            }
                        if (!currName.equals("") && !currBody.equals("") && !currApkVersion.equals("")) {
                            ExceptionText exception = new ExceptionText(currDate, currName, currBody, currApkVersion, user);
                            if(vars != null && !currApkVersion.contains(vars.VERSION)) {
                                continue;
                            }
                            exceptionCount ++;
                            String hsh = exception.getExceptionHash();
                            if (!result.containsKey(hsh)) {
                                List<ExceptionText> exceptions = new ArrayList<>();
                                result.put(hsh, exceptions);
                            }
                            result.get(hsh).add(exception);
                        }
                        }
                    }
                    br.close();
                    fstream.close();
                } catch (Exception e) {
                    System.err.println("Error: " + e.getMessage());
                }
            }
        }
        System.out.println("Analyzed " + count + " emails from " + usrs.size() + 
        		" users, parsed " + exceptionCount +" ("+  result.size() + " different) exceptions ");
        return result;
    }

    public static void writeResultToFile(Map<String, List<ExceptionText>> mapToAnalyze, Variables vars) {
		File output = new File(vars.HOME_FILE, "exceptions-" + vars.VERSION + ".csv");
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
        SimpleDateFormat eDateParser = new SimpleDateFormat("dd.MM.yyyy HH:mm:ss");
        pw.write("Name,Version,LastDate,Count,Users,Hash,Stacktrace\n");

        for (String key : st) {
            StringBuilder sb = new StringBuilder();
            List<ExceptionText> exceptions = map.get(key);
            ExceptionText exception = exceptions.get(0);
            String maxDate = exception.getDate();
            long t = 0;
            try {
				t = eDateParser.parse(maxDate).getTime();
			} catch (ParseException e) {
				e.printStackTrace();
			}
            Set<String> users = new TreeSet<>();
            for(ExceptionText et : exceptions) {
            	users.add(et.getUser());
            	try {
					if(eDateParser.parse(et.getDate()).getTime() > t) {
						maxDate = et.getDate();
						t = eDateParser.parse(et.getDate()).getTime();
					}
				} catch (ParseException e) {
					e.printStackTrace();
				}
            }
            
            //for (ExceptionText exception: exceptions) {
                sb.append(exception.getName());
                sb.append(',');
                sb.append(exception.getApkVersion());
                sb.append(',');
                sb.append(maxDate);
                sb.append(',');
                sb.append(exceptions.size());
                sb.append(',');
                sb.append(users.size());
                sb.append(',');

                sb.append(exception.getExceptionHash().replaceAll("\n", "").replaceAll("\t", " ").replaceAll(",", " "));
                sb.append(',');
                String body = exception.getStackTrace().replaceAll("\n", "").replaceAll("\t", " ").replaceAll(",", " ");

                sb.append(body);
                sb.append('\n');
            //}

            pw.write(sb.toString());
        }


        pw.close();

        System.out.println("done!");
    }

}