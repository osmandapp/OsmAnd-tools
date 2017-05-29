package net.osmand.exceptionanalyzer.data;


/**
 * Created by user on 28.05.17.
 */
public class ExceptionText {
    private static final int PACKAGE_LINES = 3;
	private static final int TOP_LINES_HASH = 3;
	private String date;
    private String name;
    private String body;
    private String apkVersion;
	private String user;

    public ExceptionText(String date, String name, String body, String apkVersion, String user) {
        this.date = date;
		this.user = user;
        this.name = simplify(name);
        this.body = body;
        this.apkVersion = apkVersion;
    }
    
    public String getUser() {
		return user;
	}
    
    public String getExceptionHash() {
    	if(name.contains("java.lang.OutOfMemoryError")) {
    		return name;
    	}
    	String[] lines = body.split("\n");
    	int begin = 0;
    	for(int i = 0; i < lines.length - 1; i++) {
    		if(lines[i].contains("Caused by:")) {
    			name = simplify(lines[i]);
    			begin = i;
    			if(name.contains("java.lang.OutOfMemoryError")) {
    	    		return name;
    	    	}
    		}
    	}
    	
    	String hsh = simplify(name);
    	int top = PACKAGE_LINES;
    	int i = begin;
    	while(top > 0 && i < lines.length) {
    		if(lines[i].contains("net.osmand")) {
    			hsh += "\n" + simplify(lines[i]);
    			top --;
    		}
    		i++;
    	}
    	
    	top = TOP_LINES_HASH;
    	i = begin;
    	while(top > 0 && i < lines.length) {
    			hsh += "\n" + simplify(lines[i]);
    		top --;
    		i++;
    	}
    	return hsh;
    }

    private String simplify(String string) {
    	if(string.contains("java.lang.OutOfMemoryError")) {
    		return "java.lang.OutOfMemoryError";
    	}
    	if(string.contains("java.lang.IllegalArgumentException: Receiver not registered: net.osmand.aidl.OsmandAidlApi")) {
    		return "java.lang.IllegalArgumentException: Receiver not registered: net.osmand.aidl.OsmandAidlApi";
    	}
		return string;
	}

	public String getName() {
        return String.valueOf(name);
    }

    public String getApkVersion() {
		return apkVersion;
	}
    
    public String getDate() {
		return date;
	}

    public String getStackTrace() {
        return String.valueOf(body);
    }

    @Override
    public boolean equals(Object thatObject) {
        if (thatObject instanceof ExceptionText) {
            ExceptionText exceptionText = (ExceptionText) thatObject;
            if (name.equals(exceptionText.getName()) && body.equals(exceptionText.getStackTrace())) {
                return true;
            }
        }
        return false;
    }
}
