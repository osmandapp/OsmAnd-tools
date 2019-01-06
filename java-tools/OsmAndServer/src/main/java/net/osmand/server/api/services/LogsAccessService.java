package net.osmand.server.api.services;

import java.io.File;
import java.io.IOException;
import java.io.OutputStream;
import java.io.RandomAccessFile;
import java.net.MalformedURLException;
import java.net.URL;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Set;
import java.util.TreeMap;
import java.util.TreeSet;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import nl.basjes.parse.core.Field;
import nl.basjes.parse.core.Parser;
import nl.basjes.parse.core.Parser.SetterPolicy;
import nl.basjes.parse.httpdlog.HttpdLoglineParser;
import nl.basjes.parse.httpdlog.dissectors.TimeStampDissector;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.annotations.Expose;

@Service

public class LogsAccessService {
    protected static final Log LOGGER = LogFactory.getLog(LogsAccessService.class);
    private static final String APACHE_LOG_FORMAT = "%h %l %u %t \"%r\" %>s %O \"%{Referer}i\" \"%{User-Agent}i\"";
    private static final String DEFAULT_LOG_LOCATION = "/var/log/nginx/";
    
    Gson gson = new Gson();
    
    @Autowired
	private IpLocationService locationService;
	
    
    public enum LogsPresentation {
    	PLAIN,
    	BEHAVIOR,
    	STATS
    }
    
	public void parseLogs(Date startTime, Date endTime, boolean parseRegion, int limit, String filter, LogsPresentation presentation, 
			OutputStream out) throws IOException {
		gson = new GsonBuilder().setPrettyPrinting().create();
		Parser<LogEntry> parser = new HttpdLoglineParser<>(LogEntry.class, APACHE_LOG_FORMAT);
		File logFile = new File(DEFAULT_LOG_LOCATION, "access.log");
		RandomAccessFile raf = new RandomAccessFile(logFile, "r");
		Pattern aidPattern = Pattern.compile("aid=([a-z,0-9]*)");
		Map<String, UserAccount> behaviorMap = new LinkedHashMap<String, UserAccount>();
		Map<String, Stat> stats = new LinkedHashMap<String, Stat>();
		try {
			String ln = null;
			LogEntry l = new LogEntry();
			long currentLimit = raf.length();
			int rows = 0;
			int err = 0;
			
			boolean found = true;
			if(startTime != null) {
				found = seekStartTime(parser, startTime, raf);
			}
			
			if(presentation == LogsPresentation.PLAIN) {
				out.write((LogEntry.toCSVHeader()+"\n").getBytes());
			}
			out.flush();
			while (found && (ln = raf.readLine()) != null) {
				if(raf.getFilePointer() > currentLimit) {
					break;
				}
				l.clear();
				try {
					parser.parse(l, ln);
				} catch (Exception e) {
					if (err++ >= 100) {
						out.write("Error parsing\n".getBytes());
						break;
					}
					continue;
				}
				if(l.date == null) {
					continue;
				}
				if(startTime != null && startTime.getTime() > l.date.getTime()) {
					continue;
				}
				if(endTime != null && endTime.getTime() < l.date.getTime()) {
					break;
				}
				Matcher aidMatcher = aidPattern.matcher(l.uri);
				String aid = aidMatcher.find() ? aidMatcher.group(1) : null ;
				if(filter != null && filter.length() > 0) {
					if(!l.uri.contains(filter) && !behaviorMap.containsKey(l.ip) && 
							!behaviorMap.containsKey(aid)) {
						continue;
					}
				}
				rows++;
				UserAccount accountAid = presentation == LogsPresentation.BEHAVIOR? retrieveUniqueAccount(aid, l, behaviorMap) : null;
				if(parseRegion) {
					l.region = locationService.getField(l.ip, IpLocationService.COUNTRY_NAME);
					if(accountAid != null) {
						accountAid.regions.add(l.region);
					}
				}
				if(presentation == LogsPresentation.BEHAVIOR) {
					accountAid.add(l);
				} else if(presentation == LogsPresentation.STATS) {
					String uri = l.uri;
					int i = uri.indexOf('?');
					if(i > 0) {
						uri = uri.substring(0, i);
					}
					if(!uri.startsWith("/api") && !uri.startsWith("/subscription")) {
						i = uri.indexOf('/', 1);
						if(i > 0) {
							uri = uri.substring(0, i);
						}
					}
					Stat stat = stats.get(uri);
					if(stat == null) {
						stat = new Stat();
						stats.put(uri, stat);
					}
					stat.add(aid, l);
				} else {
					out.write((l.toCSVString() + "\n").getBytes());
				}
				if(rows > limit && limit != -1) {
					break;
				}
				if(rows % 1000 == 0) {
					out.flush();
				}
			}
			if(presentation == LogsPresentation.BEHAVIOR) {
				out.write("{\"accounts\" : [".getBytes());
				Iterator<Entry<String, UserAccount>> i = behaviorMap.entrySet().iterator();
				boolean f = true;
				while (i.hasNext()) {
					Entry<String, UserAccount> nxt = i.next();
					UserAccount v = nxt.getValue();
					if (!nxt.getKey().equals(v.aid)) {
						continue;
					}
					if(!f) {
						out.write(",\n".getBytes());
					} else {
						f = false;
					}
					int duration = (int) ((v.maxdate - v.mindate) / (60 * 1000l));
					v.duration = String.format("%02d:%02d", duration / 60, duration % 60);
					out.write(gson.toJson(v).getBytes());
				}
				out.write("]}".getBytes());
			} else if(presentation == LogsPresentation.STATS) {
				out.write(gson.toJson(stats).getBytes());
			}
			out.close();
		} finally {
			raf.close();
		}
	}
	
	private UserAccount retrieveUniqueAccount(String aid, LogEntry l, Map<String, UserAccount> behaviorMap) {
		UserAccount accountAid = behaviorMap.get(aid);
		UserAccount accountIp = behaviorMap.get(l.ip);
		if(aid == null) {
			if(accountIp == null) {
				accountIp = new UserAccount(aid, l.ip, l.date);
				behaviorMap.put(l.ip, accountIp);
			}
			accountAid = accountIp;
		} else {
			if(accountAid == null) {
				accountAid = new UserAccount(aid, l.ip, l.date);
				behaviorMap.put(aid, accountAid);
			}
			if(accountIp == null) {
				accountIp = accountAid; 
				behaviorMap.put(l.ip, accountIp);
			} else if(accountIp != accountAid){
				accountIp = accountAid.merge(accountIp);
				behaviorMap.put(l.ip, accountIp);
			}
		}
		return accountAid;
	}
	
	private boolean seekStartTime(Parser<LogEntry> parser, Date startTime, RandomAccessFile raf)
			throws IOException {
		long currentLimit = raf.length();
		long pos = 0;
		boolean found = false;
		// seek position
		long step = 1 << 24; // 16 MB
		LogEntry lt = new LogEntry();

		while(!found) {
			if(currentLimit > pos + step) {
				raf.seek(pos + step);
				// skip incomplete line
				raf.readLine();
				try {
					parser.parse(lt, raf.readLine());
					if(startTime.getTime() < lt.date.getTime()) {
						raf.seek(pos);
						// skip incomplete line
						raf.readLine();
						found = true;
						break;
					}
				} catch (Exception e) {
				}
				pos += step;
			} else {
				break;
			}
		}
		return found;
	}
	
	
	protected static class UserAction {
		String ip;
		long time;
		String uri;
		String timeFormat;
		String region;
		Map<String, String> params = new TreeMap<String, String>();
		public UserAction(LogEntry l) {
			time = l.date.getTime();
			this.region = l.region;
			timeFormat = String.format("%1$tF %1$tR", l.date);
			ip = l.ip;
			uri = l.uri;
			int i = uri.indexOf('?');
			if(i > 0) {
				uri = uri.substring(0, i);
				String[] ls = l.uri.substring(i+1).split("&");
				for (String lt : ls) {
					String[] ks = lt.split("=");
					if (ks.length > 1) {
						params.put(ks[0], ks[1]);
					} else if (ks.length > 0) {
						params.put(ks[0], "");
					}
				}
			}
		}
		
	}
	

	protected static class UserAccount {
		String aid;
		Set<String> ips = new LinkedHashSet<String>();
		Set<String> regions = new LinkedHashSet<String>();
		long mindate;
		long maxdate;
		String duration = "";
		List<UserAction> actions = new ArrayList<UserAction>();
		
		public UserAccount(String aid, String ip, Date date) {
			this.aid = aid == null ? ip : aid;
			ips.add(ip);
			mindate = maxdate = date.getTime();
		}
		
		public UserAccount merge(UserAccount accountIp) {
			ips.addAll(accountIp.ips);
			regions.addAll(accountIp.regions);
			actions.addAll(accountIp.actions);
			this.mindate = Math.min(accountIp.mindate, this.mindate);
			this.maxdate = Math.max(accountIp.maxdate, this.maxdate);
			Collections.sort(actions, new Comparator<UserAction>() {

				@Override
				public int compare(UserAction o1, UserAction o2) {
					return Long.compare(o1.time, o2.time);
				}
				
			});
			return this;
		}

		public void add(LogEntry l) {
			this.mindate = Math.min(l.date.getTime(), this.mindate);
			this.maxdate = Math.max(l.date.getTime(), this.maxdate);
			actions.add(new UserAction(l));
		}
		
		
	}
	
	protected static class Stat {
		String uri;
		int count;
		int uniqueCount;
		
		@Expose(serialize=false)
		Set<String> aids = new TreeSet<String>();
		@Expose(serialize=false)
		Set<String> ips = new TreeSet<String>();
		
		public void add(String aid, LogEntry l) {
			if(aid != null) {
				aids.add(aid);
			}
			ips.add(l.ip);
			count++;
			uniqueCount = getUniqueCount();
		}
		
		public int getUniqueCount() {
		    return (aids.size() == 0 ? ips.size() : aids.size()) ;
		}
		
	}
	
	public static class LogEntry {
		public String ip;
		public Date date;
		public String uri;
		private String userAgent;
		private String status;
		private String referrer;
		private String region;
		private static final SimpleDateFormat format = new SimpleDateFormat(TimeStampDissector.DEFAULT_APACHE_DATE_TIME_PATTERN);
		
	    @Field("IP:connection.client.host")
	    public void setIP(final String value) {
	        ip =  value;
	    }
	    
	    @Field("TIME.STAMP:request.receive.time")
	    public void setTime(final String value) throws ParseException {
			date = format.parse(value);
	    }
	    
	    @Field("HTTP.URI:request.firstline.uri")
	    public void setURI(String value) {
	    	uri = value;
	    }
	    
	    @Field("HTTP.USERAGENT:request.user-agent")
	    public void setUserAgent(String value) {
	    	userAgent = value;
	    }
	    
	    @Field("STRING:request.status.last")
	    public void setStatusCode(String value) {
	    	status = value;
	    }
	    @Field(value = {"HTTP.URI:request.referer"}, setterPolicy = SetterPolicy.ALWAYS)
	    public void setReferrer(String value) {
	    	referrer = value == null  ? "" : value;
	    }
	    
	    public static String toCSVHeader() {
	    	// add nd, aid, np, ...
	    	return "IP,Region,Date,Time,Status,User-Agent,Referrer,Path,Version,Lang,Query,URL"; 
	    }
	    
	    public String toCSVString() {
	    	String path = "";
	    	String query = "";
	    	String version = "";
	    	String lang = "";
//	    	String nd = "";
//	    	String ns = "";
//	    	String aid = "";
	    	if(uri != null && uri.length() > 0) {
				try {
					URL l = new URL("http://127.0.0.1" + uri);
					if (l.getPath() != null) {
						path = l.getPath();
					}
					if (l.getQuery() != null) {
						query = l.getQuery();
						String[] params = query.split("&");
						for (String param : params) {
							int ind = param.indexOf('=');
							if(ind == -1) {
								continue;
							}
							String name = param.substring(0, ind);
							String value = param.substring(ind + 1);
							if ("version".equals(name)) {
								version = value;
							} else if ("lang".equals(name)) {
								lang = value;
							}
						}
					}
					uri = "";
					
				} catch (MalformedURLException e) {
				}
	    		
	    	}
	    	
			return String.format("%s,%s,%tF,%tT,%s,%s,%s,%s,%s,%s,%s,%s", 
					ip, region, date, date, status, userAgent == null ? "" : userAgent.replace(",", ";"), referrer, 
					path, version, lang, query, uri);
	    }
	    
	    public String toString() {
	        return String.format("Request %s %tF %2$tT %s (user-agent %s, referrer %s): %s", ip, date, status, userAgent, referrer, uri);
	    }

	    public void clear() {
	    	this.ip = "";
	    	this.date = null;
	    	this.referrer = "";
	    	this.region = "";
	    	this.userAgent = "";
	    	this.status = "";
	    	this.uri = "";
	    }
	}
}
