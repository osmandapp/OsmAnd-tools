package net.osmand.server.api.services;

import java.io.File;
import java.io.FileReader;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;

import javax.sql.DataSource;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.jdbc.datasource.DataSourceUtils;
import org.springframework.stereotype.Service;

import com.google.gson.Gson;

import net.osmand.util.Algorithms;

@Service
public class PollsService {
    private static final Log LOGGER = LogFactory.getLog(PollsService.class);

    @Value("${osmand.web.location}")
    private String websiteLocation;
    
    Gson gson = new Gson();
    
    @Autowired
	private DataSource dataSource;
    
    private PollInfo polls;
    
    private static class PollInfo {
    	private List<PollQuestion> questions = new ArrayList<>();
    }
    
    public static class PollQuestion {
    	public String title = "";
    	public List<String> pub = new ArrayList<>();
    	public boolean active ;
    	public String pubdate = "";
    	public List<String> answers = new ArrayList<>();
    	public List<Integer> votes = new ArrayList<>();
    	public int orderDate;
		public String id;
    }
    
    public void reloadConfigs(List<String> errors) {
    	try { 
    		getPollsConfig(true);
    	} catch(Exception e) {
    		LOGGER.warn(e.getMessage(), e);
    		errors.add("Error while reloading polls configuration " + e.getMessage());
    	}
    }
    
	public PollQuestion getPollById(String pollId) {
		List<PollQuestion> pq = getPollsConfig(false);
		for (PollQuestion p : pq) {
			if (p.id.equals(pollId)) {
				return p;
			}
		}
		return null;
	}
    
	public void submitVote(String remoteAddr, PollQuestion q, int ans) {
		// insert db
		if (q != null && ans < q.votes.size()) {
			Connection conn = DataSourceUtils.getConnection(dataSource);
			try {
				PreparedStatement p = conn.prepareStatement(
						"insert into poll_results(ip, date, pollid, answer) values (?, ?, ?, ?)");
				p.setString(1, remoteAddr);
				p.setTimestamp(2, new Timestamp(System.currentTimeMillis()));
				p.setString(3, q.id);
				p.setInt(4, ans);
				p.executeUpdate();
			} catch (SQLException e) {
				throw new IllegalStateException(e);
			} finally {
				DataSourceUtils.releaseConnection(conn, dataSource);
			}
			q.votes.set(ans, q.votes.get(ans) + 1);
		}
	}
    
	public List<PollQuestion> getPollsConfig(boolean reload) {
		if (reload || polls == null) {
			try {
				File pollsConfig = new File(websiteLocation, "api/poll.json");
				polls = gson.fromJson(new FileReader(pollsConfig), PollInfo.class);
				for (int i = polls.questions.size() - 1; i > 0; i--) {
					PollQuestion prev = polls.questions.get(i);
					PollQuestion cur = polls.questions.get(i - 1);
					if (Algorithms.objectEquals(prev.pubdate, cur.pubdate)) {
						cur.orderDate = prev.orderDate + 1;
					} else {
						cur.orderDate = 0;
					}
				}
				Connection conn = DataSourceUtils.getConnection(dataSource);
				Map<String, Map<Integer, Integer>>  dbResults = new LinkedHashMap<String, Map<Integer,Integer>>(); 
				try {
					PreparedStatement p = conn.prepareStatement(
							"select pollid, answer, count(distinct ip) from poll_results group by pollid, answer");
					ResultSet rs = p.executeQuery();
					while(rs.next()) {
						String pid = rs.getString(1);
						Map<Integer, Integer> mp = dbResults.get(pid);
						if(mp == null) {
							mp = new TreeMap<Integer, Integer>();
							dbResults.put(pid, mp);
						}
						mp.put(rs.getInt(2), rs.getInt(3));
					}
				} catch (SQLException e) {
					throw new IllegalStateException(e);
				} finally {
					DataSourceUtils.releaseConnection(conn, dataSource);
				}
				for (int i = 0; i < polls.questions.size(); i++) {
					PollQuestion cur = polls.questions.get(i);
					cur.id = cur.pubdate + "_" + cur.orderDate;
					Map<Integer, Integer> dbAnswers = dbResults.get(cur.id);
					for (int j = 0; j < cur.answers.size(); j++) {
						if (dbAnswers != null && dbAnswers.containsKey(i)) {
							cur.votes.add(dbAnswers.get(i));
						} else {
							cur.votes.add(0);
						}
					}
				}
			} catch (Exception e) {
				LOGGER.warn(e.getMessage(), e);
			}
		} 
    	return polls == null ? Collections.emptyList() : polls.questions;
    }

	
}