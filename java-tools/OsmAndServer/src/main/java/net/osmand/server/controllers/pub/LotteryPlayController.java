package net.osmand.server.controllers.pub;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.net.URL;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.List;

import javax.servlet.http.HttpServletRequest;

import net.osmand.server.api.repo.LotteryRoundsRepository;
import net.osmand.server.api.repo.LotteryRoundsRepository.LotteryRound;
import net.osmand.server.api.repo.LotteryUsersRepository;
import net.osmand.server.api.repo.LotteryUsersRepository.LotteryUser;
import net.osmand.server.api.repo.MapUserRepository;
import net.osmand.util.Algorithms;

import org.apache.commons.codec.digest.DigestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.google.gson.Gson;


@RestController
@RequestMapping("/giveaway/")
public class LotteryPlayController {
	
	private static final String WON = "WON";

	Gson gson = new Gson();

	@Autowired
	LotteryRoundsRepository roundsRepo;
	
	@Autowired
	LotteryUsersRepository usersRepo;
	
	@Autowired
	MapUserRepository mapUsers;
	
	public static class LotteryResult {
		String month;
		long date;
		String message;
		String user;
		int participants;
		int activeParticipants;
		int winners;
		List<LotteryUsersRepository.LotteryUser> users = new ArrayList<LotteryUsersRepository.LotteryUser>();
		List<LotteryRoundsRepository.LotteryRound> rounds = new ArrayList<LotteryRoundsRepository.LotteryRound>();
	}
	
	@GetMapping(path = {"/list"}, produces = "application/json")
	@ResponseBody
	public String index(HttpServletRequest request, @RequestParam(required = false) String email, 
			String month) throws IOException {
		LotteryResult res = new LotteryResult();
		res.month = month;
		res.date = System.currentTimeMillis() / 1000;
		res.message = "";
		if(Algorithms.isEmpty(month)) {
			month = String.format("%1$tY-%1$tm", new Date());
			if(!Algorithms.isEmpty(email)) {
				LotteryUser user = participate(request, email, month);
				if(user != null) {
					res.user = user.hashcode;
					res.message = user.message;
				}
			}
		}
		
		
		for (LotteryUsersRepository.LotteryUser u : usersRepo.findByMonthOrderByUpdateTime(month)) {
			LotteryUsersRepository.LotteryUser c = new LotteryUser();
			c.hashcode = u.hashcode;
			c.roundId = u.roundId;
			res.participants ++;
			if (Algorithms.isEmpty(u.promocode)) {
				c.status = "Participating";
				res.activeParticipants++;
			} else {
				c.status = String.format("Winner (Round %d)", u.roundId);
				res.winners++;
			}
			res.users.add(c);
		}
		for (LotteryRoundsRepository.LotteryRound rnd : roundsRepo.findByMonthOrderByUpdateTimeDesc(month)) {
			rnd.message = "Round " + rnd.roundId + " - " + 
						String.format("%1$tm/%1$td %1$tH:%1$tM", rnd.updateTime);
			rnd.seedInteger = new BigInteger(rnd.seed, 16).toString();
			res.rounds.add(rnd);
		}
		
		return gson.toJson(res);
	}
	
	public LotteryUser participate(HttpServletRequest request, String email, String month)
			throws IOException {
		String remoteAddr = request.getRemoteAddr();
    	Enumeration<String> hs = request.getHeaders("X-Forwarded-For");
        if (hs != null && hs.hasMoreElements()) {
            remoteAddr = hs.nextElement();
        }
		LotteryUser usr = new LotteryUsersRepository.LotteryUser();
		if(Algorithms.isEmpty(email) || !mapUsers.existsByEmail(email)) {
			usr.message = String.format("User with email '%s' is not subscribed", email);
			return usr;
		}
		usr.email = email;
		usr.month = month;
		usr.hashcode = DigestUtils.sha1Hex(email.getBytes());
		if(usr.hashcode.length() > 10) {
			usr.hashcode = usr.hashcode.substring(0, 10);
		}
		usr.ip = remoteAddr;
		usr.promocode = null;
		usr.updateTime = new Date();
		List<LotteryUser> col = usersRepo.findByMonthAndHashcodeOrderByUpdateTime(month, usr.hashcode);
		if(col.size() > 0) {
			usr.message = String.format("You are already subscribed as '%s'", usr.hashcode);
		}  else {
			usr.message = String.format("You are subscribed as '%s'", usr.hashcode);
			usersRepo.save(usr);
		}
		return usr;
	}
	
	@PostMapping(path = {"/run"}, produces = "application/json")
    @ResponseBody
	public String runLottery(HttpServletRequest request, @RequestParam(required = true) String latestHash)
			throws IOException {
		
		URL url = new URL("https://blockchain.info/q/latesthash");
		InputStream is = url.openStream();
		String hash = Algorithms.readFromInputStream(is).toString();
		LotteryRound rnd = new LotteryRoundsRepository.LotteryRound();
		if(!hash.equals(latestHash)) {
			rnd.message = String.format("Submitted hash is not equal '%s' to latest '%s'", 
					latestHash, hash);
			return gson.toJson(rnd);
		}
		if(hash.equals("")) {
			rnd.message = String.format("Hash is not available yet", hash);
			return gson.toJson(rnd);
		}
		String month = String.format("%1$tY-%1$tm", new Date()); 
		List<LotteryRound> rounds = roundsRepo.findByMonthOrderByUpdateTimeDesc(month);
		LotteryRound last = null;
		if (rounds.size() > 0) {
			last = rounds.get(0);
			if(rounds.get(0).seed.equals(hash) ) {
				last.message = String.format("Lottery was not played cause the latest round has same seed '%s'", 
						hash);
				return gson.toJson(last);
			}
		}
		BigInteger bi = new BigInteger(hash, 16);
		List<LotteryUser> users = usersRepo.findByMonthOrderByUpdateTime(month);
		List<String> participants = new ArrayList<String>();
		for(LotteryUser u : users) {
			if(Algorithms.isEmpty(u.promocode)) {
				participants.add(u.hashcode);
			}
		}
		rnd.seed = hash;
		rnd.size = participants.size();
		rnd.updateTime = new Date();
		rnd.month = month;
		rnd.participants = String.join(",", participants);
		rnd.roundId = last == null ? 1 : last.roundId + 1;
		 
		if(rnd.size == 0) {
			rnd.message = String.format("Empty list of participants");
			return gson.toJson(rnd);
		}
		rnd.selection = bi.remainder(BigInteger.valueOf(rnd.size)).intValue();
		rnd.winner = participants.get(rnd.selection);
		
		List<LotteryUser> lstUsers = usersRepo.findByMonthAndHashcodeOrderByUpdateTime(month, rnd.winner);
		for(LotteryUser usr : lstUsers) {
			usr.promocode = WON;
			usr.roundId = rnd.roundId;
			usersRepo.save(usr);
		}
		roundsRepo.save(rnd);
		
		return gson.toJson(rnd);
    }
    

}
