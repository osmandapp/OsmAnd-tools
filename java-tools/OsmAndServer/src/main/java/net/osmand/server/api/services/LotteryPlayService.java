package net.osmand.server.api.services;

import java.io.IOException;
import java.io.InputStream;
import java.math.BigInteger;
import java.net.URL;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.Enumeration;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeMap;

import javax.servlet.http.HttpServletRequest;

import org.apache.commons.codec.digest.DigestUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import net.osmand.server.api.repo.LotteryRoundsRepository;
import net.osmand.server.api.repo.LotteryRoundsRepository.LotteryRound;
import net.osmand.server.api.repo.LotterySeriesRepository;
import net.osmand.server.api.repo.LotterySeriesRepository.LotterySeries;
import net.osmand.server.api.repo.LotterySeriesRepository.LotteryStatus;
import net.osmand.server.api.repo.LotteryUsersRepository;
import net.osmand.server.api.repo.LotteryUsersRepository.LotteryUser;
import net.osmand.server.api.repo.MapUserRepository;
import net.osmand.server.api.repo.MapUserRepository.MapUser;
import net.osmand.util.Algorithms;

@Service
public class LotteryPlayService {

	@Autowired
	LotteryRoundsRepository roundsRepo;

	@Autowired
	LotterySeriesRepository seriesRepo;

	@Autowired
	LotteryUsersRepository usersRepo;

	@Autowired
	MapUserRepository mapUsers;

	@Autowired
	EmailSenderService emailSender;

	static SimpleDateFormat FORMAT = new SimpleDateFormat("MM/dd HH:mm");
	static {
		FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
	}

	public static class LotteryResult {
		public String series;
		String type;
		LotteryStatus status;
		String date;
		int totalRounds;

		public String message;
		public String user;

		int participants;
		int activeParticipants;
		int winners;

		List<LotteryUsersRepository.LotteryUser> users = new ArrayList<LotteryUsersRepository.LotteryUser>();
		List<LotteryRoundsRepository.LotteryRound> rounds = new ArrayList<LotteryRoundsRepository.LotteryRound>();
		List<LotterySeries> seriesList;

	}

	public LotteryResult series() {
		LotteryResult res = new LotteryResult();
		res.seriesList = new ArrayList<LotterySeriesRepository.LotterySeries>();
		for (LotterySeriesRepository.LotterySeries s : seriesRepo.findAllByOrderByUpdateTimeDesc()) {
			if (s.status != LotteryStatus.NOTPUBLIC && s.status != LotteryStatus.DRAFT) {
				s.promocodes = "";
				s.usedPromos = "";
				res.seriesList.add(s);
			}
		}
		return res;
	}

	public LotteryUser participate(String remoteAddr, String email, String series) throws IOException {
		LotteryUser usr = new LotteryUsersRepository.LotteryUser();
		if (Algorithms.isEmpty(email) || !mapUsers.existsByEmailIgnoreCase(email)) {
			usr.message = String.format("User with email '%s' is not subscribed", email);
			return usr;
		}
		Optional<LotterySeries> seriesObject = seriesRepo.findById(series);
		if (!seriesObject.isPresent()) {
			usr.message = String.format("Giveaway '%s' is not found", series);
			return usr;
		}
		if (!seriesObject.get().isOpenForRegistration()) {
			usr.message = String.format("Giveaway is not active anymore", series);
			return usr;
		}
		usr.email = email;
		usr.series = series;
		usr.hashcode = DigestUtils.sha1Hex(email.getBytes());
		if (usr.hashcode.length() > 10) {
			usr.hashcode = usr.hashcode.substring(0, 10);
		}
		usr.ip = remoteAddr;
		usr.promocode = null;
		usr.updateTime = new Date();
		List<LotteryUser> col = usersRepo.findBySeriesAndHashcodeOrderByUpdateTime(series, usr.hashcode);
		if (col.size() > 0) {
			usr.message = String.format("You are already subscribed as '%s'", usr.hashcode);
		} else {
			usr.message = String.format("You are subscribed as '%s'", usr.hashcode);
			usersRepo.save(usr);
		}
		return usr;
	}

	public void fillSeriesDetails(LotteryResult res) {
		String series = res.series;
		Optional<LotterySeries> s = seriesRepo.findById(series);
		if (s.isPresent()) {
			res.totalRounds = s.get().rounds;
			res.status = s.get().status;
			res.type = s.get().type;
			res.date = FORMAT.format(s.get().updateTime);
		}

		for (LotteryUsersRepository.LotteryUser u : usersRepo.findBySeriesOrderByUpdateTime(series)) {
			LotteryUsersRepository.LotteryUser c = new LotteryUser();
			c.hashcode = u.hashcode;
			c.roundId = u.roundId;
			c.date = FORMAT.format(u.updateTime);
			res.participants++;
			if (Algorithms.isEmpty(u.promocode)) {
				c.status = "Participating";
				res.activeParticipants++;
			} else {
				c.status = String.format("Winner (Round %d)", u.roundId);
				res.winners++;
			}
			res.users.add(c);
		}
		for (LotteryRoundsRepository.LotteryRound rnd : roundsRepo.findBySeriesOrderByUpdateTimeDesc(series)) {
			rnd.message = "Round " + rnd.roundId + " - " + FORMAT.format(rnd.updateTime) + " UTC";
			rnd.seedInteger = new BigInteger(rnd.seed, 16).toString();
			res.rounds.add(rnd);
		}

	}

	public Object runLottery(String latestHash) throws IOException {
		URL url = new URL("https://blockchain.info/q/latesthash");
		InputStream is = url.openStream();
		String hash = Algorithms.readFromInputStream(is).toString();
		if (!hash.equals(latestHash)) {
			return String.format("Submitted hash is not equal '%s' to latest '%s'", latestHash, hash);
		}
		if (hash.equals("")) {
			return String.format("Hash is not available yet", hash);
		}
		List<LotteryRound> playedRounds = new ArrayList<LotteryRound>();
		List<LotterySeries> listSeries = seriesRepo.findAll();
		for (LotterySeries series : listSeries) {
			LotteryRound rnd = new LotteryRoundsRepository.LotteryRound();
			rnd.series = series.name;
			if (series.status != LotteryStatus.RUNNING) {
				continue;
			}
			List<LotteryRound> rounds = roundsRepo.findBySeriesOrderByUpdateTimeDesc(series.name);
			if (series.rounds <= rounds.size()) {
				continue;
			}
			LotteryRound last = null;
			if (rounds.size() > 0) {
				last = rounds.get(0);
				if (rounds.get(0).seed.equals(hash)) {
					last.message = String.format("Lottery was not played cause the latest round has same seed '%s'",
							hash);
					playedRounds.add(last);
					continue;
				}
			}

			Set<String> nonUsedPromos = series.getNonUsedPromos();
			String promo = null;
			if (nonUsedPromos.isEmpty()) {
				rnd.message = String.format("All promos are used");
				playedRounds.add(rnd);
				continue;
			} else {
				promo = nonUsedPromos.iterator().next();
			}
			BigInteger bi = new BigInteger(hash, 16);
			List<LotteryUser> users = usersRepo.findBySeriesOrderByUpdateTime(series.name);
			List<String> participants = new ArrayList<String>();
			for (LotteryUser u : users) {
				if (Algorithms.isEmpty(u.promocode)) {
					participants.add(u.hashcode);
				}
			}
			rnd.seed = hash;
			rnd.size = participants.size();
			rnd.updateTime = new Date();
			rnd.participants = String.join(",", participants);
			rnd.roundId = last == null ? 1 : last.roundId + 1;

			if (rnd.size == 0) {
				rnd.message = String.format("Empty list of participants");
				playedRounds.add(rnd);
				continue;
			}
			rnd.selection = bi.remainder(BigInteger.valueOf(rnd.size)).intValue();
			rnd.winner = participants.get(rnd.selection);

			List<LotteryUser> lstUsers = usersRepo.findBySeriesAndHashcodeOrderByUpdateTime(series.name, rnd.winner);
			for (LotteryUser usr : lstUsers) {
				if (Algorithms.isEmpty(usr.promocode)) {
					usr.promocode = promo;
				} else {
					usr.promocode += "," + promo;
				}
				usr.roundId = rnd.roundId;
				usr.sent = emailSender.sendPromocodesEmails(usr.email, series.emailTemplate, usr.promocode);
				usersRepo.save(usr);
			}
			series.usePromo(promo);
			seriesRepo.save(series);
			roundsRepo.save(rnd);
			playedRounds.add(rnd);
		}
		return playedRounds;
	}

	public Map<String, Object> subscribeToGiveaways(HttpServletRequest request, String aid, String email, String os) {
		MapUser mapUser = new MapUser();
		mapUser.aid = aid;
		String remoteAddr = request.getRemoteAddr();
		Enumeration<String> hs = request.getHeaders("X-Forwarded-For");
		if (hs != null && hs.hasMoreElements()) {
			remoteAddr = hs.nextElement();
		}
		if (Algorithms.isEmpty(mapUser.aid)) {
			mapUser.aid = remoteAddr;
		}
		mapUser.email = email;
		if (!validateEmail(mapUser.email)) {
			throw new IllegalStateException(String.format("Email '%s' is not valid.", mapUser.email));
		}
		mapUser.os = os;
		mapUser.updateTime = new Date();

		Map<String, Object> res = new TreeMap<>();
		res.put("email", mapUser.email);
		res.put("time", mapUser.updateTime.getTime());
		List<MapUser> ex = mapUsers.findByEmailIgnoreCase(email);
		for (MapUser u : ex) {
			if (Algorithms.stringsEqual(os, u.os)) {
				res.put("message", "You have already subscribed to giveaways with email '" + mapUser.email + "'.");
				return res;
			}
		}
		mapUser = mapUsers.save(mapUser);
		res.put("message", "You successfully subscribed to future giveaways with email '" + mapUser.email + "'.");
		return res;
	}

	private boolean validateEmail(String email) {
		if (Algorithms.isEmpty(email)) {
			return false;
		}
		if (!email.contains("@")) {
			return false;
		}
		return true;
	}
}
