package net.osmand.server.controllers.pub;

import java.io.IOException;
import java.util.Enumeration;

import jakarta.servlet.http.HttpServletRequest;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.ResponseBody;
import org.springframework.web.bind.annotation.RestController;

import com.google.gson.Gson;

import net.osmand.server.api.repo.LotteryUsersRepository.LotteryUser;
import net.osmand.server.api.services.LotteryPlayService;
import net.osmand.server.api.services.LotteryPlayService.LotteryResult;
import net.osmand.util.Algorithms;



// TODO: THIS CONTROLLER IS DEPRECATED AND SHOULD BE DELETED ONCE NEW V2 WEB IS DEPLOYED
@RestController
@RequestMapping("/giveaway/")
public class LotteryPlayController {
	
	@Autowired
	LotteryPlayService service;

	Gson gson = new Gson();

	@GetMapping(path = {"/series"}, produces = "application/json")
	@ResponseBody
	public String series(HttpServletRequest request) throws IOException {
		return gson.toJson(service.series());
	}
	
	
	@GetMapping(path = {"/list"}, produces = "application/json")
	@ResponseBody
	public String index(HttpServletRequest request, @RequestParam(required = false) String email, 
			@RequestParam(required = true) String series) throws IOException {
		LotteryResult res = new LotteryResult();
		res.series = series;
		res.message = "";
		if (!Algorithms.isEmpty(email)) {
			String remoteAddr = request.getRemoteAddr();
	    	Enumeration<String> hs = request.getHeaders("X-Forwarded-For");
	        if (hs != null && hs.hasMoreElements()) {
	            remoteAddr = hs.nextElement();
	        }
			LotteryUser user = service.participate(remoteAddr, email, series);
			if (user != null) {
				res.user = user.hashcode;
				res.message = user.message;
			}
		}
		service.fillSeriesDetails(res);
		return gson.toJson(res);
	}
	
	
	@PostMapping(path = {"/run"}, produces = "application/json")
    @ResponseBody
	public String runLottery(@RequestParam(required = true) String latestHash) throws IOException {
		return gson.toJson(service.runLottery(latestHash));
    }
    

}
