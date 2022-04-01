package net.osmand.server;

import net.osmand.server.assist.OsmAndAssistantBot;
import net.osmand.server.monitor.OsmAndServerMonitoringBot;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.telegram.telegrambots.meta.TelegramBotsApi;
import org.telegram.telegrambots.meta.exceptions.TelegramApiException;
import org.telegram.telegrambots.updatesreceivers.DefaultBotSession;

@Service
public class TelegramBotManager {
	
	private static final Log LOG = LogFactory.getLog(TelegramBotManager.class);
	
	@Autowired
	private OsmAndServerMonitoringBot osmAndServerMonitoringBot;
	
	@Autowired
	private OsmAndAssistantBot osmAndAssistantBot;
	
	
	public void init() {
		try {
			TelegramBotsApi telegramBotsApi = new TelegramBotsApi(DefaultBotSession.class);
			if (osmAndServerMonitoringBot.isTokenPresent()) {
				telegramBotsApi.registerBot(osmAndServerMonitoringBot);
			}
			if (osmAndAssistantBot.isTokenPresent()) {
				telegramBotsApi.registerBot(osmAndAssistantBot);
			}
			LOG.info("Telegram Initialized");
		} catch (TelegramApiException e) {
			LOG.error(e.getMessage(), e);
			e.printStackTrace();
		}
	}
	
	
}
