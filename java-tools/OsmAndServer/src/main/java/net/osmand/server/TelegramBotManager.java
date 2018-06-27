package net.osmand.server;

import java.util.ArrayList;
import java.util.List;

import net.osmand.server.monitor.OsmAndServerMonitorTasks;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.telegram.telegrambots.ApiContextInitializer;
import org.telegram.telegrambots.TelegramBotsApi;
import org.telegram.telegrambots.api.methods.send.SendMessage;
import org.telegram.telegrambots.api.objects.Message;
import org.telegram.telegrambots.api.objects.Update;
import org.telegram.telegrambots.bots.TelegramLongPollingBot;
import org.telegram.telegrambots.exceptions.TelegramApiException;
import org.telegram.telegrambots.exceptions.TelegramApiRequestException;

@Service
public class TelegramBotManager {
	
	private static final Log LOG = LogFactory.getLog(TelegramBotManager.class);
	
	private List<Long> monitoringChatIds = new ArrayList<>();

	private OsmAndServerMonitoringBot osmAndServerMonitoringBot;
	
	@Autowired
	private OsmAndServerMonitorTasks monitoring;
	
	public void init() {
		if(System.getenv("OSMAND_SERVER_BOT_TOKEN") == null) {
			return;
		}
		ApiContextInitializer.init();
		TelegramBotsApi telegramBotsApi = new TelegramBotsApi();
        try {
        	osmAndServerMonitoringBot = new OsmAndServerMonitoringBot();
            telegramBotsApi.registerBot(osmAndServerMonitoringBot);
            LOG.info("Telegram Initialized");
        } catch(TelegramApiException e) {
        	LOG.error(e.getMessage(), e);
        	e.printStackTrace();
        }
	}
	
	
	public void sendMonitoringAlertMessage(String text) {
		if(osmAndServerMonitoringBot == null) {
			return;
		}
		for(Long id : monitoringChatIds) {
			SendMessage snd = new SendMessage();
			snd.setChatId(id);
			snd.setText(text);
			try {
				osmAndServerMonitoringBot.sendText(snd);
			} catch (TelegramApiException e) {
				e.printStackTrace();
				LOG.error("Error sending update to " + id, e);
			}
		}
	}
	
	private class OsmAndServerMonitoringBot extends TelegramLongPollingBot {


		@Override
		public String getBotUsername() {
			return "osmand_server_bot";
		}
		
		public void sendText(SendMessage snd) throws TelegramApiException {
			sendApiMethod(snd);
		}

		@Override
		public String getBotToken() {
			return System.getenv("OSMAND_SERVER_BOT_TOKEN");
		}

		@Override
		public void onUpdateReceived(Update update) {
			if (!update.hasMessage() || !update.getMessage().hasText()) {
				return;
			}
			Message msg = update.getMessage();
			SendMessage snd = new SendMessage();
			snd.setChatId(msg.getChatId());
			if (msg.isCommand() && "/start_monitoring".equals(msg.getText())) {
				monitoringChatIds.add(msg.getChatId());
				snd.setText("Monitoring of OsmAnd server has started");
			} else if (msg.isCommand() && "/stop_monitoring".equals(msg.getText())) {
				monitoringChatIds.remove((Long)msg.getChatId());
				snd.setText("Monitoring of OsmAnd server has stopped");
			} else if (msg.isCommand() && "/status".equals(msg.getText())) {
				snd.setText(monitoring.getStatusMessage());
			} else {
				snd.setText("Sorry, I don't know what to do");
			}
			try {
				sendApiMethod(snd);
			} catch (TelegramApiException e) {
				LOG.error(
						"Could not send message on update " + msg.getChatId() + " " + 
								msg.getText() + ": "+ e.getMessage(), e);
			}
		}

		@Override
		public void clearWebhook() throws TelegramApiRequestException {
		}
	}
	
}
