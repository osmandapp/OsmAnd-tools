package net.osmand.server;

import java.util.Collection;

import net.osmand.server.model.MonitoringChatId;
import net.osmand.server.monitor.OsmAndServerMonitorTasks;
import net.osmand.server.repo.MonitoringChatRepository;

import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;
import org.telegram.telegrambots.ApiContextInitializer;
import org.telegram.telegrambots.TelegramBotsApi;
import org.telegram.telegrambots.api.methods.send.SendMessage;
import org.telegram.telegrambots.api.objects.Message;
import org.telegram.telegrambots.api.objects.Update;
import org.telegram.telegrambots.api.objects.User;
import org.telegram.telegrambots.bots.TelegramLongPollingBot;
import org.telegram.telegrambots.exceptions.TelegramApiException;
import org.telegram.telegrambots.exceptions.TelegramApiRequestException;

@Service
public class TelegramBotManager {
	
	private static final Log LOG = LogFactory.getLog(TelegramBotManager.class);
	
	private OsmAndServerMonitoringBot osmAndServerMonitoringBot;
	
	@Autowired
	private OsmAndServerMonitorTasks monitoring;
	
	@Autowired
	private MonitoringChatRepository chatRepo;
	
	public Collection<MonitoringChatId> getMonitoringChatIds() {
		return chatRepo.findAll();
	}
	
	public void addMonitoringChatId(MonitoringChatId cid) {
		chatRepo.save(cid);
	}
	
	public void removeMonitoringChatId(Long chatId) {
		chatRepo.deleteById(chatId);
	}
	
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
		for(MonitoringChatId id : getMonitoringChatIds()) {
			SendMessage snd = new SendMessage();
			snd.setChatId(id.id);
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
			try {
				SendMessage snd = new SendMessage();
				snd.setChatId(msg.getChatId());
				String coreMsg = msg.getText();
				if (coreMsg.startsWith("/")) {
					coreMsg = coreMsg.substring(1);
				}
				int at = coreMsg.indexOf("@");
				if (at > 0) {
					coreMsg = coreMsg.substring(0, at);
				}
				if (msg.isCommand() && "start_monitoring".equals(coreMsg)) {
					MonitoringChatId mid = new MonitoringChatId();
					mid.id = msg.getChatId();
					User from = msg.getFrom();
					if(from != null) {
						mid.firstName = from.getFirstName();
						mid.lastName = from.getLastName();
						mid.userId = from.getId() == null ? null : new Long(from.getId());
					}
					addMonitoringChatId(mid);
					snd.setText("Monitoring of OsmAnd server has started");
				} else if (msg.isCommand() && "stop_monitoring".equals(coreMsg)) {
					removeMonitoringChatId(msg.getChatId());
					snd.setText("Monitoring of OsmAnd server has stopped");
				} else if (msg.isCommand() && "refresh".equals(coreMsg)) {
					snd.setText(monitoring.refreshAll());
				} else if (msg.isCommand() && "refresh-all".equals(coreMsg)) {
					String refreshAll = monitoring.refreshAll();
					for (MonitoringChatId l : getMonitoringChatIds()) {
						snd = new SendMessage();
						snd.setChatId(l.id);
						snd.setText(refreshAll);
						osmAndServerMonitoringBot.sendText(snd);
					}
					return;
				} else if (msg.isCommand() && "status".equals(coreMsg)) {
					snd.setText(monitoring.getStatusMessage());
				} else {
					snd.setText("Sorry, I don't know what to do");
				}

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
