package net.osmand.server.assist.convers;

import net.osmand.server.assist.OsmAndAssistantBot;
import net.osmand.server.assist.TrackerConfiguration;

import org.apache.commons.logging.LogFactory;
import org.telegram.telegrambots.api.objects.Message;
import org.telegram.telegrambots.exceptions.TelegramApiException;

public class SetTrackerConversation extends AssistantConversation {
	public static final int ASK_TRACKER_ID = 0;
	public static final int ASK_TRACKER_TOKEN = 1;
	
	private static final org.apache.commons.logging.Log LOG = LogFactory.getLog(SetTrackerConversation.class);

	int state = 0;
	TrackerConfiguration config = new TrackerConfiguration();

	public SetTrackerConversation(UserChatIdentifier chatIdentifier) {
		super(chatIdentifier);
	}

	@Override
	public String getConversationId() {
		return "settracker";
	}

	@Override
	public boolean updateMessage(OsmAndAssistantBot bot, Message msg, String reply) throws TelegramApiException {
		if (state == ASK_TRACKER_ID) {
			bot.sendTextMsg(getSendMessage("Please specify tracker site to monitor:"));
			config.lastName = chatIdentifier.getLastName();
			config.firstName = chatIdentifier.getFirstName();
			config.userId = chatIdentifier.getUserId();
			config.dateCreated = getUpdateTime();
			state++;
			return false;
		} else if (state == ASK_TRACKER_TOKEN) {
			if(validateEmptyInput(bot, reply)) {
				config.trackerId = reply;
				bot.sendTextMsg(getSendMessage("Please specify tracker token:"));
				state++;
			}
			return false;
		} else {
			if (validateEmptyInput(bot, reply)) {
				config.token = reply;
				String suffix = config.token.substring(config.token.length() - 3, config.token.length());
				config.trackerName = config.trackerId + "-" + suffix;
				LOG.info("Register new tracker config: " + config);
				String error = bot.saveTrackerConfiguration(config);
				if (error == null) {
					bot.sendTextMsg(getSendMessage("Tracker configuration '" + config.trackerName
							+ "' is successfully saved"));
				} else {
					bot.sendTextMsg(getSendMessage(error));
				}
				return true;
			}
			return false;
		}
	}
	
}
