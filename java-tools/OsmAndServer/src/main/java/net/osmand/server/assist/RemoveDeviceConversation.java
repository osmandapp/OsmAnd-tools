package net.osmand.server.assist;


import net.osmand.server.assist.data.TrackerConfiguration;
import net.osmand.server.assist.data.UserChatIdentifier;

import org.apache.commons.logging.LogFactory;
import org.telegram.telegrambots.api.methods.send.SendMessage;
import org.telegram.telegrambots.api.objects.Message;
import org.telegram.telegrambots.exceptions.TelegramApiException;

public class RemoveDeviceConversation extends AssistantConversation {

	int state = 0;
	private static final org.apache.commons.logging.Log LOG = LogFactory.getLog(RemoveDeviceConversation.class);

	public RemoveDeviceConversation(UserChatIdentifier chatIdentifier) {
		super(chatIdentifier);
	}

	@Override
	public String getConversationId() {
		return "remove_device";
	}

	@Override
	public boolean updateMessage(OsmAndAssistantBot bot, Message msg, String reply) throws TelegramApiException {
		if (state == 0) {
			SendMessage smsg = getSendMessage("Please specify tracker configuration to delete (type a number).");
			bot.sendMethod(smsg);
			bot.sendAllTrackerConfigurations(chatIdentifier, true);
			state++;
			return false;
		} else if (state == 1) {
			try {
				int n = Integer.parseInt(reply);
				TrackerConfiguration config = bot.removeTrackerConfiguration(chatIdentifier, n);
				LOG.info("Remove tracker config: " + config);
				bot.sendAllTrackerConfigurations(chatIdentifier, false);
				state++;
				return true;
			} catch (Exception e) {
				SendMessage smsg = getSendMessage("Sorry, your input is not correct. Please try again:");
				bot.sendMethod(smsg);
			}
			return false;
		}
		return true;
	}


}
