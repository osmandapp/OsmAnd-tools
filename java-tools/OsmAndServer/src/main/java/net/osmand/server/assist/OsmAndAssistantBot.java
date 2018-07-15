package net.osmand.server.assist;

import java.io.Serializable;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.List;
import java.util.Objects;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.TimeZone;
import java.util.TreeSet;
import java.util.concurrent.TimeUnit;

import net.osmand.server.assist.data.Device;
import net.osmand.server.assist.data.DeviceMonitor;
import net.osmand.server.assist.data.DeviceRepository;
import net.osmand.server.assist.data.LocationInfo;
import net.osmand.server.assist.data.TrackerConfiguration;
import net.osmand.server.assist.data.TrackerConfigurationRepository;
import net.osmand.server.assist.data.UserChatIdentifier;
import net.osmand.server.assist.ext.ITrackerManager;
import net.osmand.server.assist.ext.ITrackerManager.DeviceInfo;
import net.osmand.util.MapUtils;

import org.apache.commons.collections4.map.PassiveExpiringMap;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Component;
import org.telegram.telegrambots.api.methods.BotApiMethod;
import org.telegram.telegrambots.api.methods.send.SendLocation;
import org.telegram.telegrambots.api.methods.send.SendMessage;
import org.telegram.telegrambots.api.methods.updatingmessages.DeleteMessage;
import org.telegram.telegrambots.api.methods.updatingmessages.EditMessageLiveLocation;
import org.telegram.telegrambots.api.methods.updatingmessages.EditMessageText;
import org.telegram.telegrambots.api.objects.CallbackQuery;
import org.telegram.telegrambots.api.objects.Location;
import org.telegram.telegrambots.api.objects.Message;
import org.telegram.telegrambots.api.objects.Update;
import org.telegram.telegrambots.api.objects.replykeyboard.InlineKeyboardMarkup;
import org.telegram.telegrambots.api.objects.replykeyboard.ReplyKeyboard;
import org.telegram.telegrambots.api.objects.replykeyboard.buttons.InlineKeyboardButton;
import org.telegram.telegrambots.bots.TelegramLongPollingBot;
import org.telegram.telegrambots.exceptions.TelegramApiException;
import org.telegram.telegrambots.updateshandlers.SentCallback;

import com.google.gson.JsonParser;

@Component
public class OsmAndAssistantBot extends TelegramLongPollingBot {

	public static final String URL_TO_POST_COORDINATES = "http://builder.osmand.net:8090/device/%s/send";
	private static final int LIMIT_CONFIGURATIONS = 3;
	private static final int USER_UNIQUENESS = 1 << 20;
	private static final int LIMIT_DEVICES_PER_USER = 200;
	private static final Log LOG = LogFactory.getLog(OsmAndAssistantBot.class);
	private static final Integer DEFAULT_UPD_PERIOD = 86400;
	
	SimpleDateFormat UTC_DATE_FORMAT = new SimpleDateFormat("yyyy-MM-dd");
	{
		UTC_DATE_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
	}
	SimpleDateFormat UTC_TIME_FORMAT = new SimpleDateFormat("HH:mm:ss");
	{
		UTC_TIME_FORMAT.setTimeZone(TimeZone.getTimeZone("UTC"));
	}
	Random rnd = new Random();

	PassiveExpiringMap<UserChatIdentifier, AssistantConversation> conversations =
			new PassiveExpiringMap<UserChatIdentifier, AssistantConversation>(5, TimeUnit.MINUTES);
	
	@Autowired
	List<ITrackerManager> trackerManagers;
	
	@Autowired
	TrackerConfigurationRepository trackerRepo;
	
	@Autowired
	DeviceLocationManager deviceLocManager;
	
	@Autowired
	DeviceRepository deviceRepo;


	
	
	@Override
	public String getBotUsername() {
		if(System.getenv("OSMAND_DEV_TEST_BOT_TOKEN") != null) {
			return System.getenv("OSMAND_DEV_TEST_BOT"); 
		}
		return "osmand_bot";
	}

	public boolean isTokenPresent() {
		return getBotToken() != null;
	}

	@Override
	public String getBotToken() {
		if(System.getenv("OSMAND_DEV_TEST_BOT_TOKEN") != null) {
			return System.getenv("OSMAND_DEV_TEST_BOT_TOKEN");
		}
		return System.getenv("OSMAND_ASSISTANT_BOT_TOKEN");
	}

	@Override
	public void onUpdateReceived(Update update) {
		Message msg = null;
		UserChatIdentifier ucid = new UserChatIdentifier();
		try {
			CallbackQuery callbackQuery = update.getCallbackQuery();
			if (callbackQuery != null) {
				msg = callbackQuery.getMessage();
				ucid.setChatId(msg.getChatId());
				ucid.setUser(callbackQuery.getFrom());
				String data = callbackQuery.getData();
				processCallback(ucid, data, callbackQuery);
			} else if (update.hasMessage() && update.getMessage().hasText()) {
				msg = update.getMessage();
				ucid = new UserChatIdentifier();
				ucid.setChatId(msg.getChatId());
				ucid.setUser(msg.getFrom());
				SendMessage snd = new SendMessage();
				snd.setChatId(msg.getChatId());
				String coreMsg = msg.getText();
				if (coreMsg.startsWith("/")) {
					coreMsg = coreMsg.substring(1);
				}
				String params = "";
				int space = coreMsg.indexOf(' ');
				if (space != -1) {
					params = coreMsg.substring(space).trim();
					coreMsg = coreMsg.substring(0, space);
					
				}
				int at = coreMsg.indexOf("@");
				if (at > 0) {
					coreMsg = coreMsg.substring(0, at);
				}
				
				if (msg.isCommand()) {
					if ("add_device".equals(coreMsg) || "add_ext_device".equals(coreMsg)) {
						AddDeviceConversation c = new AddDeviceConversation(ucid, "add_ext_device".equals(coreMsg));
						setNewConversation(c);
						c.updateMessage(this, msg);
					} else if ("mydevices".equals(coreMsg)) {
						retrieveDevices(ucid, params);
					} else if ("configs".equals(coreMsg)) {
						retrieveConfigs(ucid, params);
					} else if ("whoami".equals(coreMsg) && "start".equals(coreMsg)) {
						sendApiMethod(new SendMessage(msg.getChatId(), "I'm your OsmAnd assistant. To list your devices use /mydevices."));
					} else {
						sendApiMethod(new SendMessage(msg.getChatId(), "Sorry, the command is not recognized"));
					}
				} else {
					AssistantConversation conversation = conversations.get(ucid);
					if (conversation != null) {
						boolean finished = conversation.updateMessage(this, msg);
						if (finished) {
							conversations.remove(ucid);
						}
					}
				}
			}
		} catch (Exception e) {
			if (!(e instanceof TelegramApiException)) {
				try {
					sendMethod(new SendMessage(ucid.getChatId(), "Internal error: " + e.getMessage()));
				} catch (TelegramApiException e1) {
				}
			}
			LOG.warn(e.getMessage(), e);
		}
	}


	protected boolean processCallback(UserChatIdentifier ucid, String data, CallbackQuery callbackQuery) throws TelegramApiException {
		Message msg = callbackQuery.getMessage();
		if(msg == null) {
			return false;
		}
		UserChatIdentifier ci = new UserChatIdentifier();
		ci.setChatId(msg.getChatId());
		ci.setUser(callbackQuery.getFrom());
		if(data.equals("hide")) {
			DeleteMessage delmsg = new DeleteMessage(msg.getChatId(), msg.getMessageId());
			sendApiMethod(delmsg);
			return true;
		}
		String[] sl = data.split("\\|");
		if(data.startsWith("dv|")) {
			long deviceId = Device.getDecodedId(sl[1]);
			String pm = sl.length > 2 ? sl[2] : ""; 
			Optional<Device> device = deviceRepo.findById(deviceId);
			if(!device.isPresent()) {
				SendMessage snd = new SendMessage();
				snd.setChatId(msg.getChatId());
				snd.setText("Device is not found.");
				sendApiMethod(snd);
			} else {
				Device d = device.get();
				if(callbackQuery.getFrom() == null || d.userId != callbackQuery.getFrom().getId()){
					SendMessage snd = new SendMessage(msg.getChatId(), "Only device owner can request information about device.");
					sendApiMethod(snd);	
				} else {
					retrieveDeviceInformation(d, msg, pm);
				}
			}
		} else if(data.startsWith("cfg|")) {
			long deviceId = Long.parseLong(sl[1]);
			String pm = sl.length > 2 ? sl[2] : ""; 
			Optional<TrackerConfiguration> cfg = trackerRepo.findById(deviceId);
			if(!cfg.isPresent()) {
				SendMessage snd = new SendMessage();
				snd.setChatId(msg.getChatId());
				snd.setText("Configuration is not found.");
				sendApiMethod(snd);
			} else {
				TrackerConfiguration d = cfg.get();
				if(callbackQuery.getFrom() == null || d.userId != callbackQuery.getFrom().getId()){
					SendMessage snd = new SendMessage(msg.getChatId(), "Only device owner can request information about device.");
					sendApiMethod(snd);	
				} else {
					retrieveConfigInformation(d, msg, pm);
				}
			}
		} else if(data.startsWith("impc|") ) {
			TrackerConfiguration c = null;
			for(TrackerConfiguration  tc : getExternalConfigurations(ci)) {
				if(tc.trackerName.equals(sl[1])) {
					c = tc;
					break;
				}
			}
			if(c == null) {
				SendMessage snd = new SendMessage(msg.getChatId(), "Tracker configuration '" + sl[1]
						+ "' is not accessible.");
				sendApiMethod(snd);
			} else {
				importFromConfigurationMessage(ci, c);
			}
		} else if(data.startsWith("impd|") ) {
			Long l = Long.parseLong(sl[1]);
			String extId = sl[2];
			Optional<TrackerConfiguration> opt = trackerRepo.findById(l);
			String msgSend = "Can't find configuration or device";
			DeviceInfo info = null;
			if(opt.isPresent()) {
				TrackerConfiguration dt = opt.get();
				dt.mgr = getTrackerManagerById(dt.trackerId);
				if(dt.mgr != null) {
					List<? extends DeviceInfo> di = dt.mgr.getDevicesList(dt);
					for(DeviceInfo i : di) {
						if(extId.equals(i.getId())) {
							info = i;
							break;
						}
					}
				}
			}
			
			if(info != null) {
				if(!checkExternalDevicePresent(ci, opt.get(), info.getId())) {
					Device device = createDevice(ci, info.getName());
					device.externalConfiguration = opt.get();
					device.externalId = info.getId();
					msgSend = saveDevice(device);
				} else {
					msgSend = String.format("Device '%s' is already imported", info.getName());
				}
			}
			SendMessage msgs = new SendMessage(msg.getChatId(), msgSend);
			sendMethod(msgs);
		} else {
			AssistantConversation conversation = conversations.get(ucid);
			if (conversation != null) {
				boolean finished = conversation.updateMessage(this, msg, data);
				if (finished) {
					conversations.remove(ucid);
				}
			}
			return true;
		}
		return false;
	}

	public void retrieveDeviceInformation(Device d, 
			Message replyMsg, String pm) throws TelegramApiException {

		InlineKeyboardMarkup markup = new InlineKeyboardMarkup();
		ArrayList<InlineKeyboardButton> lt = new ArrayList<InlineKeyboardButton>();
		ArrayList<InlineKeyboardButton> lt2 = new ArrayList<InlineKeyboardButton>();
		markup.getKeyboard().add(lt);
		markup.getKeyboard().add(lt2);
		DeviceMonitor mon = deviceLocManager.getDeviceMonitor(d);
		LocationInfo sig = mon.getLastLocationSignal();
		if(pm.equals("more") && !replyMsg.getChat().isUserChat()) {
			sendApiMethod(new SendMessage(replyMsg.getChatId(), "Detailed information could not be displayed in a non-private chat."));
			return;
		} else if(pm.equals("delconfirm")) {
			EditMessageText editMsg = new EditMessageText();
			editMsg.setChatId(replyMsg.getChatId());
			editMsg.setMessageId(replyMsg.getMessageId());
			editMsg.enableHtml(true).setReplyMarkup(markup).setText("Device was deleted.");
			deviceRepo.delete(d);
			sendApiMethod(editMsg);
			return;
		} else if(pm.equals("mon")) {
			deviceLocManager.startMonitoringLocation(d, replyMsg.getChatId());
		} else if(pm.equals("stmon")) {
			int msgId = deviceLocManager.stopMonitoringLocation(d, replyMsg.getChatId());
			if(msgId == replyMsg.getMessageId()) {
				return;
			}
		} else if (pm.equals("loc")) {
			lt.add(new InlineKeyboardButton("Hide").setCallbackData("hide"));
			lt.add(new InlineKeyboardButton("Update").setCallbackData("dv|" + d.getEncodedId() + "|loc"));
			if(replyMsg.hasLocation()) {
				Location loc = replyMsg.getLocation();
				if (sig != null && sig.isLocationPresent() && 
						MapUtils.getDistance(loc.getLatitude(), loc.getLongitude(), sig.getLat(), sig.getLon()) > 5) {
					EditMessageLiveLocation sl = new EditMessageLiveLocation();
					sl.setMessageId(replyMsg.getMessageId());
					sl.setChatId(replyMsg.getChatId());
					sl.setLatitude((float) sig.getLat());
					sl.setLongitud((float) sig.getLon());
					sl.setReplyMarkup(markup);
					sendApiMethod(sl);
				}
				return;
			}
			if (sig != null && sig.isLocationPresent()) {
				SendLocation sl = new SendLocation((float) sig.getLat(), (float) sig.getLon());
				sl.setChatId(replyMsg.getChatId());
				sl.setLivePeriod(DEFAULT_UPD_PERIOD);
				sl.setReplyMarkup(markup);
				sendApiMethod(sl);
				return;
			}
			lt.clear();
		}
		String locMsg = "n/a";
		if (sig != null && sig.isLocationPresent()) {
			locMsg = String.format("%.3f, %.3f (%s)", sig.getLat(), sig.getLon(),
					formatTime(sig.getTimestamp()));
		}
		String txt = String.format("<b>Device</b>: %s\n<b>Location</b>: %s\n", d.deviceName, locMsg);
		boolean locationMonitored = mon.isLocationMonitored(replyMsg.getChatId());
		if(d.externalConfiguration != null) {
			txt += String.format("<b>External cfg</b>: %s\n", d.externalConfiguration.trackerName);
		}
		if(locationMonitored) {
			txt += String.format("Location is continuously updated");
		}
		if(pm.equals("del")) {
			txt += "<b>Are you sure, you want to delete it? </b>";
		} else if(pm.equals("more")) {
			txt = "<b>ID</b>:" + d.getEncodedId() + "\n" + txt; 
			txt += String.format("\n<b>URL to post location</b>: %s\n<b>Registered</b>: %s\n", 
					String.format(URL_TO_POST_COORDINATES, d.getEncodedId()), UTC_DATE_FORMAT.format(d.createdDate));
		} else {
			
			txt += String.format("\nTime: %s UTC\n", UTC_TIME_FORMAT.format(new Date()));
		}
		
		
		if(pm.equals("del")) {
			InlineKeyboardButton upd = new InlineKeyboardButton("No");
			upd.setCallbackData("dv|" + d.getEncodedId() + "|less");
			lt.add(upd);

			InlineKeyboardButton more = new InlineKeyboardButton("Yes");
			more.setCallbackData("dv|" + d.getEncodedId() + "|delconfirm");
			lt.add(more);			
		} else {
			if (pm.equals("more")) {
				InlineKeyboardButton ls = new InlineKeyboardButton("Less");
				ls.setCallbackData("dv|" + d.getEncodedId() + "|less");
				lt.add(ls);
				InlineKeyboardButton del = new InlineKeyboardButton("Delete");
				del.setCallbackData("dv|" + d.getEncodedId() + "|del");
				lt.add(del);
			} else {
				InlineKeyboardButton hide = new InlineKeyboardButton("Hide");
				hide.setCallbackData("hide");
				lt.add(hide);
				InlineKeyboardButton upd = new InlineKeyboardButton("Update");
				upd.setCallbackData("dv|" + d.getEncodedId() + "|upd");
				lt.add(upd);
				InlineKeyboardButton more = new InlineKeyboardButton("More");
				more.setCallbackData("dv|" + d.getEncodedId() + "|more");
				lt.add(more);
				if (sig != null && sig.isLocationPresent()) {
					InlineKeyboardButton locK = new InlineKeyboardButton("Show location");
					locK.setCallbackData("dv|" + d.getEncodedId() + "|loc");
					lt2.add(locK);
				}
				InlineKeyboardButton monL = new InlineKeyboardButton(locationMonitored ? "Stop monitoring"
						: "Start monitoring");
				monL.setCallbackData("dv|" + d.getEncodedId() + (locationMonitored ? "|stmon" : "|mon"));
				lt2.add(monL);
			}
		}
		
		if (pm.equals("")) {
			SendMessage sendMessage = new SendMessage();
			sendMessage.setChatId(replyMsg.getChatId());
			sendMessage.enableHtml(true).setReplyMarkup(markup).setText(txt);
			sendApiMethod(sendMessage);
		} else {
			EditMessageText editMsg = new EditMessageText();
			editMsg.setChatId(replyMsg.getChatId());
			editMsg.setMessageId(replyMsg.getMessageId());
			editMsg.setReplyMarkup(markup);
			editMsg.enableHtml(true).setText(txt);
			sendApiMethod(editMsg);
		}
	}

	private ReplyKeyboard getKeyboardWithHide() {
		InlineKeyboardMarkup mk = new InlineKeyboardMarkup();
		mk.getKeyboard().add(Collections.singletonList(new InlineKeyboardButton("Hide").setCallbackData("hide")));
		return mk;
	}

	private void setNewConversation(AssistantConversation c) throws TelegramApiException {
		AssistantConversation conversation = conversations.get(c.getChatIdentifier());
		if (conversation != null) {
			sendApiMethod(new SendMessage(c.getChatIdentifier().getChatId(), "FYI: Your conversation about "
					+ conversation.getConversationId() + " was interrupted"));
		}
		conversations.put(c.getChatIdentifier(), c);
	}

	

	public final <T extends Serializable, Method extends BotApiMethod<T>> T sendMethod(Method method) throws TelegramApiException {
		return super.sendApiMethod(method);
    }
	
	public final <T extends Serializable, Method extends BotApiMethod<T>, Callback extends SentCallback<T>> void sendMethodAsync(Method method, Callback callback) {
		super.sendApiMethodAsync(method, callback);
    }

	public String saveTrackerConfiguration(TrackerConfiguration config) {
		List<TrackerConfiguration> list = trackerRepo.findByUserIdOrderByCreatedDate(config.userId);
		String suffix = "" ;
		if(config.token.length() >= 3) {
			suffix = config.token.substring(config.token.length() - 3, config.token.length());
		}
		if(config.trackerName == null) {
			config.trackerName = config.trackerId + "-" + suffix;
		}
		if(list.size() >= LIMIT_CONFIGURATIONS) {
			return "Currently 1 user is allowed to have only " + LIMIT_CONFIGURATIONS + " configurations. Manage your configurations with /configs .";
		}
		for (TrackerConfiguration dc : list) {
			if (dc.trackerName.equals(config.trackerName)) {
				return "Tracker with name '" + config.trackerName + "' is already registered";
			}
		}
		trackerRepo.save(config);
		return null;
	}

	
	public TrackerConfiguration removeTrackerConfiguration(UserChatIdentifier ucid, int order) {
		List<TrackerConfiguration> list = trackerRepo.findByUserIdOrderByCreatedDate(ucid.getUserId());
		TrackerConfiguration config = list.get(order - 1);
		trackerRepo.delete(config);
		return config;
	}

	public static class MyDevicesOptions {
		public long filterLocationTime;
		public UserChatIdentifier ucid;
		public TrackerConfiguration config;
		public int cfgOrder;
	}
	
	
	public void retrieveDevices(UserChatIdentifier ucid, String params) throws TelegramApiException {
		int i = 1;
		parseRecentParam(params);
		List<Device> devs = deviceRepo.findByUserIdOrderByCreatedDate(ucid.getUserId());
		InlineKeyboardMarkup markup = new InlineKeyboardMarkup();
		SendMessage msg = new SendMessage();
		msg.setChatId(ucid.getChatId());
		msg.enableHtml(true);
		msg.setReplyMarkup(markup);
		if (devs.size() > 0) {
			msg.setText("<b>Available devices:</b>");
			for (Device c : devs) {
				StringBuilder sb = new StringBuilder();
				sb.append(i).append(". ").append(c.deviceName);
				ArrayList<InlineKeyboardButton> lt = new ArrayList<InlineKeyboardButton>();
				InlineKeyboardButton button = new InlineKeyboardButton(sb.toString());
				button.setCallbackData("dv|" + c.getEncodedId());
				markup.getKeyboard().add(Collections.singletonList(button));
				i++;
			}
			markup.getKeyboard().add(Collections.singletonList(new InlineKeyboardButton("Hide").setCallbackData("hide")));
		} else {
			msg.setText("You don't have any added devices. Please use /add_device to register new devices.");
		}
		
		sendApiMethod(msg);
	}

	private long parseRecentParam(String params) {
		long filterLocationTime = 0;
		if (params.startsWith("recent")) {
			long dl = TimeUnit.HOURS.toMillis(24);
			if (params.startsWith("recent ")) {
				try {
					double hours = Double.parseDouble(params.substring("recent ".length()));
					dl = TimeUnit.MINUTES.toMillis((long) (60 * hours));
				} catch (NumberFormatException e) {
				}
			}
			filterLocationTime = (System.currentTimeMillis() - dl) / 1000l;
		}
		return filterLocationTime;
	}
	
	public void retrieveConfigs(UserChatIdentifier ucid, String params) throws TelegramApiException {
		int i = 1;
		parseRecentParam(params);
		List<TrackerConfiguration> cfgs = trackerRepo.findByUserIdOrderByCreatedDate(ucid.getUserId());
		InlineKeyboardMarkup markup = new InlineKeyboardMarkup();
		SendMessage msg = new SendMessage();
		msg.setChatId(ucid.getChatId());
		msg.enableHtml(true);
		msg.setReplyMarkup(markup);
		if (cfgs.size() > 0) {
			msg.setText("<b>Available configurations:</b>");
			for (TrackerConfiguration c : cfgs) {
				StringBuilder sb = new StringBuilder();
				sb.append(i).append(". ").append(c.trackerName);
				InlineKeyboardButton button = new InlineKeyboardButton(sb.toString()).setCallbackData("cfg|" + c.id);
				markup.getKeyboard().add(Collections.singletonList(button));
				i++;
			}
			InlineKeyboardButton button = new InlineKeyboardButton("Hide").setCallbackData("hide");
			markup.getKeyboard().add(Collections.singletonList(button));
		} else {
			msg.setText("You don't have any external configurations. Please use /add_ext_device to register 3rd party devices.");
		}
		
		sendApiMethod(msg);
	}
	
	public void retrieveConfigInformation(TrackerConfiguration d, 
			Message replyMsg, String pm) throws TelegramApiException {

		InlineKeyboardMarkup markup = new InlineKeyboardMarkup();
		ArrayList<InlineKeyboardButton> lt = new ArrayList<InlineKeyboardButton>();
		markup.getKeyboard().add(lt);
		if(pm.equals("delconfirm")) {
			deleteConfiguration(d);
			EditMessageText editMsg = new EditMessageText();
			editMsg.setChatId(replyMsg.getChatId());
			editMsg.setMessageId(replyMsg.getMessageId());
			editMsg.enableHtml(true).setReplyMarkup(markup).setText("Configuration was deleted.");
			
			sendApiMethod(editMsg);
			return;
		}
		String txt = String.format("Configuration: <b>%s</b>\nCreated: %s\n", 
				d.trackerName, UTC_DATE_FORMAT.format(d.createdDate));
		if(pm.equals("del")) {
			txt += "<b>Are you sure, you want to delete configuration and ALL devices configured with it? </b>";
		}
		
		if(pm.equals("del")) {
			InlineKeyboardButton upd = new InlineKeyboardButton("No");
			upd.setCallbackData("cfg|" + d.id + "|");
			lt.add(upd);
			InlineKeyboardButton yes = new InlineKeyboardButton("Yes");
			yes.setCallbackData("cfg|" + d.id + "|delconfirm");
			lt.add(yes);			
		} else if(!pm.equals("keep")){
			InlineKeyboardButton del = new InlineKeyboardButton("Delete");
			del.setCallbackData("cfg|" + d.id + "|del");
			lt.add(del);
			InlineKeyboardButton kp = new InlineKeyboardButton("Keep");
			kp.setCallbackData("cfg|" + d.id + "|keep");
			lt.add(kp);
		}
		
		if (pm.equals("")) {
			SendMessage sendMessage = new SendMessage();
			sendMessage.setChatId(replyMsg.getChatId());
			sendMessage.enableHtml(true).setReplyMarkup(markup).setText(txt);
			sendApiMethod(sendMessage);
		} else {
			EditMessageText editMsg = new EditMessageText();
			editMsg.setChatId(replyMsg.getChatId());
			editMsg.setMessageId(replyMsg.getMessageId());
			editMsg.enableHtml(true).setReplyMarkup(markup).setText(txt);
			sendApiMethod(editMsg);
		}
	}

	private void deleteConfiguration(TrackerConfiguration d) {
		deviceRepo.deleteAllByExternalConfiguration(d);
		trackerRepo.delete(d);
	}
	


	public Device createDevice(UserChatIdentifier chatIdentifier, String name) {
		String js = chatIdentifier.getUserJsonString();
		Device device = new Device();
		device.data.add(Device.USER_INFO, new JsonParser().parse(js));
		device.chatId = chatIdentifier.getChatId();
		device.userId = chatIdentifier.getUserId();
		device.deviceName = name;
		device.id =  rnd.nextInt(USER_UNIQUENESS) + USER_UNIQUENESS;
		return device;
	}
	
	public String saveDevice(Device device) {
		if (deviceRepo.findByUserIdOrderByCreatedDate(device.userId).size() > LIMIT_DEVICES_PER_USER) {
			return String.format("Currently 1 user is allowed to have maximum '%d' devices.", LIMIT_DEVICES_PER_USER);
		}
		deviceRepo.save(device);
		return String.format("Device '%s' is successfully added. Check it with /mydevices", device.deviceName);
	}
	
	public List<TrackerConfiguration> getExternalConfigurations(UserChatIdentifier chatIdentifier) {
		List<TrackerConfiguration> list = trackerRepo.findByUserIdOrderByCreatedDate(chatIdentifier.getUserId());
		for(TrackerConfiguration d : list) {
			d.mgr = getTrackerManagerById(d.trackerId); 
		}
		return list;
	}
	
	public ITrackerManager getTrackerManagerById(String id) {
		for(ITrackerManager c : trackerManagers) {
			if(c.getTrackerId().equals(id)) {
				return c;
			}
		}
		return null;
	}
	
	
	public boolean checkExternalDevicePresent(UserChatIdentifier chatIdentifier, TrackerConfiguration ext, String id) {
		List<Device> devices = deviceRepo.findByUserIdOrderByCreatedDate(chatIdentifier.getUserId());
		for(Device dv : devices) {
			if(dv.externalConfiguration != null && dv.externalConfiguration.id == ext.id ) {
				if(Objects.equals(dv.externalId, id)) {
					return true;
				}
				
			}
		}
		return false;
		
	}
	
	public String formatTime(long ti) {
		Date dt = new Date(ti);
		long current = System.currentTimeMillis() / 1000;
		long tm = ti / 1000;
		if(current - tm < 10) {
			return "few seconds ago";
		} else if (current - tm < 50) {
			return (current - tm) + " seconds ago";
		} else if (current - tm < 60 * 60 * 2) {
			return (current - tm) / 60 + " minutes ago";
		} else if (current - tm < 60 * 60 * 24) {
			return (current - tm) / (60 * 60) + " hours ago";
		}
		return UTC_DATE_FORMAT.format(dt) + " " + UTC_TIME_FORMAT.format(dt) + " UTC";
	}

	public void importFromConfigurationMessage(UserChatIdentifier chatIdentifier, TrackerConfiguration ext) throws TelegramApiException {
		List<? extends DeviceInfo> list = ext.mgr.getDevicesList(ext);
		List<DeviceInfo> lst = new ArrayList<DeviceInfo>(list.size());
		List<Device> existingDevices = deviceRepo.findByUserIdOrderByCreatedDate(chatIdentifier.getUserId());
		Set<String> exSet = new TreeSet<>();
		for(Device d : existingDevices) {
			if(d.externalConfiguration != null && d.externalConfiguration.id == ext.id) {
				exSet.add(d.externalId);
			}
		}
		for(DeviceInfo i : list) {
			if(!exSet.contains(i.getId())) {
				lst.add(i);
			}
		}
		InlineKeyboardMarkup markup = new InlineKeyboardMarkup();
		SendMessage smsg = new SendMessage(chatIdentifier.getChatId(), "Select devices to import: ");
		smsg.setReplyMarkup(markup);
		int i = 1;
		for (DeviceInfo c : lst) {
			StringBuilder sb = new StringBuilder();
			sb.append(i).append(". ").append(c.getName());
			ArrayList<InlineKeyboardButton> lt = new ArrayList<InlineKeyboardButton>();
			InlineKeyboardButton button = new InlineKeyboardButton(sb.toString());
			button.setCallbackData("impd|" + ext.id + "|" + c.getId());
			lt.add(button);
			markup.getKeyboard().add(lt);
			i++;
		}
		sendMethod(smsg);		
	}

	
	
}