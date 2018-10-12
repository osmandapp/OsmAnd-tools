package net.osmand.server.assist.data;

import net.osmand.server.assist.OsmAndAssistantBot;
import net.osmand.util.MapUtils;
import org.apache.commons.logging.Log;
import org.apache.commons.logging.LogFactory;
import org.hibernate.Session;
import org.telegram.telegrambots.api.methods.BotApiMethod;
import org.telegram.telegrambots.api.methods.send.SendLocation;
import org.telegram.telegrambots.api.methods.send.SendMessage;
import org.telegram.telegrambots.api.methods.updatingmessages.DeleteMessage;
import org.telegram.telegrambots.api.methods.updatingmessages.EditMessageLiveLocation;
import org.telegram.telegrambots.api.methods.updatingmessages.EditMessageText;
import org.telegram.telegrambots.api.objects.Message;
import org.telegram.telegrambots.api.objects.replykeyboard.InlineKeyboardMarkup;
import org.telegram.telegrambots.api.objects.replykeyboard.buttons.InlineKeyboardButton;
import org.telegram.telegrambots.exceptions.TelegramApiRequestException;
import org.telegram.telegrambots.updateshandlers.SentCallback;

import javax.persistence.*;
import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;

@Entity(name = "LocationChatMessage")
@Table(name = "telegram_chat_messages")
public class LocationChatMessage {
    protected static final int ERROR_THRESHOLD = 3;
    private static final Integer DEFAULT_UPD_PERIOD = 86400;
    private static final Log LOG = LogFactory.getLog(DeviceBean.class);

    @Id
    @GeneratedValue(strategy = GenerationType.IDENTITY)
    public long id;

    @ManyToOne
    @JoinColumn(name = "device_id", nullable = false)
    public DeviceBean deviceBean;

    @Column(name = "chat_type", nullable = false)
    public ChatType type;

    @Column(name = "chat_id", nullable = false)
    public long chatId;

    @Column(name = "inline_message_id")
    public String inlineMessageId;

    @Column(name = "initial_timestamp", nullable = false)
    public long initialTimestamp;

    @Column(name = "hidden")
    public boolean hidden;

    @Column(name = "message_id")
    public int messageId;

    @Column(name = "update_time")
    public long updateTime;

    @Column(name = "update_id")
    public int updateId = 1;

    @Column(name = "error_count")
    public int errorCount;

    LocationInfo lastSentLoc;

    @Transient
    Device device;

    public LocationChatMessage() { }

    public LocationChatMessage(Device d, ChatType chatType, long chatId) {
        device = d;
        this.deviceBean = d.deviceBean;
        this.type = chatType;
        this.chatId = chatId;
        this.inlineMessageId = null;
        this.initialTimestamp = System.currentTimeMillis();
    }

    public LocationChatMessage(Device d, ChatType chatType, String inlineMessageId) {
        device = d;
        this.deviceBean = d.deviceBean;
        this.type = chatType;
        this.chatId = 0;
        this.inlineMessageId = inlineMessageId;
        this.initialTimestamp = System.currentTimeMillis();
    }

    public void hide() {
        hidden = true;
    }


    public boolean isEnabled(long now) {
        return !hidden &&  (now - initialTimestamp) < DEFAULT_UPD_PERIOD * 1000 ;
    }

    public boolean deleteMessage(int msgId) {
        if(this.messageId == msgId) {
            messageId = 0;
            return true;
        }
        return false;
    }

    public int deleteOldMessage() {
        int oldMessageId = messageId;
        if(messageId != 0) {
            device.bot.sendMethodAsync(new DeleteMessage(chatId, messageId), new SentCallback<Boolean>() {
                @Override
                public void onResult(BotApiMethod<Boolean> method, Boolean response) {

                }

                @Override
                public void onException(BotApiMethod<Boolean> method, Exception exception) {
                    LOG.error(exception.getMessage(), exception);
                }

                @Override
                public void onError(BotApiMethod<Boolean> method, TelegramApiRequestException apiException) {
                    LOG.error(apiException.getMessage(), apiException);
                }
            });
            messageId = 0;
        }
        return oldMessageId;
    }


    public void sendMessage() {
        LocationInfo lastSignal= device.lastLocationSignal;
        updateTime = System.currentTimeMillis();
        if(!isEnabled(updateTime)) {
            messageId = 0;
        }
        if(type == ChatType.MAP_CHAT) {
            sendMapMessage(device.bot, lastSignal);
        } else if(type == ChatType.JSON_CHAT) {
            sendMsg(device.bot, device.getMessageJson(updateId).toString(), lastSignal);
        } else if(type == ChatType.MAP_CHAT) {
            sendMsg(device.bot, device.getMessageTxt(updateId), lastSignal);
        } else if(type == ChatType.MAP_INLINE) {
            sendInlineMap(device.bot, lastSignal);
        } else if(type == ChatType.MESSAGE_INLINE) {
            sendInline(device.bot, lastSignal);
        }
        updateId++;
    }

    private void sendInlineMap(OsmAndAssistantBot bot, LocationInfo locSig) {
        InlineKeyboardMarkup markup = new InlineKeyboardMarkup();
        markup.getKeyboard().add(Collections.singletonList(new InlineKeyboardButton("Update " + device.getDeviceName()).setCallbackData(
                "msg|" + device.getStringId() + "|updmap")));
        if (locSig != null && locSig.isLocationPresent()) {
            if (lastSentLoc == null
                    || MapUtils.getDistance(lastSentLoc.getLat(), lastSentLoc.getLon(), locSig.getLat(),
                            locSig.getLon()) > 5) {
                EditMessageLiveLocation editMessageText = new EditMessageLiveLocation();
                editMessageText.setInlineMessageId(inlineMessageId);
                editMessageText.setChatId("");
                editMessageText.setLatitude((float) locSig.getLat());
                editMessageText.setLongitud((float) locSig.getLon());
                editMessageText.setChatId((String)null);
                editMessageText.setReplyMarkup(markup);

                bot.sendMethodAsync(editMessageText, getCallback(locSig));
            }
        }
    }

    private void sendInline(OsmAndAssistantBot bot, LocationInfo lastLocationSignal) {
        InlineKeyboardMarkup markup = new InlineKeyboardMarkup();
        markup.getKeyboard().add(Collections.singletonList(new InlineKeyboardButton("Update " + device.getDeviceName()).setCallbackData(
                "msg|" + device.getStringId() + "|updtxt")));
        EditMessageText editMessageText = new EditMessageText();
        editMessageText.setText(device.getMessageTxt(updateId));
        editMessageText.enableHtml(true);
        editMessageText.setReplyMarkup(markup);
        editMessageText.setInlineMessageId(inlineMessageId);
        bot.sendMethodAsync(editMessageText, getCallback(lastLocationSignal));
    }

    private void sendMapMessage(OsmAndAssistantBot bot, LocationInfo locSig) {
        InlineKeyboardMarkup markup = new InlineKeyboardMarkup();
        ArrayList<InlineKeyboardButton> lt = new ArrayList<InlineKeyboardButton>();
        markup.getKeyboard().add(lt);
        lt.add(new InlineKeyboardButton("Hide").setCallbackData("dv|" + device.getStringId() + "|hide"));
        lt.add(new InlineKeyboardButton("Update " + device.getDeviceName()).setCallbackData("dv|" + device.getStringId() + "|loc"));
        if (locSig != null && locSig.isLocationPresent()) {
            if (messageId == 0) {
                SendLocation sl = new SendLocation((float) locSig.getLat(), (float) locSig.getLon());
                sl.setChatId(chatId);
                sl.setLivePeriod(DEFAULT_UPD_PERIOD);
                sl.setReplyMarkup(markup);
                bot.sendMethodAsync(sl, getCallback(locSig));
            } else {
                if (lastSentLoc != null
                        && MapUtils.getDistance(lastSentLoc.getLat(), lastSentLoc.getLon(), locSig.getLat(),
                                locSig.getLon()) > 5) {
                    EditMessageLiveLocation sl = new EditMessageLiveLocation();
                    sl.setMessageId(messageId);
                    sl.setChatId(chatId);
                    sl.setLatitude((float) locSig.getLat());
                    sl.setLongitud((float) locSig.getLon());
                    sl.setReplyMarkup(markup);
                    bot.sendMethodAsync(sl, getCallback(locSig));
                }
            }
        }
    }

    private void sendMsg(OsmAndAssistantBot bot, String text, LocationInfo lastLocSignal) {
        InlineKeyboardMarkup markup = new InlineKeyboardMarkup();
        markup.getKeyboard().add(Collections.singletonList(new InlineKeyboardButton("Hide").setCallbackData(
                "dv|" + device.getStringId() + "|hide")));
        if (messageId == 0) {
            bot.sendMethodAsync(new SendMessage(chatId, text).setReplyMarkup(markup).enableHtml(true),
                    getCallback(lastLocSignal));
        } else {
            EditMessageText mtd = new EditMessageText();
            mtd.setChatId(chatId);
            mtd.setMessageId(messageId);
            mtd.setText(text);
            mtd.enableHtml(true);
            mtd.setReplyMarkup(markup);
            bot.sendMethodAsync(mtd, getCallback(lastLocSignal));
        }
    }

    private <T extends Serializable> SentCallback<T> getCallback(LocationInfo locSig) {
        return new SentCallback<T>() {
            @Override
            public void onResult(BotApiMethod<T> method, T response) {
                DeviceBean db = device.deviceBean;
                Session session = device.em.unwrap(Session.class);
                lastSentLoc = locSig;
                if(response instanceof Message) {
                    System.out.println("PERSIST");
                    messageId = ((Message)response).getMessageId();
                    System.out.println(device.getChats());
                    session.saveOrUpdate(db);
                    //DeviceBean db = deviceRepository.saveAndFlush(Device.this.device);
                    //System.out.println(db.getChatMessages());
                } else {
                    System.out.println("PERSIST UPDATE");
                    session.saveOrUpdate(db);
                }
                session.close();
            }

            @Override
            public void onException(BotApiMethod<T> method, Exception exception) {
                LOG.error(exception.getMessage(), exception);
            }

            @Override
            public void onError(BotApiMethod<T> method,
                    TelegramApiRequestException apiException) {
                LOG.info(apiException.getMessage(), apiException);
                // message expired or deleted
                if(errorCount++ > ERROR_THRESHOLD) {
//						errorCount = 0;
                    hidden = true;
                }
            }
        };
    }
}
