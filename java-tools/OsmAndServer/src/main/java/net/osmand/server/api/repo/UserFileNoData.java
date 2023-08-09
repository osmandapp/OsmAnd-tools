package net.osmand.server.api.repo;

import com.google.gson.JsonObject;

import java.util.Date;

public class UserFileNoData {
    public int userid;
    public long id;
    public int deviceid;
    public long filesize;
    public String type;
    public String name;
    public Date updatetime;
    public long updatetimems;
    public Date clienttime;
    public long clienttimems;
    public long zipSize;
    public String storage;
    public JsonObject details;
    
    public UserFileNoData(PremiumUserFilesRepository.UserFile c) {
        this.userid = c.userid;
        this.id = c.id;
        this.deviceid = c.deviceid;
        this.type = c.type;
        this.name = c.name;
        this.filesize = c.filesize ;
        this.zipSize = c.zipfilesize;
        this.updatetime = c.updatetime;
        this.updatetimems = updatetime == null ? 0 : updatetime.getTime();
        this.clienttime = c.clienttime;
        this.clienttimems = clienttime == null? 0 : clienttime.getTime();
        this.details = c.details;
        this.storage = c.storage;
    }
    
    public UserFileNoData(long id, int userid, int deviceid, String type, String name,
                          Date updatetime, Date clienttime, Long filesize, Long zipSize, String storage) {
        this(id, userid, deviceid, type, name, updatetime, clienttime, filesize, zipSize, storage, null);
    }
    
    public UserFileNoData(long id, int userid, int deviceid, String type, String name,
                          Date updatetime, Date clienttime, Long filesize, Long zipSize, String storage, JsonObject details) {
        this.userid = userid;
        this.id = id;
        this.deviceid = deviceid;
        this.type = type;
        this.name = name;
        this.filesize = filesize == null ? 0 : filesize.longValue();
        this.zipSize = zipSize == null ? 0 : zipSize.longValue();
        this.updatetime = updatetime;
        this.updatetimems = updatetime == null ? 0 : updatetime.getTime();
        this.clienttime = clienttime;
        this.clienttimems = clienttime == null? 0 : clienttime.getTime();
        this.details = details;
        this.storage = storage;
    }
}
