package net.osmand.server.api.services;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import jakarta.annotation.Nullable;
import jakarta.transaction.Transactional;
import net.osmand.server.WebSecurityConfiguration;
import net.osmand.server.api.repo.CloudUserDevicesRepository;
import net.osmand.server.api.repo.CloudUserFilesRepository;
import net.osmand.server.utils.WebGpxParser;
import net.osmand.server.utils.exception.OsmAndPublicApiException;
import net.osmand.shared.gpx.GpxFile;
import net.osmand.shared.gpx.GpxUtilities;
import okio.Buffer;
import okio.Source;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Service;

import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.Date;
import java.util.zip.GZIPInputStream;

import static net.osmand.router.RouteExporter.OSMAND_ROUTER_V2;

@Service
public class FavoriteService {
    
    public static final String FILE_TYPE_FAVOURITES = "FAVOURITES";
    public static final String FILE_EXT_GPX = ".gpx";
    private static final String DEFAULT_GROUP_FILE_NAME = "favorites.gpx";
    private static final String DEFAULT_GROUP_NAME = "favorites";
    public static final String ERROR_FILE_WAS_CHANGED_MSG = "File was changed!";
    public static final String ERROR_READING_GPX_MSG = "Error reading gpx!";
    
    @Autowired
    UserdataService userdataService;
    
    @Autowired
    protected GpxService gpxService;
    
    @Autowired
    WebGpxParser webGpxParser;
    
    Gson gson = new Gson();
    
    Gson gsonWithNans = new GsonBuilder().serializeSpecialFloatingPointValues().create();
    
    @Transactional
    public ResponseEntity<String> renameFavFolder(String oldName, String newName, StorageService.InternalZipFile fl, CloudUserDevicesRepository.CloudUserDevice dev) throws IOException {
        userdataService.validateUserForUpload(dev, FILE_TYPE_FAVOURITES, fl.getSize());
        userdataService.uploadFile(fl, dev, newName, FILE_TYPE_FAVOURITES, System.currentTimeMillis());
        userdataService.deleteFile(oldName, FILE_TYPE_FAVOURITES, null, null, dev);
        
        return userdataService.ok();
    }
    
    public void uploadFavoriteFile(File tmpFile, CloudUserDevicesRepository.CloudUserDevice dev, String name, Long updatetime) throws IOException {
        uploadFavoriteFile(tmpFile, dev, name, updatetime, null);
    }
    
    public void uploadFavoriteFile(File tmpFile, CloudUserDevicesRepository.CloudUserDevice dev, String name, Long updatetime, Date clienttime) throws IOException {
        StorageService.InternalZipFile fl = StorageService.InternalZipFile.buildFromFile(tmpFile);
        userdataService.validateUserForUpload(dev, FILE_TYPE_FAVOURITES, fl.getSize());
        userdataService.uploadFile(fl, dev, name, FILE_TYPE_FAVOURITES, clienttime != null ? clienttime.getTime() : System.currentTimeMillis());
        if (updatetime != null) {
            userdataService.deleteFileVersion(updatetime, dev.userid, name, FILE_TYPE_FAVOURITES, null);
        }
    }
    
    public ResponseEntity<String> updateFavoriteFile(String fileName, CloudUserDevicesRepository.CloudUserDevice dev,
                                                     Long updatetime, GpxFile file) throws IOException {
        File tmpGpx = gpxService.createTmpFileByGpxFile(file, fileName);
        uploadFavoriteFile(tmpGpx, dev, fileName, updatetime);
        UserdataService.ResponseFileStatus resp = createResponse(dev, fileName, file, tmpGpx);
        
        return ResponseEntity.ok(gson.toJson(resp));
    }
    
    public UserdataService.ResponseFileStatus createResponse(CloudUserDevicesRepository.CloudUserDevice dev,
                                                             String groupName, GpxFile file, File tmpFile) throws IOException {
        UserdataService.ResponseFileStatus resp = null;
        if (file != null && tmpFile != null) {
            CloudUserFilesRepository.UserFile userFile = userdataService.getLastFileVersion(dev.userid, groupName, FILE_TYPE_FAVOURITES);
            if (userFile.details == null) {
                userFile.details = new JsonObject();
            }
            WebGpxParser.TrackData trackData = gpxService.buildTrackDataFromGpxFile(file, null);
            userFile.details.add("trackData", gson.toJsonTree(gsonWithNans.toJson(trackData)));
            resp = new UserdataService.ResponseFileStatus(userFile);
        }
        return resp;
    }

    @Nullable
    public GpxFile createGpxFile(String groupName, CloudUserDevicesRepository.CloudUserDevice dev, Long updatetime) throws IOException {
        CloudUserFilesRepository.UserFile userGroupFile = userdataService.getLastFileVersion(dev.userid, groupName, FILE_TYPE_FAVOURITES);
        if (userGroupFile == null || userGroupFile.filesize == -1) {
            if (groupName.equals(DEFAULT_GROUP_FILE_NAME)) {
                userGroupFile = createDefaultGroup(groupName, dev, updatetime);
            } else {
                throw new OsmAndPublicApiException(UserdataService.ERROR_CODE_FILE_NOT_AVAILABLE, UserdataService.ERROR_MESSAGE_FILE_IS_NOT_AVAILABLE);
            }
        }
        if (updatetime != null && userGroupFile.updatetime.getTime() != updatetime) {
            throw new OsmAndPublicApiException(UserdataService.ERROR_CODE_FILE_NOT_AVAILABLE, ERROR_FILE_WAS_CHANGED_MSG);
        }
        InputStream in = userGroupFile.data != null ? new ByteArrayInputStream(userGroupFile.data) : userdataService.getInputStream(userGroupFile);
        if (in != null) {
            in = new GZIPInputStream(in);
            GpxFile gpxFile;
            try (Source source = new Buffer().readFrom(in)) {
                gpxFile = GpxUtilities.INSTANCE.loadGpxFile(source);
            } catch (IOException e) {
                throw new OsmAndPublicApiException(HttpStatus.BAD_REQUEST.value(), ERROR_READING_GPX_MSG);
            }
            if (gpxFile.getError() != null) {
                throw new OsmAndPublicApiException(HttpStatus.BAD_REQUEST.value(), ERROR_READING_GPX_MSG);
            } else {
                return gpxFile;
            }
        }
        return null;
    }
    
    private CloudUserFilesRepository.UserFile createDefaultGroup(String groupName, CloudUserDevicesRepository.CloudUserDevice dev, Long updatetime) throws IOException {
        GpxFile gpxFile = new GpxFile(OSMAND_ROUTER_V2);
        gpxFile.getMetadata().setName(DEFAULT_GROUP_NAME);
        File tmpGpx = gpxService.createTmpFileByGpxFile(gpxFile, groupName);
        uploadFavoriteFile(tmpGpx, dev, groupName, updatetime);
        return userdataService.getLastFileVersion(dev.userid, groupName, FILE_TYPE_FAVOURITES);
    }
    
    public ResponseEntity<String> addNewGroup(WebGpxParser.TrackData trackData, String groupName, CloudUserDevicesRepository.CloudUserDevice dev) throws IOException {
        GpxFile gpxFile = webGpxParser.createGpxFileFromTrackData(trackData);
        gpxFile.getMetadata().setName(groupName);
        String name = DEFAULT_GROUP_NAME + "-" + groupName + FILE_EXT_GPX;
        File tmpGpx = gpxService.createTmpFileByGpxFile(gpxFile, name);
        uploadFavoriteFile(tmpGpx, dev, name, null);
        UserdataService.ResponseFileStatus resp = createResponse(dev, name, gpxFile, tmpGpx);
        if (resp != null) {
            JsonObject obj = new JsonObject();
            obj.add("pointGroups", gson.toJsonTree(gsonWithNans.toJson(webGpxParser.getPointsGroups(gpxFile))));
            resp.setJsonObject(obj);
        }
        return ResponseEntity.ok(gson.toJson(resp));
    }

    public ResponseEntity<String> updateGroup(String fileName, WebGpxParser.TrackData updateGroupData, String groupName,
                                              CloudUserDevicesRepository.CloudUserDevice dev, Long updatetime) throws IOException {
        GpxFile gpxFile = createGpxFile(fileName, dev, updatetime);
        GpxFile gpxUpdateGroupData = webGpxParser.createGpxFileFromTrackData(updateGroupData);
        if (gpxFile == null) {
            throw new OsmAndPublicApiException(UserdataService.ERROR_CODE_FILE_NOT_AVAILABLE,
                    UserdataService.ERROR_MESSAGE_FILE_IS_NOT_AVAILABLE);
        }
        gpxFile.updatePointsGroup(groupName, gpxUpdateGroupData.getPointsGroups().get(groupName));
        File tmpGpx = gpxService.createTmpFileByGpxFile(gpxFile, fileName);
        uploadFavoriteFile(tmpGpx, dev, fileName, updatetime);
        UserdataService.ResponseFileStatus resp = createResponse(dev, fileName, gpxFile, tmpGpx);
        return ResponseEntity.ok(gson.toJson(resp));
    }
    
    public CloudUserDevicesRepository.CloudUserDevice getUserId() {
        Object user = SecurityContextHolder.getContext().getAuthentication().getPrincipal();
        CloudUserDevicesRepository.CloudUserDevice dev = null;
        if (user instanceof WebSecurityConfiguration.OsmAndProUser) {
            dev = ((WebSecurityConfiguration.OsmAndProUser) user).getUserDevice();
        }
        if (dev == null) {
            throw new OsmAndPublicApiException(UserdataService.ERROR_CODE_PROVIDED_TOKEN_IS_NOT_VALID, "provided deviceid or token is not valid");
        }
        return dev;
    }
}
