package net.osmand.server.api.services;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import net.osmand.gpx.GPXFile;
import net.osmand.gpx.GPXUtilities;
import net.osmand.server.WebSecurityConfiguration;
import net.osmand.server.api.repo.PremiumUserDevicesRepository;
import net.osmand.server.api.repo.PremiumUserFilesRepository;
import net.osmand.server.utils.WebGpxParser;
import net.osmand.server.utils.exception.OsmAndPublicApiException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.security.core.context.SecurityContextHolder;
import org.springframework.stereotype.Service;

import javax.transaction.Transactional;
import java.io.ByteArrayInputStream;
import java.io.File;
import java.io.IOException;
import java.io.InputStream;
import java.util.zip.GZIPInputStream;

import static net.osmand.router.RouteExporter.OSMAND_ROUTER_V2;

@Service
public class FavoriteService {
    
    public static final String ERROR_WRITING_GPX_MSG = "Error writing gpx!";
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
    public ResponseEntity<String> renameFavFolder(String oldName, String newName, StorageService.InternalZipFile fl, PremiumUserDevicesRepository.PremiumUserDevice dev) throws IOException {
        userdataService.validateUserForUpload(dev, FILE_TYPE_FAVOURITES, fl.getSize());
        userdataService.uploadFile(fl, dev, newName, FILE_TYPE_FAVOURITES, System.currentTimeMillis());
        userdataService.deleteFile(oldName, FILE_TYPE_FAVOURITES, null, null, dev);
        
        return userdataService.ok();
    }
    
    public void uploadFavoriteFile(File tmpFile, PremiumUserDevicesRepository.PremiumUserDevice dev, String name, Long updatetime) throws IOException {
        StorageService.InternalZipFile fl = StorageService.InternalZipFile.buildFromFile(tmpFile);
        userdataService.validateUserForUpload(dev, FILE_TYPE_FAVOURITES, fl.getSize());
        userdataService.uploadFile(fl, dev, name, FILE_TYPE_FAVOURITES, System.currentTimeMillis());
        if (updatetime != null) {
            userdataService.deleteFileVersion(updatetime, dev.userid, name, FILE_TYPE_FAVOURITES, null);
        }
    }
    
    public ResponseEntity<String> updateFavoriteFile(String fileName, PremiumUserDevicesRepository.PremiumUserDevice dev,
                                                     Long updatetime, GPXFile file) throws IOException {
        File tmpGpx = createTmpGpxFile(file, fileName);
        uploadFavoriteFile(tmpGpx, dev, fileName, updatetime);
        UserdataService.ResponseFileStatus resp = createResponse(dev, fileName, file, tmpGpx);
        
        return ResponseEntity.ok(gson.toJson(resp));
    }
    
    public UserdataService.ResponseFileStatus createResponse(PremiumUserDevicesRepository.PremiumUserDevice dev,
                                                             String groupName, GPXFile file, File tmpFile) {
        UserdataService.ResponseFileStatus resp = null;
        if (file != null && tmpFile != null) {
            PremiumUserFilesRepository.UserFile userFile = userdataService.getLastFileVersion(dev.userid, groupName, FILE_TYPE_FAVOURITES);
            if (userFile.details == null) {
                userFile.details = new JsonObject();
            }
            WebGpxParser.TrackData trackData = gpxService.getTrackDataByGpxFile(file, tmpFile);
            userFile.details.add("trackData", gson.toJsonTree(gsonWithNans.toJson(trackData)));
            resp = new UserdataService.ResponseFileStatus(userFile);
        }
        return resp;
    }
    
    public File createTmpGpxFile(GPXFile file, String fileName) throws IOException {
        File tmpGpx = File.createTempFile(fileName, FILE_EXT_GPX);
        Exception exception = GPXUtilities.writeGpxFile(tmpGpx, file);
        if (exception != null) {
            throw new OsmAndPublicApiException(HttpStatus.BAD_REQUEST.value(), ERROR_WRITING_GPX_MSG);
        }
        return tmpGpx;
    }
    
    public GPXFile createGpxFile(String groupName, PremiumUserDevicesRepository.PremiumUserDevice dev, Long updatetime) throws IOException {
        PremiumUserFilesRepository.UserFile userGroupFile = userdataService.getLastFileVersion(dev.userid, groupName, FILE_TYPE_FAVOURITES);
        if (userGroupFile == null) {
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
            GPXFile gpxFile = GPXUtilities.loadGPXFile(new GZIPInputStream(in));
            if (gpxFile.error != null) {
                throw new OsmAndPublicApiException(HttpStatus.BAD_REQUEST.value(), ERROR_READING_GPX_MSG);
            } else {
                return gpxFile;
            }
        }
        return null;
    }
    
    private PremiumUserFilesRepository.UserFile createDefaultGroup(String groupName, PremiumUserDevicesRepository.PremiumUserDevice dev, Long updatetime) throws IOException {
        GPXFile file = new GPXFile(OSMAND_ROUTER_V2);
        file.metadata.name = DEFAULT_GROUP_NAME;
        File tmpGpx = createTmpGpxFile(file, groupName);
        uploadFavoriteFile(tmpGpx, dev, groupName, updatetime);
        return userdataService.getLastFileVersion(dev.userid, groupName, FILE_TYPE_FAVOURITES);
    }
    
    public ResponseEntity<String> addNewGroup(WebGpxParser.TrackData trackData, String groupName, PremiumUserDevicesRepository.PremiumUserDevice dev) throws IOException {
        GPXFile gpxFile = webGpxParser.createGpxFileFromTrackData(trackData);
        gpxFile.metadata.name = groupName;
        String name = DEFAULT_GROUP_NAME + "-" + groupName + FILE_EXT_GPX;
        File tmpGpx = createTmpGpxFile(gpxFile, name);
        uploadFavoriteFile(tmpGpx, dev, name, null);
        UserdataService.ResponseFileStatus resp = createResponse(dev, name, gpxFile, tmpGpx);
        if (resp != null) {
            JsonObject obj = new JsonObject();
            obj.add("pointGroups", gson.toJsonTree(gsonWithNans.toJson(webGpxParser.getPointsGroups(gpxFile))));
            resp.setJsonObject(obj);
        }
        return ResponseEntity.ok(gson.toJson(resp));
    }
    
    public PremiumUserDevicesRepository.PremiumUserDevice getUserId() {
        Object user = SecurityContextHolder.getContext().getAuthentication().getPrincipal();
        PremiumUserDevicesRepository.PremiumUserDevice dev = null;
        if (user instanceof WebSecurityConfiguration.OsmAndProUser) {
            dev = ((WebSecurityConfiguration.OsmAndProUser) user).getUserDevice();
        }
        if (dev == null) {
            throw new OsmAndPublicApiException(UserdataService.ERROR_CODE_PROVIDED_TOKEN_IS_NOT_VALID, "provided deviceid or token is not valid");
        }
        return dev;
    }
}
