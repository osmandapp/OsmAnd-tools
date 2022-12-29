package net.osmand.server.controllers.user;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import net.osmand.gpx.GPXFile;
import net.osmand.gpx.GPXUtilities;
import net.osmand.server.api.repo.PremiumUserDevicesRepository;
import net.osmand.server.api.repo.PremiumUserFilesRepository;
import net.osmand.server.api.services.GpxService;
import net.osmand.server.api.services.StorageService.InternalZipFile;
import net.osmand.server.api.services.UserdataService;
import net.osmand.server.utils.WebGpxParser;
import net.osmand.server.utils.exception.OsmAndPublicApiException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.io.*;
import java.util.Map;
import java.util.zip.GZIPInputStream;

@Controller
@RequestMapping("/mapapi/fav")
public class FavoriteController {
    
    public static final String FILE_TYPE_FAVOURITES = "favourites";
    public static final String FILE_EXT_GPX = ".gpx";
    public static final String ERROR_WRITING_GPX_MSG = "Error writing gpx!";
    public static final String ERROR_FILE_WAS_CHANGED_MSG = "File was changed!";
    public static final String ERROR_READING_GPX_MSG = "Error reading gpx!";
    
    @Autowired
    UserdataService userdataService;
    
    @Autowired
    WebGpxParser webGpxParser;
    
    @Autowired
    protected GpxService gpxService;
    
    Gson gson = new Gson();
    JsonParser jsonParser = new JsonParser();
    
    Gson gsonWithNans = new GsonBuilder().serializeSpecialFloatingPointValues().create();
    
    @PostMapping(value = "/delete")
    @ResponseBody
    public ResponseEntity<String> deleteFav(@RequestBody String data,
                                            @RequestParam String fileName,
                                            @RequestParam Long updatetime) throws IOException {
        PremiumUserDevicesRepository.PremiumUserDevice dev = userdataService.checkValidateUser();
        GPXFile file = createGpxFile(fileName, dev, updatetime);
        if (file != null) {
            file.deleteWptPt(webGpxParser.convertToWptPt(gson.fromJson(data, WebGpxParser.Wpt.class)));
        } else
            throw new OsmAndPublicApiException(UserdataService.ERROR_CODE_FILE_NOT_AVAILABLE,
                    UserdataService.ERROR_MESSAGE_FILE_IS_NOT_AVAILABLE);
        
        return updateFavoriteFile(fileName, dev, updatetime, file);
    }
    
    @PostMapping(value = "/add")
    @ResponseBody
    public ResponseEntity<String> addFav(@RequestBody String data,
                                         @RequestParam String fileName,
                                         @RequestParam Long updatetime) throws IOException {
        PremiumUserDevicesRepository.PremiumUserDevice dev = userdataService.checkValidateUser();
        GPXFile file = createGpxFile(fileName, dev, updatetime);
        if (file != null) {
            file.addPoint(webGpxParser.convertToWptPt(gson.fromJson(data, WebGpxParser.Wpt.class)));
        } else
            throw new OsmAndPublicApiException(UserdataService.ERROR_CODE_FILE_NOT_AVAILABLE,
                    UserdataService.ERROR_MESSAGE_FILE_IS_NOT_AVAILABLE);
        
        return updateFavoriteFile(fileName, dev, updatetime, file);
    }
    
    @PostMapping(value = "/update")
    @ResponseBody
    public ResponseEntity<String> updateFav(@RequestBody String data,
                                            @RequestParam String wptName,
                                            @RequestParam String oldGroupName,
                                            @RequestParam String newGroupName,
                                            @RequestParam Long oldGroupUpdatetime,
                                            @RequestParam Long newGroupUpdatetime,
                                            @RequestParam int ind) throws IOException {
        PremiumUserDevicesRepository.PremiumUserDevice dev = userdataService.checkValidateUser();
        GPXUtilities.WptPt wptPt = webGpxParser.convertToWptPt(gson.fromJson(data, WebGpxParser.Wpt.class));
        
        GPXFile newGpxFile = createGpxFile(newGroupName, dev, newGroupUpdatetime);
        if (newGpxFile != null) {
            newGpxFile.updateWptPt(wptName, ind, wptPt);
        } else
            throw new OsmAndPublicApiException(UserdataService.ERROR_CODE_FILE_NOT_AVAILABLE,
                    UserdataService.ERROR_MESSAGE_FILE_IS_NOT_AVAILABLE);
        
        boolean changeGroup = !oldGroupName.equals(newGroupName);
        GPXFile oldGpxFile = null;
        if (changeGroup) {
            oldGpxFile = createGpxFile(oldGroupName, dev, oldGroupUpdatetime);
            if (oldGpxFile != null) {
                oldGpxFile.deleteWptPt(wptPt);
            } else
                throw new OsmAndPublicApiException(UserdataService.ERROR_CODE_FILE_NOT_AVAILABLE,
                        UserdataService.ERROR_MESSAGE_FILE_IS_NOT_AVAILABLE);
        }
        
        File newTmpGpx = createTmpGpxFile(newGpxFile, newGroupName);
        uploadFavoriteFile(newTmpGpx, dev, newGroupName, newGroupUpdatetime);
        
        File oldTmpGpx = null;
        if (changeGroup) {
            oldTmpGpx = createTmpGpxFile(oldGpxFile, oldGroupName);
            uploadFavoriteFile(oldTmpGpx, dev, oldGroupName, oldGroupUpdatetime);
        }
        
        UserdataService.ResponseFileStatus respNewGroup = createResponse(dev, newGroupName, newGpxFile, newTmpGpx);
        UserdataService.ResponseFileStatus respOldGroup = changeGroup ? createResponse(dev, oldGroupName, oldGpxFile, oldTmpGpx) : null;
        
        return ResponseEntity.ok(gson.toJson(Map.of(
                "respNewGroup", respNewGroup,
                "respOldGroup", respOldGroup != null ? respOldGroup : "")));
    }
    
    private void uploadFavoriteFile(File tmpFile, PremiumUserDevicesRepository.PremiumUserDevice dev, String name, Long updatetime) throws IOException {
        userdataService.uploadFile(InternalZipFile.buildFromFile(tmpFile), dev, name, FILE_TYPE_FAVOURITES, System.currentTimeMillis());
        userdataService.deleteFileVersion(updatetime, dev.userid, name, FILE_TYPE_FAVOURITES, null);
    }
    
    private ResponseEntity<String> updateFavoriteFile(String fileName, PremiumUserDevicesRepository.PremiumUserDevice dev,
                                                      Long updatetime, GPXFile file) throws IOException {
        File tmpGpx = createTmpGpxFile(file, fileName);
        uploadFavoriteFile(tmpGpx, dev, fileName, updatetime);
        
        PremiumUserFilesRepository.UserFile newFile = userdataService.getLastFileVersion(dev.userid, fileName, FILE_TYPE_FAVOURITES);
        WebGpxParser.TrackData trackData = gpxService.getTrackDataByGpxFile(file, tmpGpx);
        
        UserdataService.ResponseFileStatus resp = new UserdataService.ResponseFileStatus(newFile);
        resp.setJsonObject((JsonObject) jsonParser.parse(gsonWithNans.toJson(trackData)));
        
        return ResponseEntity.ok(gson.toJson(resp));
    }
    
    private UserdataService.ResponseFileStatus createResponse(PremiumUserDevicesRepository.PremiumUserDevice dev,
                                                              String groupName, GPXFile file, File tmpFile) {
        UserdataService.ResponseFileStatus resp = null;
        if (file != null && tmpFile != null) {
            PremiumUserFilesRepository.UserFile userFile = userdataService.getLastFileVersion(dev.userid, groupName, FILE_TYPE_FAVOURITES);
            resp = new UserdataService.ResponseFileStatus(userFile);
            resp.setJsonObject((JsonObject) jsonParser.parse(gsonWithNans.toJson(gpxService.getTrackDataByGpxFile(file, tmpFile))));
        }
        return resp;
    }
    
    private File createTmpGpxFile(GPXFile file, String fileName) throws IOException {
        File tmpGpx = File.createTempFile(fileName, FILE_EXT_GPX);
        Exception exception = GPXUtilities.writeGpxFile(tmpGpx, file);
        if (exception != null) {
            throw new OsmAndPublicApiException(HttpStatus.BAD_REQUEST.value(), ERROR_WRITING_GPX_MSG);
        }
        return tmpGpx;
    }
    
    private GPXFile createGpxFile(String groupName, PremiumUserDevicesRepository.PremiumUserDevice dev, Long updatetime) throws IOException {
        PremiumUserFilesRepository.UserFile userGroupFile = userdataService.getLastFileVersion(dev.userid, groupName, FILE_TYPE_FAVOURITES);
        if (userGroupFile == null) {
            throw new OsmAndPublicApiException(UserdataService.ERROR_CODE_FILE_NOT_AVAILABLE, UserdataService.ERROR_MESSAGE_FILE_IS_NOT_AVAILABLE);
        }
        if (userGroupFile.updatetime.getTime() == updatetime) {
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
}
