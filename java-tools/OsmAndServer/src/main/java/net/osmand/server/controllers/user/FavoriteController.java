package net.osmand.server.controllers.user;

import com.google.gson.Gson;
import com.google.gson.GsonBuilder;
import net.osmand.GPXUtilities;
import net.osmand.server.api.repo.PremiumUserDevicesRepository;
import net.osmand.server.api.repo.PremiumUserFilesRepository;
import net.osmand.server.api.services.GpxService;
import net.osmand.server.api.services.UserdataService;
import net.osmand.server.utils.WebGpxParser;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.io.*;
import java.util.Map;
import java.util.zip.GZIPInputStream;

@Controller
@RequestMapping("/mapapi/fav")
public class FavoriteController {
    
    public static final String ADD_FAVORITE = "add";
    public static final String DELETE_FAVORITE = "delete";
    
    @Autowired
    UserdataService userdataService;
    
    @Autowired
    WebGpxParser webGpxParser;
    
    @Autowired
    protected GpxService gpxService;
    
    Gson gson = new Gson();
    
    Gson gsonWithNans = new GsonBuilder().serializeSpecialFloatingPointValues().create();
    
    @PostMapping(value = "/delete")
    @ResponseBody
    public ResponseEntity<String> deleteFav(@RequestBody String data,
                                            @RequestParam String fileName,
                                            @RequestParam String fileType,
                                            @RequestParam String updatetime) throws IOException {
        WebGpxParser.Wpt wpt = gson.fromJson(data, WebGpxParser.Wpt.class);
        
        PremiumUserDevicesRepository.PremiumUserDevice dev = userdataService.checkUser();
        ResponseEntity<String> validateError = userdataService.validate(dev);
        if (validateError != null) {
            return validateError;
        }
        
        return addOrDeleteFavorite(wpt, fileName, dev, fileType, updatetime, DELETE_FAVORITE);
    }
    
    @PostMapping(value = "/add")
    @ResponseBody
    public ResponseEntity<String> addFav(@RequestBody String data,
                                         @RequestParam String fileName,
                                         @RequestParam String fileType,
                                         @RequestParam String updatetime) throws IOException {
        WebGpxParser.Wpt wpt = gson.fromJson(data, WebGpxParser.Wpt.class);
        
        PremiumUserDevicesRepository.PremiumUserDevice dev = userdataService.checkUser();
        ResponseEntity<String> validateError = userdataService.validate(dev);
        if (validateError != null) {
            return validateError;
        }
        
        return addOrDeleteFavorite(wpt, fileName, dev, fileType, updatetime, ADD_FAVORITE);
    }
    
    @PostMapping(value = "/update")
    @ResponseBody
    public ResponseEntity<String> updateFav(@RequestBody String data,
                                            @RequestParam String wptName,
                                            @RequestParam String oldGroupName,
                                            @RequestParam String newGroupName,
                                            @RequestParam String oldGroupUpdatetime,
                                            @RequestParam String newGroupUpdatetime,
                                            @RequestParam String fileType,
                                            @RequestParam int ind) throws IOException {
        WebGpxParser.Wpt wpt = gson.fromJson(data, WebGpxParser.Wpt.class);
        
        PremiumUserDevicesRepository.PremiumUserDevice dev = userdataService.checkUser();
        ResponseEntity<String> validateError = userdataService.validate(dev);
        if (validateError != null) {
            return validateError;
        }
        
        return updateFavorite(wpt, wptName, oldGroupName, newGroupName, oldGroupUpdatetime, newGroupUpdatetime, dev, fileType, ind);
    }
    
    private ResponseEntity<String> updateFavorite(WebGpxParser.Wpt wpt,
                                                  String wptName,
                                                  String oldGroupName,
                                                  String newGroupName,
                                                  String oldGroupUpdatetime,
                                                  String newGroupUpdatetime,
                                                  PremiumUserDevicesRepository.PremiumUserDevice dev,
                                                  String fileType,
                                                  int ind) throws IOException {
        
        PreparedFavoriteFile preparedNewFavoriteGroup = prepareFile(newGroupName, fileType, dev, newGroupUpdatetime);
        if (preparedNewFavoriteGroup.error != null) {
            return preparedNewFavoriteGroup.error;
        }
        PreparedFavoriteFile preparedOldFavoriteGroup = null;
        boolean changeGroup = !oldGroupName.equals(newGroupName);
        if (changeGroup) {
            preparedOldFavoriteGroup = prepareFile(oldGroupName, fileType, dev, oldGroupUpdatetime);
            if (preparedOldFavoriteGroup.error != null) {
                return preparedOldFavoriteGroup.error;
            }
        }
        
        preparedNewFavoriteGroup.file.updateWptPtWeb(webGpxParser.updateWpt(wpt), wptName, ind);
        
        File newTmpGpx = File.createTempFile(newGroupName, ".gpx");
        File oldTmpGpx = null;
        Exception exception = GPXUtilities.writeGpxFile(newTmpGpx, preparedNewFavoriteGroup.file);
        if (exception != null) {
            return ResponseEntity.badRequest().body("Error writing gpx!");
        }
        
        ResponseEntity<String> error = updateFile(newTmpGpx, dev, newGroupName, fileType, newGroupUpdatetime);
        if (error != null) {
            return error;
        } else {
            if (changeGroup) {
                preparedOldFavoriteGroup.file.deleteWptPt(wptName, ind);
                oldTmpGpx = File.createTempFile(oldGroupName, ".gpx");
                exception = GPXUtilities.writeGpxFile(oldTmpGpx, preparedOldFavoriteGroup.file);
                if (exception != null) {
                    return ResponseEntity.badRequest().body("Error writing gpx!");
                }
                error = updateFile(oldTmpGpx, dev, oldGroupName, fileType, oldGroupUpdatetime);
                if (error != null) {
                    return error;
                }
            }
        }
        
        PremiumUserFilesRepository.UserFile updatedNewGroupFile = userdataService.getLastFileVersion(dev.userid, newGroupName, fileType);
        WebGpxParser.TrackData newGroupTrackData = gpxService.getTrackDataByGpxFile(preparedNewFavoriteGroup.file, newTmpGpx);
        PremiumUserFilesRepository.UserFile updatedOldGroupFile = changeGroup ? userdataService.getLastFileVersion(dev.userid, oldGroupName, fileType) : null;
        WebGpxParser.TrackData oldGroupTrackData = changeGroup ? gpxService.getTrackDataByGpxFile(preparedOldFavoriteGroup.file, oldTmpGpx) : null;
        
        
        Map<String, String> res = new java.util.HashMap<>(Map.of(
                "newGroupClienttimems", String.valueOf(updatedNewGroupFile.clienttime.getTime()),
                "newGroupUpdatetimems", String.valueOf(updatedNewGroupFile.updatetime.getTime()),
                "newGroupTrackData", gsonWithNans.toJson(newGroupTrackData)));
        
        if (changeGroup) {
            res.put("oldGroupClienttimems", String.valueOf(updatedOldGroupFile.clienttime.getTime()));
            res.put("oldGroupUpdatetimems", String.valueOf(updatedOldGroupFile.updatetime.getTime()));
            res.put("oldGroupTrackData", gsonWithNans.toJson(oldGroupTrackData));
        }
        
        return ResponseEntity.ok(gson.toJson(res));
    }
    
    private ResponseEntity<String> addOrDeleteFavorite(WebGpxParser.Wpt wpt,
                                                      String fileName,
                                                      PremiumUserDevicesRepository.PremiumUserDevice dev,
                                                      String fileType,
                                                      String updatetime,
                                                      String action) throws IOException {
        PreparedFavoriteFile preparedFavoriteGroup = prepareFile(fileName, fileType, dev, updatetime);
        if (preparedFavoriteGroup.error != null) {
            return preparedFavoriteGroup.error;
        }
        GPXUtilities.WptPt wptPt = webGpxParser.updateWpt(wpt);
        
        if (action.equals(ADD_FAVORITE)) {
            preparedFavoriteGroup.file.addPoint(wptPt);
        } else if (action.equals(DELETE_FAVORITE)) {
            preparedFavoriteGroup.file.deleteWptPt(wptPt);
        }
        
        File tmpGpx = File.createTempFile(fileName, ".gpx");
        Exception exception = GPXUtilities.writeGpxFile(tmpGpx, preparedFavoriteGroup.file);
        if (exception != null) {
            return ResponseEntity.badRequest().body("Error writing gpx!");
        }
        
        ResponseEntity<String> error = updateFile(tmpGpx, dev, fileName, fileType, updatetime);
        if (error != null) {
            return error;
        }
        PremiumUserFilesRepository.UserFile newFile = userdataService.getLastFileVersion(dev.userid, fileName, fileType);
        WebGpxParser.TrackData trackData = gpxService.getTrackDataByGpxFile(preparedFavoriteGroup.file, tmpGpx);
        
        return ResponseEntity.ok(gson.toJson(Map.of(
                "clienttimems", newFile.clienttime.getTime(),
                "updatetimems", newFile.updatetime.getTime(),
                "trackData", gsonWithNans.toJson(trackData))));
    }
    
    static class PreparedFavoriteFile {
        GPXUtilities.GPXFile file;
        ResponseEntity<String> error;
        
        PreparedFavoriteFile(GPXUtilities.GPXFile file, ResponseEntity<String> error) {
            this.file = file;
            this.error = error;
        }
    }
    
    private PreparedFavoriteFile prepareFile(String groupName,
                                            String fileType,
                                            PremiumUserDevicesRepository.PremiumUserDevice dev,
                                            String updatetime) throws IOException {
        PremiumUserFilesRepository.UserFile userGroupFile = userdataService.getLastFileVersion(dev.userid, groupName, fileType);
        if (userGroupFile == null) {
            return new PreparedFavoriteFile(null, userdataService.error(UserdataService.ERROR_CODE_FILE_NOT_AVAILABLE, UserdataService.ERROR_MESSAGE_FILE_IS_NOT_AVAILABLE));
        }
        if (!Long.toString(userGroupFile.updatetime.getTime()).equals(updatetime)) {
            return new PreparedFavoriteFile(null, userdataService.error(UserdataService.ERROR_CODE_FILE_NOT_AVAILABLE, "File was changed"));
        }
        InputStream in = userGroupFile.data != null ? new ByteArrayInputStream(userGroupFile.data) : userdataService.getInputStream(userGroupFile);
        if (in != null) {
            GPXUtilities.GPXFile gpxFile = GPXUtilities.loadGPXFile(new GZIPInputStream(in));
            if (gpxFile.error != null) {
                return new PreparedFavoriteFile(null, ResponseEntity.badRequest().body("Error reading gpx!"));
            } else {
                return new PreparedFavoriteFile(gpxFile, null);
            }
        }
        return new PreparedFavoriteFile(null, ResponseEntity.badRequest().body("Error prepare gpx file!"));
    }
    
    private ResponseEntity<String> updateFile(File file,
                                              PremiumUserDevicesRepository.PremiumUserDevice dev,
                                              String fileName,
                                              String fileType,
                                              String updatetime) throws IOException {
        ResponseEntity<String> uploadError = userdataService.uploadFile(file, dev, fileName, fileType, System.currentTimeMillis());
        if (uploadError != null) {
            return uploadError;
        }
        //delete prev version
        ResponseEntity<String> response = userdataService.deleteFileVersion(Long.parseLong(updatetime), dev.userid, fileName, fileType, null);
        if (!response.getStatusCode().is2xxSuccessful()) {
            return response;
        }
        return null;
    }
}
