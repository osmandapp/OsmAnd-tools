package net.osmand.server.controllers.user;

import com.google.gson.Gson;
import net.osmand.server.api.repo.CloudUserDevicesRepository;
import net.osmand.server.api.repo.CloudUserFilesRepository;
import net.osmand.server.api.services.FavoriteService;
import net.osmand.server.api.services.GpxService;
import net.osmand.server.api.services.StorageService.InternalZipFile;
import net.osmand.server.api.services.UserdataService;
import net.osmand.server.utils.WebGpxParser;
import net.osmand.server.utils.exception.OsmAndPublicApiException;
import net.osmand.shared.gpx.GpxFile;
import net.osmand.shared.gpx.GpxUtilities;
import net.osmand.shared.gpx.primitives.WptPt;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.ResponseEntity;
import org.springframework.stereotype.Controller;
import org.springframework.web.bind.annotation.*;

import java.io.*;
import java.util.*;

import static net.osmand.shared.gpx.GpxUtilities.HIDDEN_EXTENSION;
import static net.osmand.shared.gpx.GpxUtilities.PINNED_EXTENSION;

@Controller
@RequestMapping("/mapapi/fav")
public class FavoriteController {
    
    @Autowired
    UserdataService userdataService;
    
    @Autowired
    WebGpxParser webGpxParser;
    
    @Autowired
    protected GpxService gpxService;
    
    @Autowired
    protected FavoriteService favoriteService;
    
    Gson gson = new Gson();
    
    @PostMapping(value = "/delete")
    @ResponseBody
    public ResponseEntity<String> deleteFav(@RequestBody String data,
                                            @RequestParam String fileName,
                                            @RequestParam Long updatetime) throws IOException {
        CloudUserDevicesRepository.CloudUserDevice dev = favoriteService.getUserId();
        GpxFile file = favoriteService.createGpxFile(fileName, dev, updatetime);
        if (file != null) {
            file.deleteWptPt(webGpxParser.convertToWptPt(gson.fromJson(data, WebGpxParser.Wpt.class)));
        } else
            throw new OsmAndPublicApiException(UserdataService.ERROR_CODE_FILE_NOT_AVAILABLE,
                    UserdataService.ERROR_MESSAGE_FILE_IS_NOT_AVAILABLE);
        
        return favoriteService.updateFavoriteFile(fileName, dev, updatetime, file);
    }
    
    @PostMapping(value = "/update-all-favorites")
    @ResponseBody
    public ResponseEntity<String> updateAllFavorites(@RequestBody List<String> data,
                                                     @RequestParam String fileName,
                                                     @RequestParam String groupName,
                                                     @RequestParam Long updatetime,
                                                     @RequestParam boolean updateTimestamp) throws IOException {
        CloudUserDevicesRepository.CloudUserDevice dev = favoriteService.getUserId();
        GpxFile file = favoriteService.createGpxFile(fileName, dev, updatetime);
        UserdataService.ResponseFileStatus respNewGroup;
        if (file != null) {
            boolean hidden = false;
            boolean pinned = false;
            for (String d : data) {
                WptPt wptPt = webGpxParser.convertToWptPt(gson.fromJson(d, WebGpxParser.Wpt.class));
                if (!hidden) {
                    hidden = Objects.requireNonNull(wptPt.getExtensions()).get(HIDDEN_EXTENSION) != null
                            && wptPt.getExtensions().get(HIDDEN_EXTENSION).equals("true");
                }
                if (!pinned) {
                    pinned = Objects.requireNonNull(wptPt.getExtensions()).get(PINNED_EXTENSION) != null
                            && wptPt.getExtensions().get(PINNED_EXTENSION).equals("true");
                }
                file.updateWptPt(Objects.requireNonNull(wptPt.getName()), data.indexOf(d), wptPt, updateTimestamp);
            }
            boolean groupHidden = file.getPointsGroups().get(groupName).getHidden();
            if (groupHidden != hidden) {
                file.getPointsGroups().get(groupName).setHidden(hidden);
            }
            boolean groupPinned = file.getPointsGroups().get(groupName).getPinned();
            if (groupPinned != pinned) {
                file.getPointsGroups().get(groupName).setPinned(pinned);
            }
            file.updatePointsGroup(groupName, file.getPointsGroups().get(groupName));

            File newTmpGpx = gpxService.createTmpFileByGpxFile(file, fileName);
            Date clienttime = null;
            
            if (!updateTimestamp) {
                CloudUserFilesRepository.UserFile userFile = userdataService.getLastFileVersion(dev.userid, fileName, UserdataService.FILE_TYPE_FAVOURITES);
                if (userFile != null) {
                    clienttime = userFile.clienttime;
                }
            }
            favoriteService.uploadFavoriteFile(newTmpGpx, dev, fileName, updatetime, clienttime);
            respNewGroup = favoriteService.createResponse(dev, fileName, file, newTmpGpx);
        } else
            throw new OsmAndPublicApiException(UserdataService.ERROR_CODE_FILE_NOT_AVAILABLE,
                    UserdataService.ERROR_MESSAGE_FILE_IS_NOT_AVAILABLE);
        
        return ResponseEntity.ok(gson.toJson(Map.of(
                "respNewGroup", respNewGroup,
                "respOldGroup", "")));
    }
    
    @PostMapping(value = "/add")
    @ResponseBody
    public ResponseEntity<String> addFav(@RequestBody String data,
                                         @RequestParam String fileName,
                                         @RequestParam(required = false) Long updatetime) throws IOException {
        CloudUserDevicesRepository.CloudUserDevice dev = favoriteService.getUserId();
        GpxFile file = favoriteService.createGpxFile(fileName, dev, updatetime);
        if (file != null) {
            file.addPoint(webGpxParser.convertToWptPt(gson.fromJson(data, WebGpxParser.Wpt.class)));
        } else
            throw new OsmAndPublicApiException(UserdataService.ERROR_CODE_FILE_NOT_AVAILABLE,
                    UserdataService.ERROR_MESSAGE_FILE_IS_NOT_AVAILABLE);
        if (updatetime == null) {
            CloudUserFilesRepository.UserFile userGroupFile = userdataService.getLastFileVersion(dev.userid, fileName, UserdataService.FILE_TYPE_FAVOURITES);
            updatetime = userGroupFile.updatetime.getTime();
        }
        return favoriteService.updateFavoriteFile(fileName, dev, updatetime, file);
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
        CloudUserDevicesRepository.CloudUserDevice dev = favoriteService.getUserId();
        WptPt wptPt = webGpxParser.convertToWptPt(gson.fromJson(data, WebGpxParser.Wpt.class));
        GpxFile newGpxFile = favoriteService.createGpxFile(newGroupName, dev, newGroupUpdatetime);
        if (newGpxFile != null) {
            newGpxFile.updateWptPt(wptName, ind, wptPt, true);
        } else
            throw new OsmAndPublicApiException(UserdataService.ERROR_CODE_FILE_NOT_AVAILABLE,
                    UserdataService.ERROR_MESSAGE_FILE_IS_NOT_AVAILABLE);
        
        boolean changeGroup = !oldGroupName.equals(newGroupName);
        GpxFile oldGpxFile = null;
        if (changeGroup) {
            oldGpxFile = favoriteService.createGpxFile(oldGroupName, dev, oldGroupUpdatetime);
            if (oldGpxFile != null) {
                oldGpxFile.deleteWptPt(wptName, ind);
            } else
                throw new OsmAndPublicApiException(UserdataService.ERROR_CODE_FILE_NOT_AVAILABLE,
                        UserdataService.ERROR_MESSAGE_FILE_IS_NOT_AVAILABLE);
        }
        
        File newTmpGpx = gpxService.createTmpFileByGpxFile(newGpxFile, newGroupName);
        favoriteService.uploadFavoriteFile(newTmpGpx, dev, newGroupName, newGroupUpdatetime);
        
        File oldTmpGpx = null;
        if (changeGroup) {
            oldTmpGpx = gpxService.createTmpFileByGpxFile(oldGpxFile, oldGroupName);
            favoriteService.uploadFavoriteFile(oldTmpGpx, dev, oldGroupName, oldGroupUpdatetime);
        }
        
        UserdataService.ResponseFileStatus respNewGroup = favoriteService.createResponse(dev, newGroupName, newGpxFile, newTmpGpx);
        UserdataService.ResponseFileStatus respOldGroup = changeGroup ? favoriteService.createResponse(dev, oldGroupName, oldGpxFile, oldTmpGpx) : null;
        
        return ResponseEntity.ok(gson.toJson(Map.of(
                "respNewGroup", respNewGroup,
                "respOldGroup", respOldGroup != null ? respOldGroup : "")));
    }
    
    @PostMapping(value = "/add-group")
    @ResponseBody
    public ResponseEntity<String> addGroup(@RequestBody String data, @RequestParam String groupName) throws IOException {
        CloudUserDevicesRepository.CloudUserDevice dev = favoriteService.getUserId();
        WebGpxParser.TrackData trackData = new Gson().fromJson(data, WebGpxParser.TrackData.class);
        return favoriteService.addNewGroup(trackData, groupName, dev);
    }
    
    @GetMapping(value = "/rename-fav-group")
    @ResponseBody
    public ResponseEntity<String> renameGroup(@RequestParam String oldName,
                                              @RequestParam String newName,
                                              @RequestParam String fullOldName,
                                              @RequestParam String fullNewName,
                                              @RequestParam Long oldUpdatetime) throws IOException {
        CloudUserDevicesRepository.CloudUserDevice dev = favoriteService.getUserId();
        GpxFile gpxFile = favoriteService.createGpxFile(fullOldName, dev, oldUpdatetime);
        if (gpxFile != null) {
            GpxUtilities.PointsGroup pointsGroup = gpxFile.getPointsGroups().get(oldName);
            pointsGroup.setName(newName);
            pointsGroup.getPoints().forEach(p -> p.setCategory(newName));
            gpxFile.updatePointsGroup(oldName, pointsGroup);
            
            File tmpGpx = gpxService.createTmpFileByGpxFile(gpxFile, fullNewName);
            InternalZipFile fl = InternalZipFile.buildFromFile(tmpGpx);
            
            return favoriteService.renameFavFolder(fullOldName, fullNewName, fl, dev);
        }
        return ResponseEntity.badRequest().body(FavoriteService.ERROR_READING_GPX_MSG);
    }
}
