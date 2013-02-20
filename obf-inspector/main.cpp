
#include <string>
#include <iostream>
#include <sstream>
#include <map>
#include <memory>

#include <QFile>
#include <QStringList>

#include <ObfReader.h>
#include <Utilities.h>

// Options
bool verboseAddress = false;
bool verboseMap = false;
bool verbosePoi = false;
bool verboseTrasport = false;
double latTop = 85;
double latBottom = -85;
double lonLeft = -180;
double lonRight = 180;
int zoom = 15;

// Forward declarations
void printUsage(std::string warning = std::string());
void printFileInformation(std::string fileName);
void printFileInformation(QFile* file);
void printAddressDetailedInfo(OsmAnd::ObfReader* reader, OsmAnd::ObfAddressSection* section);
std::string formatBounds(int left, int right, int top, int bottom);
std::string formatGeoBounds(double l, double r, double t, double b);

int main(int argc, char* argv[])
{
    if(argc <= 1)
    {
        printUsage();
        return -1;
    }

    std::string cmd = argv[1];
    if (cmd[0] == '-')
    {
        // command
        if (cmd == "-c" || cmd == "-combine") {
            if (argc < 5)
            {
                printUsage("Too few parameters to extract (require minimum 4)");
                return -1;
            }

            std::map<std::shared_ptr<QFile>, std::string> parts;
            /*for (int i = 3; i < argc; i++)
            {
                file = new File(args[i]);
                if (!file.exists()) {
                    System.err.std::cout << "File to extract from doesn't exist " + args[i]);
                    return;
                }
                parts.put(file, null);
                if (i < args.length - 1) {
                    if (args[i + 1].startsWith("-") || args[i + 1].startsWith("+")) {
                        parts.put(file, args[i + 1]);
                        i++;
                    }
                }
            }
            List<Float> extracted = combineParts(new File(args[1]), parts);
            if (extracted != null) {
                std::cout << "\n" + extracted.size() + " parts were successfully extracted to " + args[1]);
            }*/
        }
        else if (cmd.find("-v") == 0)
        {
            if (argc < 3)
            {
                printUsage("Missing file parameter");
                return -1;
            }

            for(int argIdx = 1; argIdx < argc - 1; argIdx++)
            {
                std::string arg = argv[argIdx];
                if(arg == "-vaddress")
                    verboseAddress = true;
                else if(arg == "-vmap")
                    verboseMap = true;
                else if(arg == "-vpoi")
                    verbosePoi = true;
                else if(arg == "-vtransport")
                    verboseTrasport = true;
                else if(arg.find("-zoom=") == 0)
                {
                    zoom = atoi(arg.c_str() + 5);
                }
                else if(arg.find("-bbox=") == 0)
                {
                    auto values = QString(arg.c_str() + 5).split(",");
                    lonLeft = values[0].toDouble();
                    latTop = values[1].toDouble();
                    lonRight = values[2].toDouble();
                    latBottom =  values[3].toDouble();
                }
            }

            printFileInformation(argv[argc - 1]);
        } else {
            printUsage("Unknown command : " + cmd);
        }
    }
    else
    {
        printFileInformation(cmd);
    }
    return 0;
}

void printUsage(std::string warning)
{
    if(!warning.empty())
        std::cout << warning << std::endl;
    std::cout << "Inspector is console utility for working with binary indexes of OsmAnd." << std::endl;
    std::cout << "It allows print info about file, extract parts and merge indexes." << std::endl;
    std::cout << "\nUsage for print info : inspector [-vaddress] [-vmap] [-vpoi] [-vtransport] [-zoom=Zoom] [-bbox=LeftLon,TopLat,RightLon,BottomLan] [file]" << std::endl;
    std::cout << "  Prints information about [file] binary index of OsmAnd." << std::endl;
    std::cout << "  -v.. more verbose output (like all cities and their streets or all map objects with tags/values and coordinates)" << std::endl;
    std::cout << "\nUsage for combining indexes : inspector -c file_to_create (file_from_extract ((+|-)parts_to_extract)? )*" << std::endl;
    std::cout << "\tCreate new file of extracted parts from input file. [parts_to_extract] could be parts to include or exclude." << std::endl;
    std::cout << "  Example : inspector -c output_file input_file +1,2,3\n\tExtracts 1, 2, 3 parts (could be find in print info)" << std::endl;
    std::cout << "  Example : inspector -c output_file input_file -2,3\n\tExtracts all parts excluding 2, 3" << std::endl;
    std::cout << "  Example : inspector -c output_file input_file1 input_file2 input_file3\n\tSimply combine 3 files" << std::endl;
    std::cout << "  Example : inspector -c output_file input_file1 input_file2 -4\n\tCombine all parts of 1st file and all parts excluding 4th part of 2nd file" << std::endl;
}

void printFileInformation(std::string fileName)
{
    QFile file(QString::fromStdString(fileName));
    if(!file.exists())
    {
        std::cout << "Binary OsmAnd index " << fileName << " was not found." << std::endl;
        return;
    }

    printFileInformation(&file);
}

void printFileInformation(QFile* file)
{
    if(!file->open(QIODevice::ReadOnly))
    {
        std::cout << "Failed to open file " << file->fileName().toStdString().c_str() << std::endl;
        return;
    }

    OsmAnd::ObfReader obfMap(file);
    std::cout << "Binary index " << file->fileName().toStdString() << " version = " << obfMap.getVersion() << std::endl;
    const auto& sections = obfMap.getSections();
    int idx = 1;
    for(auto itSection = sections.begin(); itSection != sections.end(); ++itSection, idx++)
    {
        OsmAnd::ObfSection* section = *itSection;

        std::string sectionType = "unknown";
        if(dynamic_cast<OsmAnd::ObfMapSection*>(section))
            sectionType = "Map";
        else if(dynamic_cast<OsmAnd::ObfTransportSection*>(section))
            sectionType = "Transport";
        else if(dynamic_cast<OsmAnd::ObfRoutingSection*>(section))
            sectionType = "Route";
        else if(dynamic_cast<OsmAnd::ObfPoiSection*>(section))
            sectionType = "Poi";
        else if(dynamic_cast<OsmAnd::ObfAddressSection*>(section))
            sectionType = "Address";

        std::cout << "#" << idx << " " << sectionType << " data " << section->_name << " - " << section->_length << " bytes" << std::endl;
        
        if(dynamic_cast<OsmAnd::ObfTransportSection*>(section))
        {
            auto transportSection = dynamic_cast<OsmAnd::ObfTransportSection*>(section);
            int sh = (31 - OsmAnd::ObfReader::TransportStopZoom);
            std::cout << "\t Bounds " << formatBounds(transportSection->_left << sh, transportSection->_right << sh, transportSection->_top << sh, transportSection->_bottom << sh) << std::endl;
        }
        else if(dynamic_cast<OsmAnd::ObfRoutingSection*>(section))
        {
            auto routingSection = dynamic_cast<OsmAnd::ObfRoutingSection*>(section);
            double lonLeft = 180;
            double lonRight = -180;
            double latTop = -90;
            double latBottom = 90;
            for(auto itSubregion = routingSection->_subregions.begin(); itSubregion != routingSection->_subregions.end(); ++itSubregion)
            {
                OsmAnd::ObfRoutingSection::Subregion* subregion = itSubregion->get();

                lonLeft = std::min(lonLeft, OsmAnd::Utilities::get31LongitudeX(subregion->_left));
                lonRight = std::max(lonRight, OsmAnd::Utilities::get31LongitudeX(subregion->_right));
                latTop = std::max(latTop, OsmAnd::Utilities::get31LatitudeY(subregion->_top));
                latBottom = std::min(latBottom, OsmAnd::Utilities::get31LatitudeY(subregion->_bottom));
            }
            std::cout << "\t Bounds " << formatGeoBounds(lonLeft, lonRight, latTop, latBottom) << std::endl;
        }
        else if(dynamic_cast<OsmAnd::ObfMapSection*>(section))
        {
            auto mapSection = dynamic_cast<OsmAnd::ObfMapSection*>(section);
            int levelIdx = 1;
            for(auto itLevel = mapSection->_levels.begin(); itLevel != mapSection->_levels.end(); ++itLevel, levelIdx++)
            {
                OsmAnd::ObfMapSection::MapRoot* level = itLevel->get();
                std::cout << "\t" << idx << "." << levelIdx << " Map level minZoom = " << level->_minZoom << ", maxZoom = " << level->_maxZoom << ", size = " << level->_length << " bytes" << std::endl;
                std::cout << "\t\tBounds " << formatBounds(level->_left, level->_right, level->_top, level->_bottom) << std::endl;
            }

            //if(verboseMap)
                //printMapDetailInfo(mapSection);
        }
        else if(dynamic_cast<OsmAnd::ObfPoiSection*>(section) && verbosePoi)
        {
//            printPOIDetailInfo(dynamic_cast<OsmAnd::ObfPoiSection*>(section));
        }
        else if (dynamic_cast<OsmAnd::ObfAddressSection*>(section) && verbosePoi)
        {
            printAddressDetailedInfo(&obfMap, dynamic_cast<OsmAnd::ObfAddressSection*>(section));
        }
    }

    file->close();
}

void printAddressDetailedInfo(OsmAnd::ObfReader* reader, OsmAnd::ObfAddressSection* section)
{
    std::cout << "\tRegion: " << section->_enName << std::endl;
    char* strTypes[] = {
        "City/Towns",
        "Villages",
        "Postcodes",
    };
    /*for(int typeIdx = 0; typeIdx < sizeof(types)/sizeof(types[0]); typeIdx++)
    {
        auto type = types[typeIdx];
        int total = 0;
        std::cout << "\t\t" << strTypes[typeIdx] << ":" << std::endl;*/
        /*for(auto itEntry = section->_entries.begin(); itEntry != section->_entries.end(); ++itEntry)
        {
            auto entry = *itEntry;
            if(entry->_type != type)
                continue;

            total++;
        }*/
        
        //auto cities = OsmAnd::ObfAddressSection::readCities(reader, section, type);
        //auto cities = section->loadCities();//TODO:
    ///}
}
//                for (City c : index.getCities(region, null, type)) {				
//                    index.preloadStreets(c, null);
//                    std::cout << "\t\t" + c + " " + c.getId() + "\t(" + c.getStreets().size() + ")");
//                    for (Street t : c.getStreets()) {
//                        if (verbose.contains(t)) {
//                            index.preloadBuildings(t, null);
//                            print("\t\t\t\t" + t.getName() + getId(t) + "\t(" + t.getBuildings().size() + ")");
//                            // if (type == BinaryMapAddressReaderAdapter.CITY_TOWN_TYPE) {
//                            List<Building> buildings = t.getBuildings();
//                            if (buildings != null && !buildings.isEmpty()) {
//                                print("\t\t (");
//                                for (Building b : buildings) {
//                                    print(b.toString() + ",");
//                                }
//                                print(")");
//                            }
//                            List<Street> streets = t.getIntersectedStreets();
//                            if (streets != null && !streets.isEmpty()) {
//                                print("\n\t\t\t\t\t\t\t\t\t x (");
//                                for (Street s : streets) {
//                                    print(s.getName() + ", ");
//                                }
//                                print(")");
//                            }
//                            // }
//                            std::cout << "");
//                        }
//
//                    }
//                }


std::string formatBounds(int left, int right, int top, int bottom)
{
    double l = OsmAnd::Utilities::get31LongitudeX(left);
    double r = OsmAnd::Utilities::get31LongitudeX(right);
    double t = OsmAnd::Utilities::get31LatitudeY(top);
    double b = OsmAnd::Utilities::get31LatitudeY(bottom);
    return formatGeoBounds(l, r, t, b);
}

std::string formatGeoBounds(double l, double r, double t, double b)
{
    std::ostringstream oStream;
    static std::locale enUS("en-US");
    oStream.imbue(enUS);
    oStream << "(left top - right bottom) : " << l << ", " << t << " NE - " << r << ", " << b << " NE";
    return oStream.str();
}

//
//package net.osmand.binary;
//
//
//import gnu.trove.list.array.TIntArrayList;
//import gnu.trove.map.hash.TIntObjectHashMap;
//
//import java.io.File;
//import java.io.FileOutputStream;
//import java.io.IOException;
//import java.io.RandomAccessFile;
//import java.text.MessageFormat;
//import java.util.ArrayList;
//import java.util.HashMap;
//import java.util.Iterator;
//import java.util.LinkedHashSet;
//import java.util.List;
//import java.util.Locale;
//import java.util.Map;
//import java.util.Set;
//
//import net.osmand.ResultMatcher;
//import net.osmand.binary.BinaryMapAddressReaderAdapter.AddressRegion;
//import net.osmand.binary.BinaryMapIndexReader.MapIndex;
//import net.osmand.binary.BinaryMapIndexReader.MapRoot;
//import net.osmand.binary.BinaryMapIndexReader.SearchFilter;
//import net.osmand.binary.BinaryMapIndexReader.SearchPoiTypeFilter;
//import net.osmand.binary.BinaryMapIndexReader.SearchRequest;
//import net.osmand.binary.BinaryMapIndexReader.TagValuePair;
//import net.osmand.binary.BinaryMapPoiReaderAdapter.PoiRegion;
//import net.osmand.binary.BinaryMapRouteReaderAdapter.RouteRegion;
//import net.osmand.binary.BinaryMapTransportReaderAdapter.TransportIndex;
//import net.osmand.data.Amenity;
//import net.osmand.data.AmenityType;
//import net.osmand.data.Building;
//import net.osmand.data.City;
//import net.osmand.data.MapObject;
//import net.osmand.data.Street;
//import net.osmand.util.MapUtils;
//
//import com.google.protobuf.CodedOutputStream;
//import com.google.protobuf.WireFormat;
//
//public class BinaryInspector {
//
//
//    public static final int BUFFER_SIZE = 1 << 20;
//
//    public static final void writeInt(CodedOutputStream ous, int v) throws IOException {
//        ous.writeRawByte((v >>> 24) & 0xFF);
//        ous.writeRawByte((v >>> 16) & 0xFF);
//        ous.writeRawByte((v >>>  8) & 0xFF);
//        ous.writeRawByte((v >>>  0) & 0xFF);
//        //written += 4;
//    }
//
//    @SuppressWarnings("unchecked")
//        public static List<Float> combineParts(File fileToExtract, Map<File, String> partsToExtractFrom) throws IOException {
//            BinaryMapIndexReader[] indexes = new BinaryMapIndexReader[partsToExtractFrom.size()];
//            RandomAccessFile[] rafs = new RandomAccessFile[partsToExtractFrom.size()];
//
//            LinkedHashSet<Float>[] partsSet = new LinkedHashSet[partsToExtractFrom.size()];
//            int c = 0;
//            Set<String> addressNames = new LinkedHashSet<String>();
//
//
//            int version = -1;
//            // Go through all files and validate conistency 
//            for(File f : partsToExtractFrom.keySet()){
//                if(f.getAbsolutePath().equals(fileToExtract.getAbsolutePath())){
//                    System.err.std::cout << "Error : Input file is equal to output file " + f.getAbsolutePath());
//                    return null;
//                }
//                rafs[c] = new RandomAccessFile(f.getAbsolutePath(), "r");
//                indexes[c] = new BinaryMapIndexReader(rafs[c]);
//                partsSet[c] = new LinkedHashSet<Float>();
//                if(version == -1){
//                    version = indexes[c].getVersion();
//                } else {
//                    if(indexes[c].getVersion() != version){
//                        System.err.std::cout << "Error : Different input files has different input versions " + indexes[c].getVersion() + " != " + version);
//                        return null;
//                    }
//                }
//
//                LinkedHashSet<Float> temp = new LinkedHashSet<Float>();
//                String pattern = partsToExtractFrom.get(f);
//                boolean minus = true;
//                for (int i = 0; i < indexes[c].getIndexes().size(); i++) {
//                    partsSet[c].add(new Float(i + 1f));
//                    BinaryIndexPart part = indexes[c].getIndexes().get(i);
//                    if(part instanceof MapIndex){
//                        List<MapRoot> roots = ((MapIndex) part).getRoots();
//                        int rsize = roots.size(); 
//                        for(int j=0; j<rsize; j++){
//                            partsSet[c].add(new Float((i+1f)+(j+1)/10f));
//                        }
//                    }
//                }
//                if(pattern != null){
//                    minus = pattern.startsWith("-");
//                    String[] split = pattern.substring(1).split(",");
//                    for(String s : split){
//                        temp.add(Float.valueOf(s));
//                    }
//                }
//
//                Iterator<Float> p = partsSet[c].iterator();
//                while (p.hasNext()) {
//                    Float part = p.next();
//                    if (minus) {
//                        if (temp.contains(part)) {
//                            p.remove();
//                        }
//                    } else {
//                        if (!temp.contains(part)) {
//                            p.remove();
//                        }
//                    }
//                }
//
//                c++;
//            }
//
//            // write files 
//            FileOutputStream fout = new FileOutputStream(fileToExtract);
//            CodedOutputStream ous = CodedOutputStream.newInstance(fout, BUFFER_SIZE);
//            List<Float> list = new ArrayList<Float>();
//            byte[] BUFFER_TO_READ = new byte[BUFFER_SIZE];
//
//            ous.writeInt32(OsmandOdb.OsmAndStructure.VERSION_FIELD_NUMBER, version);
//            ous.writeInt64(OsmandOdb.OsmAndStructure.DATECREATED_FIELD_NUMBER, System.currentTimeMillis());
//
//
//            for (int k = 0; k < indexes.length; k++) {
//                LinkedHashSet<Float> partSet = partsSet[k];
//                BinaryMapIndexReader index = indexes[k];
//                RandomAccessFile raf = rafs[k];
//                for (int i = 0; i < index.getIndexes().size(); i++) {
//                    if (!partSet.contains(Float.valueOf(i + 1f))) {
//                        continue;
//                    }
//                    list.add(new Float(i + 1f));
//
//                    BinaryIndexPart part = index.getIndexes().get(i);
//                    String map;
//
//                    if (part instanceof MapIndex) {
//                        ous.writeTag(OsmandOdb.OsmAndStructure.MAPINDEX_FIELD_NUMBER, WireFormat.WIRETYPE_FIXED32_LENGTH_DELIMITED);
//                        map = "Map";
//                    } else if (part instanceof AddressRegion) {
//                        ous.writeTag(OsmandOdb.OsmAndStructure.ADDRESSINDEX_FIELD_NUMBER, WireFormat.WIRETYPE_FIXED32_LENGTH_DELIMITED);
//                        map = "Address";
//                        if (addressNames.contains(part.getName())) {
//                            System.err.std::cout << "Error : going to merge 2 addresses with same names. Skip " + part.getName());
//                            continue;
//                        }
//                        addressNames.add(part.getName());
//                    } else if (part instanceof TransportIndex) {
//                        ous.writeTag(OsmandOdb.OsmAndStructure.TRANSPORTINDEX_FIELD_NUMBER, WireFormat.WIRETYPE_FIXED32_LENGTH_DELIMITED);
//                        map = "Transport";
//                    } else if (part instanceof PoiRegion) {
//                        ous.writeTag(OsmandOdb.OsmAndStructure.POIINDEX_FIELD_NUMBER, WireFormat.WIRETYPE_FIXED32_LENGTH_DELIMITED);
//                        map = "POI";
//                    } else if (part instanceof RouteRegion) {
//                        ous.writeTag(OsmandOdb.OsmAndStructure.ROUTINGINDEX_FIELD_NUMBER, WireFormat.WIRETYPE_FIXED32_LENGTH_DELIMITED);
//                        map = "Routing";
//                    } else {
//                        throw new UnsupportedOperationException();
//                    }
//                    writeInt(ous, part.getLength());
//                    copyBinaryPart(ous, BUFFER_TO_READ, raf, part.getFilePointer(), part.getLength());
//                    std::cout << MessageFormat.format("{2} part {0} is extracted {1} bytes",
//                        new Object[]{part.getName(), part.getLength(), map}));
//
//                }
//            }
//
//            ous.writeInt32(OsmandOdb.OsmAndStructure.VERSIONCONFIRM_FIELD_NUMBER, version);
//            ous.flush();
//            fout.close();
//
//
//            return list;
//    }
//
//
//    private static void copyBinaryPart(CodedOutputStream ous, byte[] BUFFER, RandomAccessFile raf, long fp, int length)
//        throws IOException {
//            raf.seek(fp);
//            int toRead = length;
//            while (toRead > 0) {
//                int read = raf.read(BUFFER);
//                if (read == -1) {
//                    throw new IllegalArgumentException("Unexpected end of file");
//                }
//                if (toRead < read) {
//                    read = toRead;
//                }
//                ous.writeRawBytes(BUFFER, 0, read);
//                toRead -= read;
//            }
//    }
//
//
//    
//
//   
//

//
//    private static void printMapDetailInfo(VerboseInfo verbose, BinaryMapIndexReader index) throws IOException {
//        final StringBuilder b = new StringBuilder();
//        SearchRequest<BinaryMapDataObject> req = BinaryMapIndexReader.buildSearchRequest(MapUtils.get31TileNumberX(verbose.lonleft),
//            MapUtils.get31TileNumberX(verbose.lonright),
//            MapUtils.get31TileNumberY(verbose.lattop),
//            MapUtils.get31TileNumberY(verbose.latbottom), verbose.getZoom(),
//            new SearchFilter() {
//                @Override
//                    public boolean accept(TIntArrayList types, MapIndex index) {
//                        return true;
//                }
//        },
//            new ResultMatcher<BinaryMapDataObject>() {
//                @Override
//                    public boolean publish(BinaryMapDataObject obj) {
//                        b.setLength(0);
//                        boolean multipolygon = obj.getPolygonInnerCoordinates() != null && obj.getPolygonInnerCoordinates().length > 0;
//                        if(multipolygon ) {
//                            b.append("Multipolygon");
//                        } else {
//                            b.append(obj.area? "Area" : (obj.getPointsLength() > 1? "Way" : "Point"));
//                        }
//                        int[] types = obj.getTypes();
//                        b.append(" types [");
//                        for(int j = 0; j<types.length; j++){
//                            if(j > 0) {
//                                b.append(", ");
//                            }
//                            TagValuePair pair = obj.getMapIndex().decodeType(types[j]);
//                            if(pair == null) {
//                                System.err.std::cout << "Type " + types[j] + "was not found");
//                                continue;
//                                //								throw new NullPointerException("Type " + obj.getAdditionalTypes()[j] + "was not found");
//                            }
//                            b.append(pair.toSimpleString()+" ("+types[j]+")");
//                        }
//                        b.append("]");
//                        if(obj.getAdditionalTypes() != null && obj.getAdditionalTypes().length > 0){
//                            b.append(" add_types [");
//                            for(int j = 0; j<obj.getAdditionalTypes().length; j++){
//                                if(j > 0) {
//                                    b.append(", ");
//                                }
//                                TagValuePair pair = obj.getMapIndex().decodeType(obj.getAdditionalTypes()[j]);
//                                if(pair == null) {
//                                    System.err.std::cout << "Type " + obj.getAdditionalTypes()[j] + "was not found");
//                                    continue;
//                                    //									throw new NullPointerException("Type " + obj.getAdditionalTypes()[j] + "was not found");
//                                }
//                                b.append(pair.toSimpleString()+"("+obj.getAdditionalTypes()[j]+")");
//
//                            }
//                            b.append("]");
//                        }
//                        TIntObjectHashMap<String> names = obj.getObjectNames();
//                        if(names != null && !names.isEmpty()) {
//                            b.append(" Names [");
//                            int[] keys = names.keys();
//                            for(int j = 0; j<keys.length; j++){
//                                if(j > 0) {
//                                    b.append(", ");
//                                }
//                                TagValuePair pair = obj.getMapIndex().decodeType(keys[j]);
//                                if(pair == null) {
//                                    throw new NullPointerException("Type " + keys[j] + "was not found");
//                                }
//                                b.append(pair.toSimpleString()+"("+keys[j]+")");
//                                b.append(" - ").append(names.get(keys[j]));
//                            }
//                            b.append("]");
//                        }
//
//                        b.append(" id ").append((obj.getId() >> 1));
//                        b.append(" lat/lon : ");
//                        for(int i=0; i<obj.getPointsLength(); i++) {
//                            float x = (float) MapUtils.get31LongitudeX(obj.getPoint31XTile(i));
//                            float y = (float) MapUtils.get31LatitudeY(obj.getPoint31YTile(i));
//                            b.append(x).append(" / ").append(y).append(" , ");
//                        }
//                        std::cout << b.toString());
//                        return false;
//                }
//                @Override
//                    public boolean isCancelled() {
//                        return false;
//                }
//        });
//        index.searchMapIndex(req);
//    }
//
//    private static void printPOIDetailInfo(VerboseInfo verbose, BinaryMapIndexReader index, PoiRegion p) throws IOException {
//        SearchRequest<Amenity> req = BinaryMapIndexReader.buildSearchPoiRequest(MapUtils.get31TileNumberX(verbose.lonleft),
//            MapUtils.get31TileNumberX(verbose.lonright),
//            MapUtils.get31TileNumberY(verbose.lattop),
//            MapUtils.get31TileNumberY(verbose.latbottom), verbose.getZoom(),
//            new SearchPoiTypeFilter() {
//                @Override
//                    public boolean accept(AmenityType type, String subcategory) {
//                        return true;
//                }
//        },
//            new ResultMatcher<Amenity>() {
//                @Override
//                    public boolean publish(Amenity object) {
//                        std::cout << object.toString() + " " + object.getLocation());
//                        return false;
//                }
//                @Override
//                    public boolean isCancelled() {
//                        return false;
//                }
//        });
//        index.searchPoi(req);
//        std::cout << "Categories : ");
//        for(int i =0; i< p.categories.size(); i++) {
//            std::cout << p.categories.get(i) + " - " + p.subcategories.get(i));	
//        }
//
//
//    }
//
//    private static String getId(MapObject o ){
//        if(o.getId() == null) {
//            return " no id " ;
//        }
//        return " " + (o.getId().longValue() >> 1);
//    }
//
//}
