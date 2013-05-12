#include "MainApplicationSettings.h"
#include <QDir>
#include <QString>
#include <iostream>
#include <strstream>
#include <sstream>
#include <OsmAndCore.h>
#include <ObfReader.h>
#include <Utilities.h>

MainApplicationSettings::MainApplicationSettings(QObject *parent) :
    QObject(parent)
{
    app = OsmAnd::OsmAndApplication::getAndInitializeApplication();
    reloadFiles();
}


QString MainApplicationSettings::getOsmandDirectiory() {

    return app->getSettings()->APPLICATION_DIRECTORY.get().toString();
}
std::string formatGeoBounds(double l, double r, double t, double b)
{
    std::ostringstream oStream;
    oStream << "(left top - right bottom) : " << l << ", " << t << " NE - " << r << ", " << b << " NE";
    return oStream.str();
}

std::string formatSize(uint32_t sz)
{
    std::ostringstream oStream;
    if(sz >= 1024 * 1024) {
        oStream << sz / 1024. / 1024.f << " MB";
    } else {
        oStream << sz / 1024. << " KB";
    }
    return oStream.str();
}


std::string formatBounds(uint32_t left, uint32_t right, uint32_t top, uint32_t bottom)
{
    double l = OsmAnd::Utilities::get31LongitudeX(left);
    double r = OsmAnd::Utilities::get31LongitudeX(right);
    double t = OsmAnd::Utilities::get31LatitudeY(top);
    double b = OsmAnd::Utilities::get31LatitudeY(bottom);
    return formatGeoBounds(l, r, t, b);
}



void dump(std::ostream &output, const QString& filePath)
{
    std::shared_ptr<QFile> file(new QFile(filePath));
    if(!file->exists())
    {
        output << "Binary OsmAnd index " << qPrintable(filePath) << " was not found." << std::endl;
        return;
    }

    if(!file->open(QIODevice::ReadOnly))
    {
        output << "Failed to open file " << qPrintable(file->fileName()) << std::endl;
        return;
    }

    OsmAnd::ObfReader obfMap(file);
    output << "Binary index " << qPrintable(file->fileName().split('/').last()) << " version = " << obfMap.version << std::endl;
    int idx = 1;
    for(auto itSection = obfMap.sections.begin(); itSection != obfMap.sections.end(); ++itSection, idx++)
    {
        OsmAnd::ObfSection* section = *itSection;

        std::string sectionType = "unknown";
        if(dynamic_cast<OsmAnd::ObfMapSection*>(section))
            sectionType = "Map";
        else if(dynamic_cast<OsmAnd::ObfTransportSection*>(section))
            sectionType = "Transport";
        else if(dynamic_cast<OsmAnd::ObfRoutingSection*>(section))
            sectionType = "Routing";
        else if(dynamic_cast<OsmAnd::ObfPoiSection*>(section))
            sectionType = "Poi";
        else if(dynamic_cast<OsmAnd::ObfAddressSection*>(section))
            sectionType = "Address";

        output << idx << ". " << sectionType << " data " << section->_name.toStdString() << " - " << formatSize( section->_length ) << std::endl;

        if(dynamic_cast<OsmAnd::ObfTransportSection*>(section))
        {
            auto transportSection = dynamic_cast<OsmAnd::ObfTransportSection*>(section);
            int sh = (31 - OsmAnd::ObfTransportSection::StopZoom);
            output << "    Bounds " << formatBounds(transportSection->_left << sh, transportSection->_right << sh, transportSection->_top << sh, transportSection->_bottom << sh) << std::endl;
        }
        else if(dynamic_cast<OsmAnd::ObfRoutingSection*>(section))
        {
            auto routingSection = dynamic_cast<OsmAnd::ObfRoutingSection*>(section);
            double lonLeft = 180;
            double lonRight = -180;
            double latTop = -90;
            double latBottom = 90;
            for(auto itSubsection = routingSection->_subsections.begin(); itSubsection != routingSection->_subsections.end(); ++itSubsection)
            {
                auto subsection = itSubsection->get();

                lonLeft = std::min(lonLeft, OsmAnd::Utilities::get31LongitudeX(subsection->area31.left));
                lonRight = std::max(lonRight, OsmAnd::Utilities::get31LongitudeX(subsection->area31.right));
                latTop = std::max(latTop, OsmAnd::Utilities::get31LatitudeY(subsection->area31.top));
                latBottom = std::min(latBottom, OsmAnd::Utilities::get31LatitudeY(subsection->area31.bottom));
            }
            output << "    Bounds " << formatGeoBounds(lonLeft, lonRight, latTop, latBottom) << std::endl;
        }
        else if(dynamic_cast<OsmAnd::ObfMapSection*>(section))
        {
            auto mapSection = dynamic_cast<OsmAnd::ObfMapSection*>(section);
            int levelIdx = 1;
            for(auto itLevel = mapSection->_levels.begin(); itLevel != mapSection->_levels.end(); ++itLevel, levelIdx++)
            {
                auto level = itLevel->get();
                output << "  " << idx << "." << levelIdx << " Map level minZoom = " << level->_minZoom << ", maxZoom = " << level->_maxZoom << ", size = ";
                output << formatSize(level->_length );
                output << std::endl;
                output << "      Bounds " << formatBounds(level->_area31.left, level->_area31.right, level->_area31.top, level->_area31.bottom) << std::endl;
            }

            //            if(cfg.verboseMap)
            //                printMapDetailInfo(output, cfg, &obfMap, mapSection);
        }
        //        else if(dynamic_cast<OsmAnd::ObfPoiSection*>(section) && cfg.verbosePoi)
        //        {
        // printPOIDetailInfo(output, cfg, &obfMap, dynamic_cast<OsmAnd::ObfPoiSection*>(section));
        //        }
        //        else if (dynamic_cast<OsmAnd::ObfAddressSection*>(section) &&cfg. verboseAddress)
        //        {
        // printAddressDetailedInfo(output, cfg, &obfMap, dynamic_cast<OsmAnd::ObfAddressSection*>(section));
        //        }
    }

    file->close();
}


QString MainApplicationSettings::describeFile(int index)
{
    std::ostringstream s;
    dump(s, app->getSettings()->APPLICATION_DIRECTORY.get().toString() + "/"+getFiles()[index]);
    return QString(s.str().c_str());
}

void MainApplicationSettings::setOsmandDirectiory(QString directory) {
    app->getSettings()->APPLICATION_DIRECTORY.set(directory);
    reloadFiles();
}

void MainApplicationSettings::reloadFiles(){
    this->files.clear();
    QString d = app->getSettings()->APPLICATION_DIRECTORY.get().toString();
    if(d != "") {
        QDir dir(d);
        QStringList files = dir.entryList();
        for(QString it : files) {
            if(it.endsWith(".obf")) {
                this->files.append(it);
            }
        }
    }
}
