#include <ctime>
#include <chrono>
#include <sstream>
#include <ostream>


#include <QFile>
#include <QDir>


#include <SkDevice.h>

#include <Utilities.h>
#include <ObfReader.h>
#include <Rasterizer.h>
#include <RasterizerContext.h>
#include <RasterizationStyles.h>
#include <RasterizationStyle.h>
#include <RasterizationStyleEvaluator.h>
#include <RoutePlannerContext.h>
#include <RoutePlanner.h>
#include <RoutingConfiguration.h>

#include "MapActions.h"
#include "MapLayersData.h"




MapActions::MapActions(MapLayersData* d, QObject *parent) :
    data(d), QObject(parent), app(OsmAnd::OsmAndApplication::getAndInitializeApplication())
{
}

bool MapActions::isActivityRunning() {
    if(pseudoCounter > threadPool.activeThreadCount()) {
        pseudoCounter = threadPool.activeThreadCount();
    }
    return pseudoCounter > 0  && threadPool.activeThreadCount() > 0;
}

class RunRouteRasterization : public QRunnable
{
    std::shared_ptr<OsmAnd::OsmAndApplication> app;
    OsmAnd::AreaI bbox;
    uint32_t zoom;
    MapLayersData* data;
    MapActions* actions;
public:
    RunRouteRasterization(std::shared_ptr<OsmAnd::OsmAndApplication> app, OsmAnd::AreaI bbox, uint32_t zoom,
                          MapLayersData* d, MapActions* a) :
        app(app), bbox(bbox), zoom(zoom), data(d),actions(a) {

    }
    
    void run()
    {
        bool is32bit = true;
        float tileSide = 256;

        OsmAnd::RasterizationStyles stylesCollection;
        std::shared_ptr<OsmAnd::RasterizationStyle> style;
        stylesCollection.obtainStyle("default", style);

        QList< std::shared_ptr<OsmAnd::ObfReader> > obfData;
        QString d = app->getSettings()->APPLICATION_DIRECTORY.get().toString();
        if(d != "") {
            QDir dir(d);
            QStringList files = dir.entryList();
            for(QString it : files) {
                if(it.endsWith(".obf")) {
                    std::shared_ptr<QFile> qf(new QFile(dir.absolutePath() + "/" + it));
                    std::shared_ptr<OsmAnd::ObfReader> obfReader(new OsmAnd::ObfReader(qf));
                    obfData.push_back(obfReader);
                }
            }
        }

        // Collect all map objects (this should be replaced by something like RasterizerViewport/RasterizerContext)
        QList< std::shared_ptr<OsmAnd::Model::MapObject> > mapObjects;
        OsmAnd::QueryFilter filter;
        filter._bbox31 = &bbox;
        filter._zoom = &zoom;
        for(auto itObf = obfData.begin(); itObf != obfData.end(); ++itObf)
        {
            auto obf = *itObf;

            for(auto itMapSection = obf->mapSections.begin(); itMapSection != obf->mapSections.end(); ++itMapSection)
            {
                auto mapSection = *itMapSection;

                OsmAnd::ObfMapSection::loadMapObjects(obf.get(), mapSection.get(), &mapObjects, &filter, nullptr);
            }
        }

        // Allocate render target
        SkBitmap renderSurface;
        const auto pixelWidth = (bbox.right >> (31 - zoom + 8)) - (bbox.left >> (31 - zoom + 8));
        const auto pixelHeight = (bbox.top >> (31 - zoom + 8)) - (bbox.bottom >> (31 - zoom + 8));
        renderSurface.setConfig(is32bit ? SkBitmap::kARGB_8888_Config : SkBitmap::kRGB_565_Config, pixelWidth, pixelHeight);
        if(!renderSurface.allocPixels())
        {
            return;
        }
        SkDevice renderTarget(renderSurface);
        // Create render canvas
        SkCanvas canvas(&renderTarget);

        // Perform actual rendering
        OsmAnd::RasterizerContext rasterizerContext(style);
        OsmAnd::AreaD dbox;
        dbox.left = OsmAnd::Utilities::get31LongitudeX(bbox.left);
        dbox.right = OsmAnd::Utilities::get31LongitudeX(bbox.right);
        dbox.top = OsmAnd::Utilities::get31LatitudeY(bbox.top);
        dbox.bottom = OsmAnd::Utilities::get31LatitudeY(bbox.bottom);
        //OsmAnd::Rasterizer::rasterize(rasterizerContext, true, canvas, dbox, zoom, tileSide, mapObjects, OsmAnd::PointI(), nullptr);
        data->setRenderedImage(renderSurface, bbox);
        actions->taskFinished();
        emit data->mapNeedsToRefresh(QString(""));
    }
};

class RunRouteCalculation : public QRunnable
{
public:
    MapLayersData* data;
    MapActions* a;
    RunRouteCalculation(MapLayersData* d, MapActions* a) : data(d), a(a) {
    }

    void run() {
        QString s = runMsg();
        emit data->mapNeedsToRefresh(s);
    }

    QString runMsg()
    {
        auto app = OsmAnd::OsmAndApplication::getAndInitializeApplication();
        QList< std::shared_ptr<OsmAnd::ObfReader> > obfData;
        QString d = app->getSettings()->APPLICATION_DIRECTORY.get().toString();
        float slat = app->getSettings()->START_LATITUDE.get().toFloat();
        float slon = app->getSettings()->START_LONGITUDE.get().toFloat();
        float tlat = app->getSettings()->TARGET_LATITUDE.get().toFloat();
        float tlon = app->getSettings()->TARGET_LONGITUDE.get().toFloat();
        if(d != "") {
            QDir dir(d);
            QStringList files = dir.entryList();
            for(QString it : files) {
                if(it.endsWith(".obf")) {
                    std::shared_ptr<QFile> qf(new QFile(dir.absolutePath() + "/" + it));
                    std::shared_ptr<OsmAnd::ObfReader> obfReader(new OsmAnd::ObfReader(qf));
                    obfData.push_back(obfReader);
                }
            }
        }
        std::shared_ptr<OsmAnd::RoutingConfiguration> routingConfig(new OsmAnd::RoutingConfiguration);
        OsmAnd::RoutingConfiguration::loadDefault(*routingConfig);
        OsmAnd::RoutePlannerContext plannerContext(obfData, routingConfig, QString("car"), false);
        std::shared_ptr<OsmAnd::Model::Road> startRoad;
        if(!OsmAnd::RoutePlanner::findClosestRoadPoint(&plannerContext, slat, slon, &startRoad))
        {
            return "Failed to find road near start point";
        }
        std::shared_ptr<OsmAnd::Model::Road> endRoad;
        if(!OsmAnd::RoutePlanner::findClosestRoadPoint(&plannerContext, tlat, tlon, &endRoad))
        {
            return "Failed to find road near end point";
        }
        QList< std::pair<double, double> > points;
        points.push_back(std::pair<double, double>(slat, slon));
        points.push_back(std::pair<double, double>(tlat, tlon));

        QList< std::shared_ptr<OsmAnd::RouteSegment> > route;
        auto routeCalculationStart = std::chrono::steady_clock::now();
        bool routeFound = OsmAnd::RoutePlanner::calculateRoute(&plannerContext, points, false, nullptr, &route);
        auto routeCalculationFinish = std::chrono::steady_clock::now();

        if(!routeFound) {
            return "Route is not found" ;
        }
        std::stringstream os;
        os << "Route in " << std::chrono::duration<double, std::milli> (routeCalculationFinish - routeCalculationStart).count() << " ms ";
        os << route.length() << " segments ";
        data->setRoute(route);
        a->taskFinished();
        return QString(os.str().c_str());
    }
};

void MapActions::calculateRoute(){
    start(new RunRouteCalculation(data, this));
}

void MapActions::runRasterization(OsmAnd::AreaI bbox, uint32_t zoom){
    if(!isActivityRunning()) {
        start(new RunRouteRasterization(app, bbox, zoom, data, this));
    }
}
