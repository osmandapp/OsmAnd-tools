#include <QFile>
#include <QDir>
#include <ctime>
#include <chrono>
#include <sstream>
#include <ostream>
#include "MapActions.h"
#include "ObfReader.h"
#include "RoutePlannerContext.h"
#include "RoutePlanner.h"
#include "MapLayersData.h"
#include "RoutingConfiguration.h"


MapActions::MapActions(MapLayersData* d, QObject *parent) :
    data(d), QObject(parent), app(OsmAnd::OsmAndApplication::getAndInitializeApplication())
{
}

QString MapActions::calculateRoute(){
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
    return QString(os.str().c_str());

}
