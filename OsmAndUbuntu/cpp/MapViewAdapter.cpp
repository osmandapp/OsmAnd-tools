#include "MapViewAdapter.h"

MapViewAdapter::MapViewAdapter(QObject *) : app(OsmAnd::OsmAndApplication::getAndInitializeApplication())
{

    mapView = std::shared_ptr<OsmAnd::OsmAndMapView>(new OsmAnd::OsmAndMapView(app));
}
