#include "applicationdata.h"
#include <QDir>
#include <QString>
#include <iostream>

ApplicationData::ApplicationData(QObject *parent) :
    QObject(parent)
{
}

QString ApplicationData::getOsmandDirectiory() {
    auto app = OsmAnd::OsmAndApplication::getAndInitializeApplication();
    return app->getSettings()->APPLICATION_DIRECTORY.get().toString();
}

void ApplicationData::setOsmandDirectiory(QString directory) {
    auto app = OsmAnd::OsmAndApplication::getAndInitializeApplication();
    app->getSettings()->APPLICATION_DIRECTORY.set(directory);
    this->files.clear();
    QDir dir(directory);
    QStringList files = dir.entryList();
    for(QString it : files) {
        if(it.endsWith(".obf")) {
            this->files.append(it);
        }
    }
}
