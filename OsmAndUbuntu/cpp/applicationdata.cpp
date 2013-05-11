#include "applicationdata.h"
#include <QDir>
#include <QString>
#include <iostream>

ApplicationData::ApplicationData(QObject *parent) :
    QObject(parent), osmandDirectory("")
{
}
QString ApplicationData::setOsmandDirectiory(QString directory) {
    osmandDirectory = directory;
    this->files.clear();
    QDir dir(osmandDirectory);
    QStringList files = dir.entryList();
    for(QString it : files) {
        if(it.endsWith(".obf")) {
            this->files.append(it);
        }
    }
    return directory;
}
