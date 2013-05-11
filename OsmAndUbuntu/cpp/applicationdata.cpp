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
    std::cout << "SIZE : " << files.size() << std::endl;
    for(QString it : files) {
        if(it.endsWith(".obf")) {
            std::cout << it.toStdString() << std::endl;
            this->files.append(it);
        }
    }
    std::cout.flush();
    return directory;
}
