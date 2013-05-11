#include "applicationdata.h"
ApplicationData::ApplicationData(QObject *parent) :
    QObject(parent)
{
}
QString ApplicationData::calculate(QString command) const {
	// Some Logic comes here
    return command + "+";
}
