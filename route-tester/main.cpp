#include <string>
#include <iostream>
#include <sstream>
#include <map>
#include <memory>
#if (defined(UNICODE) || defined(_UNICODE)) && defined(_WIN32)
#   include <io.h>
#   include <fcntl.h>
#endif

#include <OsmAndCore/QtExtensions.h>
#include <QFile>
#include <QStringList>

#include <OsmAndCoreUtils/Voyager.h>

void printUsage(std::string warning = std::string());

int main(int argc, char* argv[])
{
#if defined(UNICODE) || defined(_UNICODE)
#   if defined(_WIN32)
    _setmode(_fileno(stdout), _O_U16TEXT);
#   else
    std::locale::global(std::locale(""));
#   endif
#endif

    OsmAnd::Voyager::Configuration cfg;

    QString error;
    QStringList args;
    for (int idx = 1; idx < argc; idx++)
        args.push_back(argv[idx]);

    if(!OsmAnd::Voyager::parseCommandLineArguments(args, cfg, error))
    {
        printUsage(error.toStdString());
        return -1;
    }
    OsmAnd::Voyager::logJourneyToStdOut(cfg);
    return 0;
}

void printUsage(std::string warning)
{
    if(!warning.empty())
        std::cout << warning << std::endl;
    std::cout << "Voyager is console utility to test OsmAnd routing engine." << std::endl;
    std::cout << std::endl << "Usage: voyager [-config=path/to/config.xml] [-verbose] [-obfsDir=path/to/OBFs] [-vehicle=car] [-memlimit=0] [-start=lat;lon] [-waypoint=lat;lon] [-end=lat;lon] [-left] [-gpx=path/to/file]" << std::endl;
    std::cout << "\tconfig - Routing configuration file. If not specified, default configuration will be used" << std::endl;
    std::cout << "\tverbose - Be verbose?" << std::endl;
    std::cout << "\tobfsDir - Root directory of OBF files" << std::endl;
    std::cout << "\tstart - Route start point" << std::endl;
    std::cout << "\tend - Route end point" << std::endl;
    std::cout << "\tleft - Use left-side navigation" << std::endl;
}
