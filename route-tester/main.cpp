/**
* @file
*
* @section LICENSE
*
* OsmAnd - Android navigation software based on OSM maps.
* Copyright (C) 2010-2013  OsmAnd Authors listed in AUTHORS file
*
* This program is free software: you can redistribute it and/or modify
* it under the terms of the GNU General Public License as published by
* the Free Software Foundation, either version 3 of the License, or
* (at your option) any later version.
*
* This program is distributed in the hope that it will be useful,
* but WITHOUT ANY WARRANTY; without even the implied warranty of
* MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
* GNU General Public License for more details.

* You should have received a copy of the GNU General Public License
* along with this program.  If not, see <http://www.gnu.org/licenses/>.
*/

#include <string>
#include <iostream>
#include <sstream>
#include <map>
#include <memory>
#if (defined(UNICODE) || defined(_UNICODE)) && defined(_WIN32)
#   include <io.h>
#   include <fcntl.h>
#endif

#include <QFile>
#include <QStringList>

#include <Voyager.h>

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
    std::cout << std::endl << "Usage: voyager [-config=path/to/config.xml] [-verbose] [-obfsDir=path/to/OBFs] [-vehicle=car] [-memlimit=0] [-start=lat;lon] [-waypoint=lat;lon] [-end=lat;lon] [-left]" << std::endl;
    std::cout << "\tconfig - Routing configuration file. If not specified, default configuration will be used" << std::endl;
    std::cout << "\tverbose - Be verbose?" << std::endl;
    std::cout << "\tobfsDir - Root folder of OBF files" << std::endl;
    std::cout << "\tstart - Route start point" << std::endl;
    std::cout << "\tend - Route end point" << std::endl;
    std::cout << "\tleft - Use left-side navigation" << std::endl;
}

