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

#include <QFile>
#include <QStringList>

#include <Inspector.h>

void printUsage(const std::string& warning = std::string());

int main(int argc, char* argv[])
{
    std::locale::global(std::locale(""));

    OsmAnd::Inspector::Configuration cfg;

    QString error;
    QStringList args;
    for (int idx = 1; idx < argc; idx++)
        args.push_back(argv[idx]);

    if(!OsmAnd::Inspector::parseCommandLineArguments(args, cfg, error))
    {
        printUsage(error.toStdString());
        return -1;
    }
    OsmAnd::Inspector::dumpToStdOut(cfg);
    return 0;
}

void printUsage(const std::string& warning)
{
    if(!warning.empty())
        std::cout << warning << std::endl;
    std::cout << "Inspector is console utility for working with binary indexes of OsmAnd." << std::endl;
    std::cout << std::endl << "Usage: inspector -obf=path [-vaddress] [-vstreetgroups] [-vstreets] [-vbuildings] [-vintersections] [-vmap] [-vmapObjects] [-vpoi] [-vtransport] [-zoom=Zoom] [-bbox=LeftLon,TopLat,RightLon,BottomLan]" << std::endl;
}

