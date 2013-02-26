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

void printUsage(std::string warning = std::string());

int main(int argc, char* argv[])
{
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

void printUsage(std::string warning)
{
    if(!warning.empty())
        std::cout << warning << std::endl;
    std::cout << "Inspector is console utility for working with binary indexes of OsmAnd." << std::endl;
    std::cout << "It allows print info about file, extract parts and merge indexes." << std::endl;
    std::cout << "\nUsage for print info : inspector [-vaddress] [-vstreetgroups] [-vstreets] [-vbuildings] [-vintersections] [-vmap] [-vpoi] [] [-vtransport] [-zoom=Zoom] [-bbox=LeftLon,TopLat,RightLon,BottomLan] [file]" << std::endl;
    std::cout << "  Prints information about [file] binary index of OsmAnd." << std::endl;
    std::cout << "  -v.. more verbose output (like all cities and their streets or all map objects with tags/values and coordinates)" << std::endl;
    std::cout << "\nUsage for combining indexes : inspector -c file_to_create (file_from_extract ((+|-)parts_to_extract)? )*" << std::endl;
    std::cout << "\tCreate new file of extracted parts from input file. [parts_to_extract] could be parts to include or exclude." << std::endl;
    std::cout << "  Example : inspector -c output_file input_file +1,2,3\n\tExtracts 1, 2, 3 parts (could be find in print info)" << std::endl;
    std::cout << "  Example : inspector -c output_file input_file -2,3\n\tExtracts all parts excluding 2, 3" << std::endl;
    std::cout << "  Example : inspector -c output_file input_file1 input_file2 input_file3\n\tSimply combine 3 files" << std::endl;
    std::cout << "  Example : inspector -c output_file input_file1 input_file2 -4\n\tCombine all parts of 1st file and all parts excluding 4th part of 2nd file" << std::endl;
}

