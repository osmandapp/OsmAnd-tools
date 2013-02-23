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

    if(argc <= 1)
    {
        printUsage();
        return -1;
    }

    std::string cmd = argv[1];
    if (cmd[0] == '-')
    {
        // command
        if (cmd == "-c" || cmd == "-combine") {
            if (argc < 5)
            {
                printUsage("Too few parameters to extract (require minimum 4)");
                return -1;
            }

            std::map<std::shared_ptr<QFile>, std::string> parts;
            /*for (int i = 3; i < argc; i++)
            {
                file = new File(args[i]);
                if (!file.exists()) {
                    System.err.std::cout << "File to extract from doesn't exist " + args[i]);
                    return;
                }
                parts.put(file, null);
                if (i < args.length - 1) {
                    if (args[i + 1].startsWith("-") || args[i + 1].startsWith("+")) {
                        parts.put(file, args[i + 1]);
                        i++;
                    }
                }
            }
            List<Float> extracted = combineParts(new File(args[1]), parts);
            if (extracted != null) {
                std::cout << "\n" + extracted.size() + " parts were successfully extracted to " + args[1]);
            }*/
        }
        else if (cmd.find("-v") == 0)
        {
            if (argc < 3)
            {
                printUsage("Missing file parameter");
                return -1;
            }

            for(int argIdx = 1; argIdx < argc - 1; argIdx++)
            {
                std::string arg = argv[argIdx];
                if(arg == "-vaddress")
                    cfg.verboseAddress = true;
                else if(arg == "-vstreets")
                    cfg.verboseStreets = true;
                else if(arg == "-vstreetgroups")
                    cfg.verboseStreetGroups = true;
                else if(arg == "-vbuildings")
                    cfg.verboseBuildings = true;
                else if(arg == "-vintersections")
                    cfg.verboseIntersections = true;
                else if(arg == "-vmap")
                    cfg.verboseMap = true;
                else if(arg == "-vpoi")
                    cfg.verbosePoi = true;
                else if(arg == "-vtransport")
                    cfg.verboseTrasport = true;
                else if(arg.find("-zoom=") == 0)
                    cfg.zoom = atoi(arg.c_str() + 5);
                else if(arg.find("-bbox=") == 0)
                {
                    auto values = QString(arg.c_str() + 5).split(",");
                    cfg.lonLeft = values[0].toDouble();
                    cfg.latTop = values[1].toDouble();
                    cfg.lonRight = values[2].toDouble();
                    cfg.latBottom =  values[3].toDouble();
                }
            }

            OsmAnd::Inspector::dumpToStdOut(QString::fromStdString(argv[argc - 1]), cfg);
        } else {
            printUsage("Unknown command : " + cmd);
        }
    }
    else
    {
        OsmAnd::Inspector::dumpToStdOut(QString::fromStdString(cmd), cfg);
    }
    return 0;
}

void printUsage(std::string warning)
{
    if(!warning.empty())
        std::cout << warning << std::endl;
    std::cout << "Inspector is console utility for working with binary indexes of OsmAnd." << std::endl;
    std::cout << "It allows print info about file, extract parts and merge indexes." << std::endl;
    std::cout << "\nUsage for print info : inspector [-vaddress] [-vstreetgroups] [-vstreets] [-vbuildings] [-vintersections] [-vmap] [-vpoi] [-vtransport] [-zoom=Zoom] [-bbox=LeftLon,TopLat,RightLon,BottomLan] [file]" << std::endl;
    std::cout << "  Prints information about [file] binary index of OsmAnd." << std::endl;
    std::cout << "  -v.. more verbose output (like all cities and their streets or all map objects with tags/values and coordinates)" << std::endl;
    std::cout << "\nUsage for combining indexes : inspector -c file_to_create (file_from_extract ((+|-)parts_to_extract)? )*" << std::endl;
    std::cout << "\tCreate new file of extracted parts from input file. [parts_to_extract] could be parts to include or exclude." << std::endl;
    std::cout << "  Example : inspector -c output_file input_file +1,2,3\n\tExtracts 1, 2, 3 parts (could be find in print info)" << std::endl;
    std::cout << "  Example : inspector -c output_file input_file -2,3\n\tExtracts all parts excluding 2, 3" << std::endl;
    std::cout << "  Example : inspector -c output_file input_file1 input_file2 input_file3\n\tSimply combine 3 files" << std::endl;
    std::cout << "  Example : inspector -c output_file input_file1 input_file2 -4\n\tCombine all parts of 1st file and all parts excluding 4th part of 2nd file" << std::endl;
}

