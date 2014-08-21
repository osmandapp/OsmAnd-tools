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

#include <OsmAndCoreTools/EyePiece.h>

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

    OsmAnd::EyePiece::Configuration cfg;

    QString error;
    QStringList args;
    for (int idx = 1; idx < argc; idx++)
        args.push_back(argv[idx]);

    if(!OsmAnd::EyePiece::parseCommandLineArguments(args, cfg, error))
    {
        printUsage(error.toStdString());
        return -1;
    }
    OsmAnd::EyePiece::rasterizeToStdOut(cfg);
    return 0;
}

void printUsage(std::string warning)
{
    if(!warning.empty())
        std::cout << warning << std::endl;
    std::cout << "EyePiece is console utility to rasterize OsmAnd map tile." << std::endl;
    std::cout << std::endl << "Usage: eyepiece -stylesPath=path/to/styles1";
    std::cout << " [-stylesPath=path/to/styles2]";
    std::cout << " -style=style";
    std::cout << " [-obfsDir=path/to/obf/collection]";
    std::cout << " [-verbose]";
    std::cout << " [-dumpRules]";
    std::cout << " [-zoom=15]";
    std::cout << " [-32bit]";
    std::cout << " [-tileSide=256]";
    std::cout << " [-density=1.0]";
    std::cout << " [-bbox=LeftLon,TopLat,RightLon,BottomLan]";
    std::cout << " [-output=path/to/image.png]";
    std::cout << " [-map]";
    std::cout << " [-text]";
    std::cout << " [-icons]";
    std::cout << std::endl;
}

