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
#include <QStringList>
#include <QFile>

#include <OsmAndCoreTools/Inspector.h>

void printUsage(const QString& warning = QString::null);

int main(int argc, char* argv[])
{
    /*
    OsmAnd::InitializeCore();
#if defined(UNICODE) || defined(_UNICODE)
#   if defined(_WIN32)
    _setmode(_fileno(stdout), _O_U16TEXT);
#   else
    std::locale::global(std::locale(""));
#   endif
#endif
    OsmAnd::Inspector::Configuration cfg;

    QString error;
    QStringList args;
    for (int idx = 1; idx < argc; idx++)
        args.push_back(argv[idx]);

    if(!OsmAnd::Inspector::parseCommandLineArguments(args, cfg, error))
    {
        printUsage(error);

        OsmAnd::ReleaseCore();
        return -1;
    }
    OsmAnd::Inspector::dumpToStdOut(cfg);

    OsmAnd::ReleaseCore();
    return 0;
    */
}

void printUsage(const QString& warning)
{
#if defined(UNICODE) || defined(_UNICODE)
    if(!warning.isEmpty())
        std::wcout << warning.toStdWString() << std::endl;
    std::wcout << "Inspector is console utility for working with binary indexes of OsmAnd." << std::endl;
    std::wcout << std::endl
        << "Usage: inspector "
        << "-obf=path "
        << "[-vaddress] "
        << "[-vstreetgroups] "
        << "[-vstreets] "
        << "[-vbuildings] "
        << "[-vintersections] "
        << "[-vmap] "
        << "[-vmapObjects] "
        << "[-vpoi] "
        << "[-vtransport] "
        << "[-zoom=Zoom] "
        << "[-bbox=LeftLon,TopLat,RightLon,BottomLan]" << std::endl;
#else
    if(!warning.isEmpty())
        std::cout << warning.toStdString() << std::endl;
    std::cout << "Inspector is console utility for working with binary indexes of OsmAnd." << std::endl;
    std::cout << std::endl
        << "Usage: inspector "
        << "-obf=path "
        << "[-vaddress] "
        << "[-vstreetgroups] "
        << "[-vstreets] "
        << "[-vbuildings] "
        << "[-vintersections] "
        << "[-vmap] "
        << "[-vmapObjects] "
        << "[-vpoi] "
        << "[-vtransport] "
        << "[-zoom=Zoom] "
        << "[-bbox=LeftLon,TopLat,RightLon,BottomLan]" << std::endl;
#endif
}

