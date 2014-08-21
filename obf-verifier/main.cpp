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

#include <OsmAndCoreTools/Verifier.h>

void printUsage(const std::string& warning = std::string());

int main(int argc, char* argv[])
{
#if defined(UNICODE) || defined(_UNICODE)
#   if defined(_WIN32)
    _setmode(_fileno(stdout), _O_U16TEXT);
#   else
    std::locale::global(std::locale(""));
#   endif
#endif
    OsmAnd::Verifier::Configuration cfg;

    QString error;
    QStringList args;
    for (int idx = 1; idx < argc; idx++)
        args.push_back(argv[idx]);

    if(!OsmAnd::Verifier::parseCommandLineArguments(args, cfg, error))
    {
        printUsage(error.toStdString());
        return -1;
    }
    OsmAnd::Verifier::dumpToStdOut(cfg);
    return 0;
}

void printUsage(const std::string& warning)
{
    if(!warning.empty())
        std::cout << warning << std::endl;
    std::cout << "Verifier is console utility for working with OsmAnd OBF files." << std::endl;
    std::cout << std::endl
        << "Usage: inspector "
        << "-obfsDir=path "
        << "[-obfsDir=path] "
        << "-obf=path "
        << "[-obf=path] "
        << "-uniqueMapObjectIds"
        << std::endl;
}

