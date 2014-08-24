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

#if defined(UNICODE) || defined(_UNICODE)
void printUsage(const std::wstring& warning = std::wstring());
#else
void printUsage(const std::string& warning = std::string());
#endif

int main(int argc, char* argv[])
{
#if defined(UNICODE) || defined(_UNICODE)
#   if defined(_WIN32)
    _setmode(_fileno(stdout), _O_U16TEXT);
#   else
    std::locale::global(std::locale(""));
#   endif
#endif

    // Parse configuration
    OsmAndTools::EyePiece::Configuration configuration;
    QString error;
    QStringList commandLineArgs;
    for (int idx = 1; idx < argc; idx++)
        commandLineArgs.push_back(argv[idx]);
    if (!OsmAndTools::EyePiece::Configuration::parseFromCommandLineArguments(commandLineArgs, configuration, error))
    {
        printUsage(QStringToStlString(error));
        return -1;
    }

    // Process rasterization request
    const auto success = OsmAndTools::EyePiece(configuration).rasterize();
    return (success ? 0 : -1);
}

#if defined(UNICODE) || defined(_UNICODE)
void printUsage(const std::wstring& warning /*= std::wstring()*/)
#else
void printUsage(const std::string& warning /*= std::string()*/)
#endif
{
#if defined(UNICODE) || defined(_UNICODE)
    auto& tout = std::wcout;
#else
    auto& tout = std::cout;
#endif
    if(!warning.empty())
        tout << warning << std::endl;
    tout << xT("OsmAnd EyePiece tool is a console utility to rasterize part of map.") << std::endl;
    tout << std::endl;
    tout << xT("Arguments:") << std::endl;
    //std::cout << "\t-stylesPath=path/to/main/styles";
    //std::cout << "\t[-stylesPath=path/to/extra/styles]";
    //std::cout << "\t-style=styleName";
    //std::cout << "\t[-obfsPath=path/to/OBFs/collection]";
    //std::cout << "\t[-obfFile=OBF/file/path/with/name]";
    /*std::cout << "\t[-verbose]";
    std::cout << "\t[-dumpRules]";
    std::cout << "\t[-zoom=15]";
    std::cout << "\t[-32bit]";
    std::cout << "\t[-tileSide=256]";
    std::cout << "\t[-density=1.0]";
    std::cout << "\t[-bbox=LeftLon,TopLat,RightLon,BottomLan]";
    std::cout << "\t[-output=path/to/image.png]";
    std::cout << "\t[-map]";
    std::cout << "\t[-text]";
    std::cout << "\t[-icons]";*/
}

