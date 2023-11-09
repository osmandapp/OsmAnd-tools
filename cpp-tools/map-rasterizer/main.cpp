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
#include <OsmAndCore/CoreResourcesEmbeddedBundle.h>

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

    // Initialize OsmAnd core
    std::shared_ptr<const OsmAnd::CoreResourcesEmbeddedBundle> coreResourcesEmbeddedBundle;
#if defined(OSMAND_CORE_STATIC)
    coreResourcesEmbeddedBundle = OsmAnd::CoreResourcesEmbeddedBundle::loadFromCurrentExecutable();
#else
    coreResourcesEmbeddedBundle = OsmAnd::CoreResourcesEmbeddedBundle::loadFromSharedResourcesBundle();
#endif // defined(OSMAND_CORE_STATIC)
    OsmAnd::InitializeCore(coreResourcesEmbeddedBundle);

    // Parse configuration
    OsmAndTools::EyePiece::Configuration configuration;
    QString error;
    QStringList commandLineArgs;
    for (int idx = 1; idx < argc; idx++)
        commandLineArgs.push_back(argv[idx]);
    if (!OsmAndTools::EyePiece::Configuration::parseFromCommandLineArguments(commandLineArgs, configuration, error))
    {
        printUsage(QStringToStlString(error));
        OsmAnd::ReleaseCore();
        return -1;
    }

    // Process rasterization request
    const auto success = OsmAndTools::EyePiece(configuration).rasterize();

    OsmAnd::ReleaseCore();

    return (success ? 0 : -1);
}

#if defined(UNICODE) || defined(_UNICODE)
void printUsage(const std::wstring& warning /*= std::wstring()*/)
#else
void printUsage(const std::string& warning /*= std::string()*/)
#endif
{
#if defined(UNICODE) || defined(_UNICODE)
    auto& tcout = std::wcout;
#else
    auto& tcout = std::cout;
#endif
    if(!warning.empty())
        tcout << warning << std::endl;
    tcout << xT("OsmAnd EyePiece tool is a console utility to rasterize part of map.") << std::endl;
    tcout << std::endl;
    tcout << xT("Arguments:") << std::endl;
    tcout << xT("\t-obfsPath=path/to/OBFs/collection or -obfsRecursivePath=path/to/OBFs/collection or -obfFile=OBF/file/path/with/name.obf ...") << std::endl;
    tcout << xT("\t[-geotiffPath=path/to/heightmapTIFFs/collection]") << std::endl;
    tcout << xT("\t[-cachePath=path/to/heightmapSQLITE/DBfiles]") << std::endl;
    tcout << xT("\t[-stylesPath=path/to/main/styles or -stylesRecursivePath=path/to/main/styles ...]") << std::endl;
    tcout << xT("\t[-styleName=default]") << std::endl;
    tcout << xT("\t[-styleSetting:name=value ...]") << std::endl;
    tcout << xT("\t-outputRasterWidth=size_in_pixels") << std::endl;
    tcout << xT("\t-outputRasterHeight=size_in_pixels") << std::endl;
    tcout << xT("\t-outputImageFilename=path/with/filename and/or -outputJSONFilename=path/with/filename (without extension)") << std::endl;
    tcout << xT("\t[-outputImageFormat=png|jpeg]") << std::endl;
    tcout << xT("\t[-latLon=46.95:7.45 or -target31=x:y]") << std::endl;
    tcout << xT("\t[-targetOnRelief]") << std::endl;
    tcout << xT("\t[-zoom=15]") << std::endl;
    tcout << xT("\t[-azimuth=0]") << std::endl;
    tcout << xT("\t[-elevationAngle=90]") << std::endl;
    tcout << xT("\t[-frames=1 (>1 to animate)]") << std::endl;
    tcout << xT("\t[-endLatLon=46.95:7.45 or -endTarget31=x:y (when animated)]") << std::endl;
    tcout << xT("\t[-endZoom=15 (when animated)]") << std::endl;
    tcout << xT("\t[-endAzimuth=0 (when animated)]") << std::endl;
    tcout << xT("\t[-endElevationAngle=90 (when animated)]") << std::endl;
    tcout << xT("\t[-fov=16.5]") << std::endl;
    tcout << xT("\t[-referenceTileSize=256]") << std::endl;
    tcout << xT("\t[-displayDensityFactor=1.0]") << std::endl;
    tcout << xT("\t[-locale=en]") << std::endl;
    tcout << xT("\t[-verbose]") << std::endl;
#if defined(OSMAND_TARGET_OS_linux)
    tcout << xT("\t[-useLegacyContext]") << std::endl;
#endif
}
