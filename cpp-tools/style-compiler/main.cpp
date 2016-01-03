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

#include <OsmAndCoreTools/CStyle.h>
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
    coreResourcesEmbeddedBundle = OsmAnd::CoreResourcesEmbeddedBundle::loadFromLibrary(QLatin1String("OsmAndCore_ResourcesBundle_shared"));
#endif // defined(OSMAND_CORE_STATIC)
    OsmAnd::InitializeCore(coreResourcesEmbeddedBundle);

    // Parse configuration
    OsmAndTools::CStyle::Configuration configuration;
    QString error;
    QStringList commandLineArgs;
    for (int idx = 1; idx < argc; idx++)
        commandLineArgs.push_back(argv[idx]);
    if (!OsmAndTools::CStyle::Configuration::parseFromCommandLineArguments(commandLineArgs, configuration, error))
    {
        printUsage(QStringToStlString(error));
        OsmAnd::ReleaseCore();
        return -1;
    }

    // Compile map style
    const auto success = OsmAndTools::CStyle(configuration).compile();

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
    tcout << xT("OsmAnd CStyle tool is a console utility to compile map style using specified emitter.") << std::endl;
    tcout << std::endl;
    tcout << xT("Arguments:") << std::endl;
    tcout << xT("\t[-stylesPath=path/to/main/styles or -stylesRecursivePath=path/to/main/styles ...]") << std::endl;
    tcout << xT("\t[-styleName=default]") << std::endl;
    tcout << xT("\t[-emitter=debug]") << std::endl;
    tcout << xT("\t[-verbose]") << std::endl;
}
