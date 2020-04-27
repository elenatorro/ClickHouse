#include <Storages/StorageFile.h>
#include <Storages/StorageFactory.h>

#include <Interpreters/Context.h>
#include <Interpreters/evaluateConstantExpression.h>

#include <Parsers/ASTLiteral.h>
#include <Parsers/ASTIdentifier.h>

#include <IO/ReadBufferFromFile.h>
#include <IO/ReadHelpers.h>
#include <IO/WriteBufferFromFile.h>
#include <IO/WriteHelpers.h>

#include <Formats/FormatFactory.h>
#include <DataTypes/DataTypeString.h>
#include <DataStreams/IBlockInputStream.h>
#include <DataStreams/IBlockOutputStream.h>
#include <DataStreams/AddingDefaultsBlockInputStream.h>
#include <DataStreams/narrowBlockInputStreams.h>

#include <Common/escapeForFileName.h>
#include <Common/typeid_cast.h>
#include <Common/parseGlobs.h>

#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>

#include <Poco/Path.h>
#include <Poco/File.h>

#include <re2/re2.h>
#include <filesystem>
#include <Storages/Distributed/DirectoryMonitor.h>
#include <Processors/Sources/SourceWithProgress.h>
#include <Processors/Pipe.h>

namespace fs = std::filesystem;

namespace DB
{

namespace ErrorCodes
{
    extern const int BAD_ARGUMENTS;
    extern const int NOT_IMPLEMENTED;
    extern const int CANNOT_SEEK_THROUGH_FILE;
    extern const int CANNOT_TRUNCATE_FILE;
    extern const int DATABASE_ACCESS_DENIED;
    extern const int NUMBER_OF_ARGUMENTS_DOESNT_MATCH;
    extern const int UNKNOWN_IDENTIFIER;
    extern const int INCORRECT_FILE_NAME;
    extern const int FILE_DOESNT_EXIST;
}

namespace
{

/* Recursive directory listing with matched paths as a result.
 * Have the same method in StorageHDFS.
 */
std::vector<std::string> listFilesWithRegexpMatching(const std::string & path_for_ls, const std::string & for_match)
{
    const size_t first_glob = for_match.find_first_of("*?{");

    const size_t end_of_path_without_globs = for_match.substr(0, first_glob).rfind('/');
    const std::string suffix_with_globs = for_match.substr(end_of_path_without_globs);   /// begin with '/'

    const size_t next_slash = suffix_with_globs.find('/', 1);
    auto regexp = makeRegexpPatternFromGlobs(suffix_with_globs.substr(0, next_slash));
    re2::RE2 matcher(regexp);

    std::vector<std::string> result;
    const std::string prefix_without_globs = path_for_ls + for_match.substr(1, end_of_path_without_globs);
    if (!fs::exists(fs::path(prefix_without_globs)))
    {
        return result;
    }
    const fs::directory_iterator end;
    for (fs::directory_iterator it(prefix_without_globs); it != end; ++it)
    {
        const std::string full_path = it->path().string();
        const size_t last_slash = full_path.rfind('/');
        const String file_name = full_path.substr(last_slash);
        const bool looking_for_directory = next_slash != std::string::npos;
        /// Condition is_directory means what kind of path is it in current iteration of ls
        if (!fs::is_directory(it->path()) && !looking_for_directory)
        {
            if (re2::RE2::FullMatch(file_name, matcher))
            {
                result.push_back(it->path().string());
            }
        }
        else if (fs::is_directory(it->path()) && looking_for_directory)
        {
            if (re2::RE2::FullMatch(file_name, matcher))
            {
                /// Recursion depth is limited by pattern. '*' works only for depth = 1, for depth = 2 pattern path is '*/*'. So we do not need additional check.
                Strings result_part = listFilesWithRegexpMatching(full_path + "/", suffix_with_globs.substr(next_slash));
                std::move(result_part.begin(), result_part.end(), std::back_inserter(result));
            }
        }
    }
    return result;
}

std::string getTablePath(const std::string & table_dir_path, const std::string & format_name)
{
    return table_dir_path + "/data." + escapeForFileName(format_name);
}

/// Both db_dir_path and table_path must be converted to absolute paths (in particular, path cannot contain '..').
void checkCreationIsAllowed(const Context & context_global, const std::string & db_dir_path, const std::string & table_path)
{
    if (context_global.getApplicationType() != Context::ApplicationType::SERVER)
        return;

    /// "/dev/null" is allowed for perf testing
    if (!startsWith(table_path, db_dir_path) && table_path != "/dev/null")
        throw Exception("File is not inside " + db_dir_path, ErrorCodes::DATABASE_ACCESS_DENIED);

    Poco::File table_path_poco_file = Poco::File(table_path);
    if (table_path_poco_file.exists() && table_path_poco_file.isDirectory())
        throw Exception("File must not be a directory", ErrorCodes::INCORRECT_FILE_NAME);
}
}

;
}
}
