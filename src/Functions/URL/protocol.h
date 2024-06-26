#pragma once

#include <Common/StringUtils.h>
#include <Functions/StringHelpers.h>


namespace DB
{

/// Extracts scheme from given url.
inline std::string_view getURLScheme(const char * data, size_t size)
{
    // scheme = ALPHA *( ALPHA / DIGIT / "+" / "-" / "." )
    const char * pos = data;
    const char * end = data + size;

    if (isAlphaASCII(*pos))
    {
        for (++pos; pos < end; ++pos)
        {
            if (!(isAlphaNumericASCII(*pos) || *pos == '+' || *pos == '-' || *pos == '.'))
            {
                break;
            }
        }

        return std::string_view(data, pos - data);
    }

    return {};
}

struct ExtractProtocol
{
    static size_t getReserveLengthForElement()
    {
        return strlen("https") + 1;
    }

    static void execute(Pos data, size_t size, Pos & res_data, size_t & res_size)
    {
        res_data = data;
        res_size = 0;

        std::string_view scheme = getURLScheme(data, size);
        Pos pos = data + scheme.size();

        if (scheme.empty() || (data + size) - pos < 4)
            return;

        if (pos[0] == ':')
            res_size = pos - data;
    }
};

}
