#ifndef _DBS_ENUMS_HPP
#define _DBS_ENUMS_HPP

//     Developed for Database Systems Assignment 1
//         Anurag Ghosh - CSD - 201302179
//          Copyright Anurag Ghosh 2015.
// Distributed under the Boost Software License, Version 1.0.
//    See Copy at http://www.boost.org/LICENSE_1_0.txt

#include <iostream>
#include <sstream>

#include <string>
#include <vector>
#include <tuple>

#include <regex>
#include <algorithm>

namespace db {
    struct aggregate_type {
        enum type {
                SUM, AVG, MAX, MIN, NONE
        };
    };

    struct logic_type {
        enum type {
                OR, AND, NONE
        };
    };
}

#endif
