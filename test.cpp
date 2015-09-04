#ifndef _TEST_CPP
#define _TEST_CPP

//     Developed for Database Systems Assignment 1
//         Anurag Ghosh - CSD - 201302179
//          Copyright Anurag Ghosh 2015.
// Distributed under the Boost Software License, Version 1.0.
//    See Copy at http://www.boost.org/LICENSE_1_0.txt

#include "database.hpp"

int main(int argc, char** argv) {
        if(argc < 2 || argc > 2) {
                std::cout << "USAGE: db \"sql_query\"" << std::endl;
                return -1;
        }
        std::string sql_query(argv[1]);
        db::database anurag_db;
        anurag_db.query(sql_query);
}

#endif
