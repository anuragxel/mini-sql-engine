#ifndef _DBS_ROW_HPP
#define _DBS_ROW_HPP

//     Developed for Database Systems Assignment 1
//         Anurag Ghosh - CSD - 201302179
//          Copyright Anurag Ghosh 2015.
// Distributed under the Boost Software License, Version 1.0.
//    See Copy at http://www.boost.org/LICENSE_1_0.txt

#include <iostream>
#include <sstream>

#include <string>
#include <vector>
#include <algorithm>

#include "enums.hpp"

namespace db {
    class row {
    private:
        std::string row_of_; // Table Name
        std::vector<int> tuple_; // Data can be assumed to be only numbers, as said in specification.
    public:
        row(const std::string& table_name, const std::vector<int>& tuple)  {
            row_of_ = table_name;
            tuple_= tuple;
        }

        row(const std::vector<int>& tuple)  {
            row_of_ = "anonymous_table";
            tuple_= tuple;
        }

        row(const row& r)  {
            row_of_ = r.row_of_;
            tuple_= r.tuple_;
        }

        std::size_t size() const {
            return tuple_.size();
        }

        std::string to_string() const {
            std::stringstream ss;
            for(int i = 0; i < tuple_.size(); ++i) {
                if(i != 0) {
                    ss << ",";
                }
                ss << std::to_string(tuple_[i]);
            }
            return ss.str();
        }

        bool operator==(const std::string& row_name) const {
            if(row_of_ == "anonymous_table") {
                throw "NameError: Given row is not associated with a named table.";
            }
            return row_name == row_of_;
        }

        bool operator==(const row& other_row) const {
            if(other_row.row_of_ == row_of_ and
                other_row.tuple_.size() == tuple_.size() ) {
                for(int i = 0; i < tuple_.size(); i++) {
                    if(tuple_[i] != other_row.tuple_[i]) {
                        return false;
                    }
                }
                return true;
            }
            return false;
        }

        int operator[](int index) const {
            if(index > tuple_.size() || index < 0) {
                throw "IndexError: Out of bounds.";
            }
            return tuple_[index];
        }

    };
}

#endif
