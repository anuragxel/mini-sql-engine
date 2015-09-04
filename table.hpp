#ifndef _DBS_TABLE_HPP
#define _DBS_TABLE_HPP

//     Developed for Database Systems Assignment 1
//         Anurag Ghosh - CSD - 201302179
//          Copyright Anurag Ghosh 2015.
// Distributed under the Boost Software License, Version 1.0.
//    See Copy at http://www.boost.org/LICENSE_1_0.txt

#include <iostream>
#include <fstream>
#include <istream>
#include <ostream>
#include <sstream>

#include <string>
#include <vector>
#include <algorithm>

#include "row.hpp"

namespace db {
class table {
private:
std::string name_; // table Name
std::vector<std::string> fields_; // Fields Metadata
std::vector<row> rows_; // The actual rows.
row get_next_row(std::ifstream& str) {
        std::vector<int> result;
        std::string line;
        std::getline(str,line);
        std::stringstream lineStream(line);
        std::string cell;
        while(std::getline(lineStream,cell,',')) {
                result.push_back(std::stoi(cell));
        }
        return row(name_, result);
}
public:
table(std::ifstream& metadata_file, const std::string& table_name) {
        name_ = table_name;
        metadata_file.clear();
        metadata_file.seekg(0, std::ios::beg);
        std::string line;
        while(metadata_file >> line) {
                if(line == "<begin_table>") {
                        metadata_file >> line;
                        if(line == table_name) {
                                while(true) {
                                        metadata_file >> line;
                                        if(line == "<end_table>") {
                                                break;
                                        }
                                        fields_.push_back(line);
                                }
                        }
                }
        }
        std::ifstream row_file;
        row_file.open((table_name + ".csv").c_str());
        while(row_file) {
                row r = get_next_row(row_file);
                if(!row_file) {
                        break;
                }
                if(this->number_of_fields() != r.size()) {
                        throw ("ConsistencyError: Database Incosistent in Table -" + name_);
                }
                rows_.push_back(r);
        }
}

std::size_t size() const {
        return rows_.size();
}

std::string name() const {
        return name_;
}

std::size_t number_of_fields() const {
        return fields_.size();
}

std::vector<std::string> fields() const {
        return fields_;
}

std::string schema() {
        std::stringstream ss;
        for(int i = 0; i < fields_.size(); ++i) {
                if(i != 0) {
                        ss << ",";
                }
                ss << fields_[i];
        }
        return ss.str();
}

bool is_field_present(const std::string& cstr) const {
        std::string str(cstr);
        int index = str.find_last_of(".");

        if(index != std::string::npos) {
                if(str.substr(0,index) != name_) {
                        return false;
                }
                str = str.substr(index+1);
        }
        return (std::find(fields_.begin(), fields_.end(), str) != fields_.end());
}

// just checks if the field name is present or not,
//irrespective of table_name qualifier.
bool field_check(const std::string& cstr) const {
        std::string str(cstr);
        int index = str.find_last_of(".");

        if(index != std::string::npos) {
                str = str.substr(index+1);
        }
        return (std::find(fields_.begin(), fields_.end(), str) != fields_.end());
}

int get_index_of_field(const std::string& cstr) const {
        std::string str(cstr);
        int index = str.find_last_of(".");

        if(index != std::string::npos) {
                if(str.substr(0,index) != name_) {

                        throw "NameError: The Field " + str + "  doesn't Exist in " + name_;
                }
                str = str.substr(index+1);
        }
        auto it = std::find(fields_.begin(), fields_.end(), str);
        if(it == fields_.end()) {
                throw "NameError: The Field " + str + "  doesn't Exist in " + name_;
        }
        return it - fields_.begin();
}

bool operator==(const std::string& str) {
        return name_ == str;
}

row operator[](int index) {
        if(index > rows_.size() || index < 0) {
                throw "IndexError: Out of bounds.";
        }
        return rows_[index];
}
};
}

#endif
