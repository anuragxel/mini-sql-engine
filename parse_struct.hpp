#ifndef _DBS_PARSE_STRUCT_HPP
#define _DBS_PARSE_STRUCT_HPP

//     Developed for Database Systems Assignment 1
//         Anurag Ghosh - CSD - 201302179
//          Copyright Anurag Ghosh 2015.
// Distributed under the Boost Software License, Version 1.0.
//    See Copy at http://www.boost.org/LICENSE_1_0.txt

#include "enums.hpp"

namespace db {
struct parsed_query {
        std::string query_type; // type of query, has to be "select".

        std::vector<std::string> fields; // the selected fields
        std::vector<std::string> tables; // the tables

        aggregate_type::type agg_type; // type of aggregates. can be NONE to denote no aggregation.

        std::string agg_column; // column over which aggregation is to be done. can be empty.
        std::string dis_column; // column over which distinct is to be done. can be empty.

        std::vector<std::tuple<std::string,std::string, int>> conditionals; // column_name, comparison_type, value to compare to.

        logic_type::type logic_op; // the logical operator over which the conditionals operate. can be NONE to denote one conditional.

        bool is_join; // is a join query
        std::vector<std::tuple<std::string, std::string, std::string>> string_conditionals; // column_name, comparison_type, value to compare to.

};

template <typename T> std::string to_str(std::vector<T> strings, bool arr) {
        std::stringstream ss;
        if(arr)
                ss << "[ ";
        for(int i = 0; i < strings.size(); ++i) {
                if(i != 0) {
                        ss << ",";
                }
                ss << strings[i];
        }
        if(arr)
                ss << " ]";
        return ss.str();
}

std::string parse_output(struct parsed_query& p) {
        std::stringstream ss;
        ss << "===========================================" << std::endl;
        ss << "PARSE OUTPUT" << std::endl;
        ss << "query_type: " << p.query_type << std::endl;
        ss << "fields: " << to_str(p.fields,true) << std::endl;
        ss << "tables: " << to_str(p.tables,true) << std::endl;
        ss << "agg_type: ";
        switch(p.agg_type) {
        case db::aggregate_type::AVG:   ss << "AVG on " << p.agg_column; break;
        case db::aggregate_type::MIN:   ss << "MIN on " << p.agg_column; break;
        case db::aggregate_type::MAX:   ss << "MAX on " << p.agg_column; break;
        case db::aggregate_type::SUM:   ss << "SUM on " << p.agg_column; break;
        case db::aggregate_type::NONE:  ss << "NONE"; break;
        }
        ss << std::endl;
        if(!p.dis_column.empty()) {
                ss << "distinct_column: " << p.dis_column << std::endl;
        }
        for(auto& cond : p.string_conditionals) {
                ss << "string_condition: [ "<< std::get<0>(cond) << ", " << std::get<1>(cond) << ", " << std::get<2>(cond) << " ]"<< std::endl;
        }
        for(auto& cond : p.conditionals) {
                ss << "condition: [ "<< std::get<0>(cond) << ", " << std::get<1>(cond) << ", " << std::get<2>(cond) << " ]"<< std::endl;
        }
        ss << "logic_op: ";
        switch(p.logic_op) {
        case db::logic_type::AND:   ss << "AND" << std::endl; break;
        case db::logic_type::OR:   ss <<  "OR" << std::endl; break;
        case db::logic_type::NONE: ss << "NONE" << std::endl; break;
        }
        if(p.is_join) {
                ss << "join: true" << std::endl;
        }
        ss << "===========================================" << std::endl;
        return ss.str();
}
}

#endif
