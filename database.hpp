#ifndef _DBS_DATABASE_HPP
#define _DBS_DATABASE_HPP

//     Developed for Database Systems Assignment 1
//         Anurag Ghosh - CSD - 201302179
//          Copyright Anurag Ghosh 2015.
// Distributed under the Boost Software License, Version 1.0.
//    See Copy at http://www.boost.org/LICENSE_1_0.txt

#include "table.hpp"
#include "enums.hpp"
#include "parse_struct.hpp"

#include <dirent.h>
#include <sys/stat.h>

namespace db {
    class binary_predicate {
    private:
        int index_;
        int which_comp_;
    public:
        binary_predicate(int p, int w): index_(p), which_comp_(w) {}
        bool operator()(const db::row& a, const db::row& b) {
            if(which_comp_ == 0) return a[index_] < b[index_];
            if(which_comp_ == 1) return a[index_] == b[index_];
            return a[index_] > b[index_];
        }
    };

    class database {
    private:
        std::ifstream metadata_file_;
        std::vector<table> tables_;
        std::vector<std::string> get_csv_filenames() {
            DIR *dpdf;
            struct dirent *epdf;
            struct stat st;
            std::vector<std::string> vec;
            dpdf = opendir("./");
            if (dpdf != NULL){
                while ((epdf = readdir(dpdf))) {
                    std::string file_name = epdf->d_name;
                    if(file_name[0] == '.') {
                        continue;
                    }
                    if (stat(file_name.c_str(), &st) == -1) {
                        continue;
                    }
                    const bool is_directory = (st.st_mode & S_IFDIR) != 0;
                    if (is_directory) {
                        continue;
                    }
                    int index = file_name.find_last_of(".");
                    if(file_name.substr(index + 1) == "csv") {
                        vec.push_back(file_name.substr(0,index));
                    }
                }
            }
            closedir(dpdf);
            return vec;
        }
    public:
        database() {
            metadata_file_.open("metadata.txt");
            for(auto &table_name : get_csv_filenames()) {
                tables_.push_back(table(metadata_file_,table_name));
            }
        }

        table operator[] (const std::string& str) {
            auto it = std::find(tables_.begin(), tables_.end(), str);
            if(it == tables_.end()) {
                throw "FatalError: The Table Doesn't Exist in Database";
            }
            return *it;
        }

        table get_table(const std::string& str) {
            return this->operator[](str);
        }

        void sanitize_field(std::string& p, parsed_query& pq) {
            bool ifa = false;
            if(std::regex_match(p, std::regex("min[(].*[)]"))) {
                pq.agg_type = db::aggregate_type::MIN;
                ifa = true;
            }
            else if(std::regex_match(p, std::regex("max[(].*[)]"))) {
                pq.agg_type = db::aggregate_type::MAX;
                ifa = true;
            }
            else if(std::regex_match(p, std::regex("sum[(].*[)]"))) {
                pq.agg_type = db::aggregate_type::SUM;
                ifa = true;
            }
            else if(std::regex_match(p, std::regex("avg[(].*[)]"))) {
                pq.agg_type = db::aggregate_type::AVG;
                ifa = true;
            }
            if(ifa == true) {
                p = p.substr(4);
                p.erase(p.size() - 1);
                pq.agg_column = p;
            }
            if(std::regex_match(p, std::regex("distinct[(].*[)]"))) {
                p = p.substr(9);
                p.erase(p.size() - 1);
                pq.dis_column = p;
            }
        }

        void to_lower(std::string& data) {
            std::transform(data.begin(), data.end(), data.begin(), ::tolower);
        }

        void complete_field(std::string& field, parsed_query& p) {
            int once = 0;
            for(auto& table_name: p.tables) {
                auto table = get_table(table_name);
                if(table.field_check(field)) {
                    field = table_name + "." + field;
                    once += 1;
                }
            }
            if(once > 1) {
                throw "QueryError: Ambiguous column referred to.";
            }
            if(once == 0) {
                throw "QueryError: Field " + field + " does not exist.";
            }
        }

        parsed_query parser(const std::string& csql_query) {
            std::string sql_query(csql_query);
            parsed_query p;

            p.agg_type = db::aggregate_type::NONE;
            p.is_join = false;
            p.logic_op = db::logic_type::NONE;

            std::stringstream ss(sql_query);
            std::string token;
            int number = 0;
            bool starred = false;
            while(ss >> token) {
                if(number == 0) { // select
                    to_lower(token);
                    if(token != "select") {
                        throw "UnimplementedError: Operation " + token + " Unsupported.";
                    }
                    p.query_type = token;
                }
                else if(number == 1) { // fields
                    if(token == "*") {
                        starred =  true;
                    }
                    else {
                        starred = false;
                        std::stringstream tokenizer(token);
                        std::string field;
                        while(std::getline(tokenizer,field,',')) {
                            sanitize_field(field, p);
                            p.fields.push_back(field);
                        }
                    }
                }
                else if(number == 2) { // from
                    to_lower(token);
                    if(token != "from") {
                        throw "QueryError: Malformed Query.";
                    }
                }
                else if(number == 3) { // tables
                    std::stringstream tokenizer(token);
                    std::string table;
                    while(std::getline(tokenizer,table,',')) {
                        p.tables.push_back(table);
                    }
                    if(starred == true) {
                        for(auto& table: p.tables) {
                            std::vector<std::string> b = get_table(table).fields();
                            std::vector<std::string> c;
                            for(auto& var : b) {
                                c.push_back(table + "." + var);
                            }
                            p.fields.reserve(p.fields.size() + b.size());
                            p.fields.insert(p.fields.end(), c.begin(), c.end());
                            c.clear();
                            b.clear();
                        }
                    }
                    else {
                        for(auto& field : p.fields) {
                            complete_field(field,p);
                        }
                    }
                    if(!p.dis_column.empty()) {
                        complete_field(p.dis_column,p);
                    }
                    if(!p.agg_column.empty()) {
                        complete_field(p.agg_column,p);
                    }
                    if(p.tables.size() > 1) { // if greater than one table, join.
                        p.is_join = true;
                    }
                }
                else if(number == 4) { // where
                    to_lower(token);
                    if(token != "where") {
                        throw "QueryError: Malformed Query.";
                    }
                }
                else { // condition
                    try {
                        if(std::regex_match(token,std::regex("([a-zA-Z0-9.]+)[=<>]([-+]?[0-9]+)"))) { // not join condition
                            for(auto &c :{"=",">","<"}) {
                                if(token.find_last_of(c) != std::string::npos) {
                                        int index = token.find_last_of(c);
                                        int value = std::stoi(token.substr(index+1));
                                        std::string field = token.substr(0,index);
                                        complete_field(field,p);
                                        p.conditionals.push_back(std::make_tuple(field, c, value));
                                }
                            }
                        }
                        else if (std::regex_match(token,std::regex("([a-zA-Z0-9.]+)[=<>]([a-zA-Z0-9.]+)"))) { // join condition
                            for(auto &c :{"=",">","<"}) {
                                if(token.find_last_of(c) != std::string::npos) {
                                        int index = token.find_last_of(c);
                                        std::string field2 = token.substr(index+1);
                                        std::string field1 = token.substr(0,index);
                                        complete_field(field1,p);
                                        complete_field(field2,p);
                                        p.string_conditionals.push_back(std::make_tuple(field1, c, field2));
                                }
                            }
                        }
                        else { // logical op
                            to_lower(token);
                            if(token == "or" or token == "||") {
                                p.logic_op = db::logic_type::OR;
                            }
                            else if (token == "and" or token == "&&"){
                                p.logic_op = db::logic_type::AND;
                            }
                            else {
                                throw "QueryError: Malformed Query.";
                            }
                        }
                    }
                    catch(std::regex_error& e) {
                        throw std::string("QueryError: ") + std::string(e.what());
                    }
                }
                number += 1;
            }
            return p;
        }

        bool satisfies_join(const db::table& tb1, const db::table& tb2,
          const db::row& rb1, const db::row& rb2,
          std::tuple<std::string,std::string, std::string> join_condition) {
            std::string field1_str = std::get<0>(join_condition);
            std::string field2_str = std::get<2>(join_condition);
            int field1;
            int field2;
            int value1;
            int value2;
            if(tb1.is_field_present(field1_str)) {
                field1 = tb1.get_index_of_field(field1_str);
                value1 = rb1[field1];
                field2 = tb2.get_index_of_field(field2_str);
                value2 = rb2[field2];
            }
            else if(tb2.is_field_present(field1_str)) {
                field1 = tb2.get_index_of_field(field1_str);
                value1 = rb2[field1];
                field2 = tb1.get_index_of_field(field2_str);
                value2 = rb1[field2];
            }
            else {
                throw "RuntimeError: Join Condition is Invalid!";
            }
            std::string cond = std::get<1>(join_condition);
            if(cond == "=") return (value1 == value2);
            if(cond == ">") return (value1 > value2);
            if(cond == "<") return (value1 < value2);
            return true;
        }

        void execute_join(parsed_query& query) {

        }

        bool satisfies(const db::table& tb, const db::row& rb,
          std::vector<std::tuple<std::string,std::string, int>> conditions,
          std::vector<std::tuple<std::string,std::string, std::string>> string_conditions,
          logic_type::type logic_op) {
            bool rv =  true;
            std::vector<bool> truth_values;
            int number = 0;
            for(auto& condition : conditions) {
                int field = tb.get_index_of_field(std::get<0>(condition));
                std::string cond = std::get<1>(condition);
                int value = std::get<2>(condition);
                int orig_value = rb[field];
                if(cond == "=") rv = (orig_value == value);
                else if(cond == ">") rv = (orig_value > value);
                else if(cond == "<") rv = (orig_value < value);
                truth_values.push_back(rv);
                number += 1;
            }
            for(auto& condition : string_conditions) {
                int field1 = tb.get_index_of_field(std::get<0>(condition));
                int value1 = rb[field1];
                int field2 = tb.get_index_of_field(std::get<2>(condition));
                int value2 = rb[field2];
                std::string cond = std::get<1>(condition);
                if(cond == "=") rv = (value1 == value2);
                else if(cond == ">") rv = (value1 > value2);
                else if(cond == "<") rv = (value1 < value2);
                truth_values.push_back(rv);
                number += 1;
            }
            if(number > 1) {
                if(logic_op == db::logic_type::AND) return truth_values[0] and truth_values[1];
                else if(logic_op == db::logic_type::OR) return truth_values[0] or truth_values[1];
                else throw "RuntimeError: Logical Operator is Invalid!";
            }
            return rv;
        }

        void execute(parsed_query& query) {
          for(auto& table_name : query.tables) {
              std::vector<int> indices;
              db::table tb = get_table(table_name);
              for(auto& field : query.fields) {
                  if(tb.is_field_present(field)) {
                      indices.push_back(tb.get_index_of_field(field));
                  }
              }
              std::vector<db::row> rows;
              std::vector<int> tuple;
              for(int i = 0; i < tb.size(); i++) {
                  db::row rb = tb[i];
                  for(auto& index : indices) {
                      tuple.push_back(rb[index]);
                  }
                  db::row tmp = db::row(tuple);
                  if(satisfies(tb,rb,query.conditionals,query.string_conditionals,query.logic_op)) {
                      rows.push_back(tmp);
                  }
                  tuple.clear();
              }
              if(!query.dis_column.empty()) {
                  int index = tb.get_index_of_field(query.dis_column);
                  std::sort(rows.begin(), rows.end(), binary_predicate(index, 0));
                  auto last = std::unique(rows.begin(), rows.end(), binary_predicate(index, 1));
                  rows.erase(last, rows.end());
              }
              if(query.agg_type != db::aggregate_type::NONE) {
                  int index = tb.get_index_of_field(query.agg_column);
                  int summation = 0;
                  std::vector<int> n_tuple;
                  switch(query.agg_type) {
                      case db::aggregate_type::MIN:
                          std::sort(rows.begin(), rows.end(), binary_predicate(index, 0));
                          rows.erase(rows.begin() + 1, rows.end());
                          break;
                      case db::aggregate_type::MAX:
                          std::sort(rows.begin(), rows.end(), binary_predicate(index, 2));
                          rows.erase(rows.begin() + 1, rows.end());
                          break;
                      case db::aggregate_type::SUM:
                          std::sort(rows.begin(), rows.end(), binary_predicate(index, 0));
                          for(auto& r : rows) {
                              summation += r[index];
                          }
                          for(int i = 0; i < rows[0].size(); i++) {
                              n_tuple.push_back(rows[0][i]);
                          }
                          n_tuple[index] = summation;
                          rows.clear();
                          rows.push_back(n_tuple);
                          break;
                      case db::aggregate_type::AVG:
                          std::sort(rows.begin(), rows.end(), binary_predicate(index, 0));
                          for(auto& r : rows) {
                              summation += r[index];
                          }
                          for(int i = 0; i < rows[0].size(); i++) {
                              n_tuple.push_back(rows[0][i]);
                          }
                          summation = (int)((double)summation/(double)rows.size());
                          n_tuple[index] = summation;
                          rows.clear();
                          rows.push_back(n_tuple);
                          break;
                  }
              }
              std::cout << to_str(query.fields, false) << std::endl;
              for(auto& row : rows) {
                  std::cout << row.to_string() << std::endl;
              }
          }
        }

        void query(const std::string& sql_query) {
            try {
                parsed_query p = parser(sql_query);
                //std::cout << parse_output(p) << std::endl;
                //std::cout << "===========================================" << std::endl;
                if(p.is_join) {
                    execute_join(p);
                    return;
                }
                execute(p);
                //std::cout << "===========================================" << std::endl;
            } catch(std::string& s) {
                std::cout << "Error: " << s << std::endl;
            }
            catch(char const* s) {
                std::cout << "Error: " << s << std::endl;
            }
        }
    };
}

#endif
