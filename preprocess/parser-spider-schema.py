import json
import pickle
import os
import traceback
from canonicalized_data_format.ddl_objects import Table, DBSchemaModel

"""Parses schema for each db_id and formats info into DDL format"""
class SpiderSchemaParser:
    
    def __init__(self, datasource):
        self.dbSchemasFormatted = {}
        self.currDbSchema = None
        self.parse_data_source(datasource)

    def get_formatted_schemas(self):
        return self.dbSchemasFormatted
    
    def get_schema_by_db_id(self, db_id):
        return self.dbSchemasFormatted.get(db_id)
    
    def parse_primary_keys(self, primary_keys):
        result = {}
        primary_keys = primary_keys.split('|')
        for pair in primary_keys:
            if len(pair) != 0:
                table_name, p_key = pair.split(':')
                result[table_name.strip()] = p_key.strip()
        return result
    
    def parse_data_source(self, datasource):
        if isinstance(datasource, list):
            for db in datasource:
                self.currDbSchema = DBSchemaModel()
                db_id = db['db_id']
                table_names = db['Schema (values (type))'] 
                primary_keys = db['Primary Keys']
                foreign_keys = db['Foreign Keys']
                primary_keys = self.parse_primary_keys(primary_keys)
                self.currDbSchema.foreign_keys = self.add_foreign_key_constraints(foreign_keys)
                self.parse_schema(table_names, primary_keys)

                #Add the new formatted db-schema object to the mapping collection
                self.dbSchemasFormatted[db_id] = self.currDbSchema
                self.currDbSchema = None
        

    def parse_schema(self, schema_values, primary_keys):
        tables = schema_values.split('|')
        for table in tables:
            table = self.parse_table(table)
            p_key = primary_keys.get(table.name)
            if p_key != None:
                table.set_primary_key(p_key)
            self.currDbSchema.add_table(table)

    def parse_table(self, table_vals) -> Table:
        table_name, attrs = table_vals.split(':')
        table = Table(table_name.strip())
        self.add_attributes(table, attrs.strip().split(','))
        return table
    
    def add_attributes(self, table: Table, attributes: list):
        def extract_name_and_type(text):
            start = text.find('(')
            end = text.find(')')
            if start < 0 or end < 0:
                return text.strip(), "unknown"
            return text[:start].strip(), text[start+1:end].strip()

        for attr_str in attributes:
            name, type = extract_name_and_type(attr_str.strip())
            table.add_attribute(name, type)


    def add_foreign_key_constraints(self, foreign_keys):
        fk_dict = {}
        constraints = foreign_keys.split('|')
        for constraint in constraints:
            if len(constraint) != 0:
                lhs, rhs = constraint.strip().split('equals')
                lhs_table, lhs_attr = map(str.strip, lhs.strip().split(':'))
                rhs_table, rhs_attr = map(str.strip, rhs.strip().split(':'))
                fk_dict[(lhs_table, lhs_attr)] = (rhs_table, rhs_attr)
        return fk_dict


    
if __name__ == '__main__':
    try:
        with open('/Users/anikaraghavan/Downloads/text2sql-eval/data/spider/schema/spider_schema_rows_v2.json') as file:
            datasource = json.load(file)
            parser = SpiderSchemaParser(datasource)

        with open('/Users/anikaraghavan/Downloads/text2sql-eval/data/spider/interim_db_schemas_object', 'wb') as file:
            pickle.dump(parser.dbSchemasFormatted, file)
        
    except FileNotFoundError:
        print("Error: The file was not found.")
    except json.JSONDecodeError:
        print("Error: Could not decode JSON from the file. Check if the file contains valid JSON.")
    except Exception as e:
        print(f"An unexpected error occurred")
        traceback.print_exc()

