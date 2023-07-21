import os
from typing import Iterable
import pymysql
from dotenv import load_dotenv

class DbConnection:
    def __init__(self) -> None:
        self.connection = None
        self.cursor = None
        

    def connect(self, host:str, user:str,
        password:str, database:str) -> pymysql.Connection:
        self.connection = pymysql.connect(
            host=host,
            user=user,
            password=password,
            database=database
        )
        self.cursor = self.connection.cursor()


    def close_connection(self) -> None:
        if self.cursor:
            self.cursor.close()
        if self.connection:
            self.connection.close()


    def create_table(self, table_name:str,
        fields:dict, field_pk:str)-> None:
        try:
            field_str = ", ".join([f"{field} {fields[field]}" for field in fields])
            sql_query = f"CREATE TABLE IF NOT EXISTS {table_name} ({field_str}, PRIMARY KEY ({field_pk}))"
            self.cursor.execute(sql_query)
            self.connection.commit()
            print(f"Table '{table_name}' created successfully!")
        except pymysql.Error as e:
            print(f"Error creating table '{table_name}': {e}")


    def insert_into(self, table_name:str, data_fields:dict) -> None:
        try:
            fields = ", ".join(data_fields.keys())
            values_placeholder = ", ".join(["%s" for _ in data_fields])
            values = tuple(data_fields.values())
            sql_query = f"INSERT INTO {table_name} ({fields}) VALUES ({values_placeholder})"
            self.cursor.execute(sql_query, values)
            self.connection.commit()
            print(f"Data inserted into '{table_name}' successfully!")
        except pymysql.Error as e:
            print(f"Error in insert in '{table_name}': {e}")


    def insert_many(self, table_name:str, data_list:list[dict]) -> None:
        try:
            fields = ", ".join(data_list[0].keys())
            values_placeholder = ", ".join(["%s" for _ in data_list[0]])
            data_values = [tuple(d.values()) for d in data_list]
            sql_query = f"INSERT INTO {table_name} ({fields}) VALUES ({values_placeholder})"
            self.cursor.executemany(sql_query, data_values)
            self.connection.commit()
            print(f"Data inserted into '{table_name}' successfully!")
        except pymysql.Error as e:
            print(f"Error in insert many in '{table_name}': {e}")


    def select_all_data(self, table_name:str)-> None:
        try:
            sql_query = f"SELECT * FROM {table_name}"
            self.cursor.execute(sql_query)
            result = self.cursor.fetchall()
            for row in result:
                print(row)
        except pymysql.Error as e:
            print(f"Error to select all in '{table_name}': {e}")


    def select_data(self, table_name:str, field:str, filter:str, value:str|float) -> None:
        try:
            valid_filters = ['=', '>', '<', '>=', '<=']
            if filter not in valid_filters:
                raise ValueError("Invalid filter")
            sql_query = f"SELECT * FROM {table_name} WHERE {field} {filter} %s"
            self.cursor.execute(sql_query, (value,))
            result = self.cursor.fetchall()
            print(result)
        except pymysql.Error as e:
            print(f"Error to select in '{table_name}': {e}")            
    
    def delete_where(self, table_name:str, id:str|float):
        try:
            sql_query = f"DELETE FROM {table_name} WHERE id = {id}"
            self.cursor.execute(sql_query)
            self.connection.commit()
        except pymysql.Error as e:
            print(f"Error to delete id '{id}' in '{table_name}': {e}")

    def update_where(self, table_name:str, id:str|float,
        fields:str|Iterable, values:str|Iterable, many:bool=False) -> None:
        try:
            if many:
                if len(fields) != len(values):
                    raise ValueError("The number of fields and values \
                            must be the same when updating multiple records.")
                set_clause = ", ".join([f"{field} = %s" for field in fields])
                sql_query = f"UPDATE {table_name} SET {set_clause} WHERE id = %s"
                record_values = tuple(values + [id])
                self.cursor.execute(sql_query, record_values)
            else:
                if isinstance(fields, str):
                    fields = [fields]
                if isinstance(values, str):
                    values = [values]

                if len(fields) != len(values):
                    raise ValueError("The number of fields and values\
                            must be the same when updating a single record.")
                set_clause = ", ".join([f"{field} = %s" for field in fields])
                sql_query = f"UPDATE {table_name} SET {set_clause} WHERE id = %s"
                record_values = tuple(values + [id])
                self.cursor.execute(sql_query, record_values)
            self.connection.commit()
            print("Data updated successfully!")
        
        except (pymysql.Error, ValueError) as e:
            print(f"Error updating data in '{table_name}': {e}")    


if __name__ == '__main__':
    ...
    