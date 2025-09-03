import sqlite3
import pandas as pd 

conn = sqlite3.connect("STAFF.db")
table_name = "INSTRUCTOR"
attribute_list = ["ID","FNAME","LNAME","CITY","CCODE"]

file_path = "/home/project/INSTRUCTOR.csv"
df = pd.read_csv(file_path, names = attribute_list)
df.to_sql(table_name,conn,if_exists = "replace", index=False)
print("Table is ready")

query_statement = f"SELECT * FROM {table_name}"
query_output = pd.read_sql(query_statement,conn)
print(query_statement)
print(query_output)

query_statement = f"SELECT FNAME FROM {table_name}"
query_output = pd.read_sql(query_statement,conn)
print(query_statement)
print(query_output)

query_statement = f"SELECT COUNT(*) FROM {table_name}"
query_output = pd.read_sql(query_statement,conn)
print(query_statement)
print(query_output)

data_dict = {"ID": [100],
             "FNAME": ["Jonh"],
             "LNAME": ["Doe"],
             "CITY": ["Paris"],
             "CCODE": ["FR"]}
data_append = pd.DataFrame(data_dict)
data_append.to_sql(table_name,conn,if_exists = "append", index = False)
print("Data append successfully")

query_statement = f"SELECT COUNT(*) FROM {table_name}"
query_output = pd.read_sql(query_statement,conn)
print(query_statement)
print(query_output)


table_name2 = "Departments"
dep_attributes_list = ["DEPT_ID","DEP_NAME","MANAGER_ID","LOC_ID"]
file_path2 = "/home/project/Departments.csv"

df2 = pd.read_csv(file_path2, names = dep_attributes_list)
df2.to_sql(table_name2,conn,if_exists = "replace", index = False)
print("Table 2 is read")

data_dict2 = {"DEPT_ID": [9],
              "DEP_NAME": ["Quality Assurance"],
              "MANAGER_ID": [30010],
              "LOC_ID":["L0010"]}

data_append2 = pd.DataFrame(data_dict2)
data_append2.to_sql(table_name2,conn,if_exists = "append", index = False)
print("Data append successfully")

query_statement2 = f"SELECT * FROM {table_name2}"
query_output2 = pd.read_sql(query_statement2,conn)
print(query_statement2)
print(query_output2)

query_statement2= f"SELECT DEP_NAME FROM {table_name2}"
query_output2 = pd.read_sql(query_statement2,conn)
print(query_statement2)
print(query_output2)

query_statement2 = f"SELECT COUNT(*) FROM {table_name2}"
query_output2 = pd.read_sql(query_statement2,conn)
print(query_statement2)
print(query_output2)

conn.close()

