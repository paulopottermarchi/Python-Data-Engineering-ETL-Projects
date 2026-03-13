from bs4 import BeautifulSoup
import requests
import pandas as pd 
import numpy as np
from datetime import datetime
import sqlite3

url = "https://web.archive.org/web/20230908091635/https://en.wikipedia.org/wiki/List_of_largest_banks"
table_attribs = ["Name","MC_USD_Billion"]
db_name = "Banks.db"
table_name = "Largest_banks"
log_file = "code_log.txt"
csv_path = "./Largest_banks_data.csv"

def log_progress(message):
    time_stamp = "%Y-%h-%d-%H:%M:%S"
    now = datetime.now()
    time_stamp = now.strftime(time_stamp)
    with open("code_log.txt","a") as f:
        f.write(time_stamp + "," + message +"\n")

def extract(url,table_attribs):
    html_page = requests.get(url).text
    data = BeautifulSoup(html_page,"html.parser")
    df = pd.DataFrame(columns = table_attribs)

    tables = data.find_all("tbody")
    rows = tables[0].find_all("tr")

    for row in rows:
        col = row.find_all("td")
        if len(col)!= 0:
            data_dict = {
                "Name":col[1].get_text(strip=True),
                "MC_USD_Billion": float(col[2].text.strip()),
            }
            df1 = pd.DataFrame(data_dict, index = [0])
            df = pd.concat([df,df1], ignore_index = True)

    return df

def transform(df, csv_path):
    exchange_df = pd.read_csv(csv_path)
    exchange_rate = exchange_df.set_index("Currency")["Rate"].to_dict()

    df["MC_GBP_Billion"] = [np.round(x * exchange_rate["GBP"],2) for x in df["MC_USD_Billion"]]
    df["MC_EUR_Billion"] = [np.round(x * exchange_rate["EUR"],2) for x in df["MC_USD_Billion"]]
    df["MC_INR_Billion"] = [np.round(x * exchange_rate["INR"],2) for x in df["MC_USD_Billion"]]

    return df 

def load_to_csv(df,output_path):
    df.to_csv(output_path, index = False)

def load_to_db(df,sql_connection,table_name):
    df.to_sql(table_name,sql_connection,if_exists = "replace",index=False)

def run_query(query_statement,sql_connection):
    print(query_statement)
    query_output = pd.read_sql(query_statement,sql_connection)
    print(query_output)

log_progress("Preliminaries complete. Initiating ETL process.")
extracted_data = extract(url,table_attribs)
log_progress("Data extraction complete. Initiating Transformation process.")

transformed_data = transform(extracted_data, "./exchange_rate.csv")
log_progress("Data transformation complete. Initiating loading process.")
load_to_csv(transformed_data,csv_path)
log_progress("Data saved to CSV file.")

sql_connection = sqlite3.connect(db_name)
log_progress("SQL Connection initiated.")
load_to_db(transformed_data,sql_connection,table_name)
log_progress("Data loaded to Database as table. Running the query.")
run_query(f"SELECT * FROM {table_name}", sql_connection)
run_query(f"SELECT AVG(MC_GBP_Billion) FROM {table_name}",sql_connection)
run_query(f"SELECT Name FROM {table_name} LIMIT 5",sql_connection)
log_progress("Process Complete.")
sql_connection.close()

