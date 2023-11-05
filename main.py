#Importing necessary libraries and modules
from requests.api import head
import pandas as pd
import numpy as np
import csv
import requests
import json
import psycopg2

#Creating connection to local postgresql database
def connect():
    def create_connection():
        conn = psycopg2.connect(
            host="localhost",
            database="barbershop",
            user="postgres",
            password="Quockhanh2004@"
        )
        return conn
    conn = create_connection()
    cur = conn.cursor()
    return cur, conn

#USING THE REQUEST MODULE TO EXTRACT DATA FROM THE API
def extract():
    url = "https://api.coincap.io/v2/assets"
    headers = {
        'Application': 'application/csv'
        'Content-Type: application/csv'
    }
    response = requests.get(url, headers=headers)
    myjson = response.json()   
    return myjson

#Transforming data into lists
def transform(myjson):
    crypto_data = []
    Name =[]
    Price = []
    for x in myjson['data']:
        listing = [x['symbol'], x['rank'], x['priceUsd']]
        crypto_data.append(listing)
    for x in myjson['data']:
        Name.append(x['symbol'])
        Price.append(x['priceUsd'])
    return crypto_data

#Loading data into the database
def load(crypto_data):
    cur, conn = connect()
    cur.execute("DROP TABLE IF EXISTS crypto")
    cur.execute("CREATE TABLE crypto (symbol varchar(10), rank int, price float)")
    for x in crypto_data:
        cur.execute("INSERT INTO crypto (symbol, rank, price) VALUES (%s, %s, %s)", (x[0], x[1], x[2]))
    conn.commit()
    print("Data loaded successfully")

def query():
    cur, conn = connect()
    cur.execute("SELECT * FROM crypto")
    rows = cur.fetchall()
    for row in rows:
        print(row)
    conn.close()

if __name__ == "__main__":
    myjson = extract()
    data_transformed = transform(myjson)
    data_loaded = load(data_transformed)
    query()