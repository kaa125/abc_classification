import pandas as pd
import numpy as np
import inventorize3 as inv
from google.cloud import bigquery
import pandas as pd
import datetime
from sqlalchemy import create_engine, exc, text
import os
import json

# Load configuration from a JSON file
f = open('./configurations.json')
config = json.load(f)

# Set Google Cloud credentials from the configuration
os.environ["GOOGLE_APPLICATION_CREDENTIALS"] = config["GOOGLE_APPLICATION_CREDENTIALS"]
print(os.environ["GOOGLE_APPLICATION_CREDENTIALS"])

# Initialize the BigQuery client
client = bigquery.Client()

# Define the BigQuery table ID
table_id = "data-warehouse.dbt.stg_orders_denorm"

# Get the schema (column names and data types) of the specified table
table = client.get_table(table_id)
schema = [{'Column': field.name, 'Data Type': field.field_type} for field in table.schema]
df = pd.DataFrame(schema, columns=['Column', 'Data Type'])
print("Table schema: ", df)

# Define SQL queries to filter data
# Filter for the current month
where = "EXTRACT(MONTH FROM OrderDeliveredAt) = EXTRACT(MONTH FROM CURRENT_DATE())"
BQquery = f"SELECT City, MasterOrderID, OrderID, FulfilmentMode, OrderCreatedAt, OrderDeliveredAt, UserID, updated_at, OrderStatus, OrderType, SKU_ID, SKU_Name, PRODUCT_ID, SubCategory, Category, DeliveredGMV FROM {table_id} WHERE {where}"
currentmonthbigquerytable = client.query(BQquery).to_dataframe()

# Filter for the current week within the current month
where = "EXTRACT(WEEK FROM OrderDeliveredAt) = EXTRACT(WEEK FROM CURRENT_DATE()) AND EXTRACT(MONTH FROM OrderDeliveredAt) = EXTRACT(MONTH FROM CURRENT_DATE())"
currentweekbigquerytable = client.query(BQquery).to_dataframe()

# Get the current datetime
now = datetime.datetime.now()

# Current Month Based Business and Drop Shipping Product and SKU Classification
base_business = ['Fulfiled By', 'WaaS']
drop_shipping = ['Drop Shipping']

# Filter data based on fulfillment modes
base_business_df = currentmonthbigquerytable[currentmonthbigquerytable['FulfilmentMode'].isin(base_business)]
drop_shipping_df = currentmonthbigquerytable[currentmonthbigquerytable['FulfilmentMode'].isin(drop_shipping)]

# Data preprocessing for base business
base_business_df = base_business_df.drop_duplicates()
base_business_df = base_business_df.dropna()
base_business_df = base_business_df[base_business_df['DeliveredGMV'] > 0]
base_business_df = base_business_df.rename(columns={'Category': 'Categ'})

# Group and aggregate data by category, subcategory, and SKU name
data = base_business_df.groupby(['Categ']).agg(DeliveredGMV=('DeliveredGMV', np.sum)).reset_index()
data1 = base_business_df.groupby(['SubCategory']).agg(DeliveredGMV=('DeliveredGMV', np.sum)).reset_index()
data2 = base_business_df.groupby(['SKU_Name']).agg(DeliveredGMV=('DeliveredGMV', np.sum)).reset_index()

# Apply ABC analysis to SKU data
data_abc1 = inv.ABC(data2[['SKU_Name', 'DeliveredGMV']])
data_abc1['updated_at'] = pd.to_datetime(now)

# Apply ABC analysis to subcategory data
data_abc2 = inv.ABC(data1[['SubCategory', 'DeliveredGMV']])
data_abc2['updated_at'] = pd.to_datetime(now)

# Apply ABC analysis to category data
data_abc3 = inv.ABC(data[['Categ', 'DeliveredGMV']])
data_abc3['updated_at'] = pd.to_datetime(now)

# Print and store ABC classification results for monthly base business
print("Monthly Base Business SKU wise Classification")
print(data_abc1)
data_abc1.to_gbq('data-warehouse.dbt.monthly_basebusiness_SKU_wise_classification', if_exists='append')
data_abc1.Category.value_counts()
del data_abc1

print("Monthly Base Business SubCategory wise Classification")
print(data_abc2)
data_abc2.to_gbq('data-warehouse.dbt.monthly_basebusiness_SubCategory_wise_classification', if_exists='append')
data_abc2.Category.value_counts()
del data_abc2

print("Monthly Base Business Category wise Classification")
print(data_abc3)
data_abc3.to_gbq('data-warehouse.dbt.monthly_basebusiness_Category_wise_classification', if_exists='append')
data_abc3.Category.value_counts()
del data_abc3

# Clean up dataframes
del data
del data1
del data2

# Process Drop Shipping data
drop_shipping_df = drop_shipping_df.drop_duplicates()
drop_shipping_df = drop_shipping_df.dropna()
drop_shipping_df = drop_shipping_df[drop_shipping_df['DeliveredGMV'] > 0]
drop_shipping_df = drop_shipping_df.rename(columns={'Category': 'Categ'})

# Group and aggregate data by category, subcategory, and SKU name
data = drop_shipping_df.groupby(['Categ']).agg(DeliveredGMV=('DeliveredGMV', np.sum)).reset_index()
data1 = drop_shipping_df.groupby(['SubCategory']).agg(DeliveredGMV=('DeliveredGMV', np.sum)).reset_index()
data2 = drop_shipping_df.groupby(['SKU_Name']).agg(DeliveredGMV=('DeliveredGMV', np.sum)).reset_index()

# Apply ABC analysis to SKU data for drop shipping
data_abc1 = inv.ABC(data2[['SKU_Name', 'DeliveredGMV']])
data_abc1['updated_at'] = pd.to_datetime(now)

# Apply ABC analysis to subcategory data for drop shipping
data_abc2 = inv.ABC(data1[['SubCategory', 'DeliveredGMV']])
data_abc2['updated_at'] = pd.to_datetime(now)

# Apply ABC analysis to category data for drop shipping
data_abc3 = inv.ABC(data[['Categ', 'DeliveredGMV']])
data_abc3['updated_at'] = pd.to_datetime(now)

# Print and store ABC classification results for monthly drop shipping
print("Monthly Drop Shipping SKU wise Classification")
print(data_abc1)
data_abc1.to_gbq('data-warehouse.dbt.monthly_dropshipping_SKU_wise_classification', if_exists='append')
data_abc1.Category.value_counts()

print("Monthly Drop Shipping SubCategory wise Classification")
print(data_abc2)
data_abc2.to_gbq('data-warehouse.dbt.monthly_dropshipping_Subcategory_wise_classification', if_exists='append')
data_abc2.Category.value_counts()

print("Monthly Drop Shipping Category wise Classification")
print(data_abc3)
data_abc3.to_gbq('data-warehouse.dbt.monthly_dropshipping_Category_wise_classification', if_exists='append')
data_abc3.Category.value_counts()

del data_abc1
del data_abc2
del data_abc3
del data
del data1
del data2
