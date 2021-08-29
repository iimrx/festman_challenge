try:
    import time
    import pandas as pd
    from sqlalchemy import create_engine
except Exception as e:
    print(f'Error while importing package!!\n{e}')
    
#importing data
df = pd.read_csv('./dataset/original/festman_ecommerce.csv')
#changing column names to be as regex
df.columns = ['order_date','order_id','delivery_date','customer_id','customer_age','customer_gender','location','zone',
 'delivery_type','product_category','sub_category','product','unit_price','shipping_fee','order_quantity',
 'sale_price','status','reason','rating']

#droping an wanted columns
df.drop(['order_id','customer_id', 'zone'], axis=1, inplace=True)
#converting date column from pandas object to datetime
df[['order_date', 'delivery_date']] = df[['order_date', 'delivery_date']].apply(pd.to_datetime, format="%m/%d/%Y")

#filling nan values with 'Not Added'
df['reason'].fillna('Not Added', inplace=True)
#checking if gender column has null values
if (df['customer_gender'].isnull().any() == True):
    print('gg!') #it will give us nothing because there is no null values!
    
#extracting returned by males
male_returned = df.loc[(df['customer_gender'] == 'M') & (df['customer_age'] > 18) & (df['customer_age'] <=30) & (df['status'] == 'Returned')]
#extracting returned by females
female_returned = df.loc[(df['customer_gender'] == 'F') & (df['customer_age'] > 18) & (df['customer_age'] <=30) & (df['status'] == 'Returned')]
#concating/merging both males/females returns
all_returned = pd.concat([male_returned,female_returned])

#connecting to a database 'PostgreSQL'
# try: #try connecting to the database
#     conn = create_engine('postgresql+psycopg2://adminz1:Zaak1234@35.240.187.150:5432/festman')
# except Exception as e: #if it doesn't connect raise an error!
#     raise e
# else: #else print success connection!!
#     print(f'[🔥] Connecting to Database ... \n{conn}\
#           \n[✔] Connected Successfully!')
    
#transfering data to our database using pandas 'to_sql()'
# tran_start_time = time.time() #calc the starting time of the transfering
# all_returned.to_sql('festmanchallenge', con=conn, if_exists='replace', index=False)
# tran_end_time = time.time() #calc the end time of the transfaring
#printing the time it takes to transfer all data to the database
# pringt(f'Time to Transfer all Data: {round(tran_end_time - tran_start_time, 2)}sec')
#saving dataset to file for other uses
df.to_csv('./dataset/created/festman_ecommerce_cleaned.csv', index=False) #cleaned verion of the main dataset
all_returned.to_csv('./dataset/created/festman_ecommerce_returned.csv', index=False) #data of the returned only
