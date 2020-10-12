import kaggle
import pandas
import sys
import mysql.connector
from sqlalchemy import create_engine
from sklearn.cluster import KMeans


path = 'database'
columns = ["Report Number","Accident Date/Time", "Operator Name", "Pipeline/Facility Name", "Pipeline Location",
 "Pipeline Type", "Liquid Type", "Liquid Subtype", "Accident City", "Accident County", "Accident State", "Accident Latitude", "Accident Longitude","Cause Category",  "Unintentional Release (Barrels)",
  "Liquid Explosion", "Pipeline Shutdown","All Costs"]

dimentions = ["Operator Name", "Pipeline/Facility Name", "Pipeline Location",
 "Pipeline Type", "Liquid Type", "Liquid Subtype", "Accident City", "Accident County", "Accident State", "Cause Category",
 "Liquid Explosion", "Pipeline Shutdown"]

database_name = 'tcc_bi_puc'

def download():
    print("atualizando a base...")
    kaggle.api.authenticate()
    kaggle.api.dataset_download_files('usdot/pipeline-accidents', path=path, unzip=True)

#separa as colunas que realmente serão usadas
def update():
    print("lendo csv atual...")
    newPath = "%s/%s.csv"%(path, path)
    out = pandas.read_csv(newPath, low_memory=False, usecols=columns, index_col="Report Number")
    print("splitando csv processado...")
    mydb = mysql.connector.connect(
        host="localhost",
        user="root",
        password="<>"
    )
    
    engine = create_engine('mysql+mysqlconnector://root:padtec@localhost:3306/tcc_bi_puc', echo=False)

    mycursor = mydb.cursor()
    #apaga o banco
    sql = "DROP DATABASE IF EXISTS %s "%(database_name)
    mycursor.execute(sql)

    #cria o esquema do zero
    sql = "CREATE DATABASE IF NOT EXISTS %s "%(database_name)
    mycursor.execute(sql)

    for dimention in dimentions:
        dimention_norm = dimention.replace('/','_').replace(' ','_')
        sql = "CREATE TABLE %s.%s (%s VARCHAR(150) PRIMARY KEY)"%(database_name, dimention_norm, dimention_norm)
        mycursor.execute(sql)


    sql = "CREATE TABLE %s.accidents ('Report Number' BIGINT(20) PRIMARY KEY, 'Accident Date/Time' VARCHAR(60), 'Operator Name' VARCHAR(150)"%(database_name)
    sql = sql + ",'Pipeline/Facility Name' VARCHAR(150), 'Pipeline Location' VARCHAR(150), 'Pipeline Type' VARCHAR(150)"
    sql = sql + ",'Liquid Type' VARCHAR(150), 'Liquid Subtype' VARCHAR(150), 'Accident City' VARCHAR(150)"
    sql = sql + ",'Accident County' VARCHAR(150), 'Accident State' VARCHAR(150), 'Accident Latitude' FLOAT, 'Accident Longitude' FLOAT, 'Cause Category' VARCHAR(150), 'Unintentional Release' FLOAT"
    sql = sql + ",'Liquid Explosion' VARCHAR(5), 'Pipeline Shutdown' VARCHAR(5), 'All Costs' FLOAT, 'classification' INT)"
    sql = sql.replace("'", "`")
    mycursor.execute(sql)

 


    #add constraints
    sql = ""
    for dimention in dimentions:
        dimention_norm = dimention.replace('/','_').replace(' ','_')
        sql = "ALTER TABLE %s.accidents ADD CONSTRAINT FK_%s FOREIGN KEY ( `%s`) REFERENCES %s.%s(%s);"%(database_name,dimention_norm,dimention,database_name,dimention_norm,dimention_norm)
        mycursor.execute(sql)
        sql = ""
        unique = out[dimention].unique()
        #print(unique)
        sql = "REPLACE INTO %s.%s (%s) VALUES "%(database_name, dimention_norm, dimention_norm)
        for un in unique:
            if isinstance(un, str):
                un = un.replace('\"','$$')
            sql = sql +'("%s"),'%(un)
        sql = sql[:-1]
        sql = sql.replace('$$','')
        mycursor.execute(sql)
        mydb.commit()

        sql = "REPLACE INTO %s.%s (%s) VALUES ('None')"%(database_name, dimention_norm, dimention_norm)
        mycursor.execute(sql)
        mydb.commit()
        

    km = KMeans(n_clusters=3, init='k-means++', n_init=100, max_iter=500)

    #treina o k-means
    to_train = out.filter(['Pipeline Shutdown', 'Liquid Explosion'])
    
    to_train = to_train.apply(lambda col: pandas.factorize(col, sort=True)[0])
    to_train.insert(1, 'Unintentional Release (Barrels)', out['Unintentional Release (Barrels)'], allow_duplicates=True)
    to_train.reset_index(drop=True, inplace=True)
    print(to_train)
    y_km = km.fit_predict(to_train.to_numpy())
    c1=0
    c2=0
    c0=0
    for x in range(len(y_km)):
        if y_km[x] == 0:
            c0 = c0 + 1
        elif y_km[x] == 1:
            c1 = c1 + 1
        elif y_km[x] == 2:
            c2 = c2 + 1
        #print(y_km[x]), 
    print('---------------')
    print(c0)
    print(c1)
    print(c2)
    out['classification'] = y_km
    #popula a tabela fato
    out = out.replace('\"','$$')
    out['Pipeline/Facility Name'] = out['Pipeline/Facility Name'].str.replace('\"','')
    out = out.rename(columns={'Unintentional Release (Barrels)': 'Unintentional Release'})
    out.to_sql(con=engine, name="accidents", if_exists='append')


    


if len(sys.argv) == 2:
    print(sys.argv[0])
    if(sys.argv[1] == "download"):
        download()
    elif(sys.argv[1] == "update"):
        update()
        
else: print("função inválida")
