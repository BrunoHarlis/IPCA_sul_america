from shutil import rmtree
from urllib import request
from datetime import date, datetime
import pandas as pd
import os
import zipfile
import pyodbc


# -------------------- FUNÇÃO PARA LER ARQUIVOS CSV --------------------
def transform_DF(path_dataset, name_column):
    columns=['MES', 'Enero', 'Febrero','Marzo','Abril','Mayo','Junio',
            'Julio','Agosto','Septiembre','Octubre','Noviembre','Diciembre']

    df = pd.read_csv(path_dataset, skiprows=(10), encoding= "ISO-8859-1", header=0,
                        names=columns, usecols=columns)

    
    # Dropa todos os valores não numéricos da coluna MES
    df = df.dropna(subset=['MES'])
    df = df[df['MES'].str.isnumeric()]


    # Bring only 10 last years and filter NaN Values in 0
    df = df[-11:] 
    df = df.fillna(0)


    # Convert 'MES' in object to transpose columns/rows 
    df['MES'] = df['MES'].astype(object)
    df = df.set_index('MES').T


    # Reset index to avoid problemns when create Period column 
    df.reset_index(inplace=True)
    df = df.rename(columns = {'index':'MES'})


    # Criar as colunas MES, YEAR e coluna referente ao IPC
    df = df.melt(id_vars='MES', var_name='YEAR', value_name=name_column)


    # Criar a coluna Period (une as colunas YEAR e MES convertendo para formato numérico)
    df['YEAR'] = df['YEAR'].astype(int)
    df['Period'] = df.apply(lambda row: datetime(year=row.YEAR, month=month_string[row.MES], day=1).strftime('%Y-%m'), axis=1)
    df.drop(['MES', 'YEAR'],axis=1,inplace=True)

    return df
# -------------------- FIM FUNÇÃO PARA LER ARQUIVOS CSV --------------------




# -------------------- DOWNLOAD DOS ARQUIVOS --------------------
print('\nIniciando o download dos dados')

year = date.today().year
month = date.today().month - 1


# Dicionário dos meses com chave em string
month_string = {'Enero':1, 'Febrero':2, 'Marzo':3, 'Abril':4, 'Mayo':5, 'Junio':6, 'Julio':7, 'Agosto':8,
               'Septiembre':9,'Octubre':10, 'Noviembre':11, 'Diciembre':12}


# Dicionário dos meses com chave numérica
month_numeric = {1:'Enero', 2:'Febrero', 3:'Marzo', 4:'Abril', 5:'Mayo', 6:'Junio', 7:'Julio', 8:'Agosto',
               9:'Septiembre', 10:'Octubre', 11:'Noviembre', 12:'Diciembre'}


# HTML/WEBSITE para baixar dados
html = f'https://www.ecuadorencifras.gob.ec/documentos/web-inec/Inflacion/{year}/{month_numeric[month]}-{year}/Tabulados%20y%20series%20historicas_CSV.zip'


# Variáveis para criar o caminho onde será baixado os dados
iso_pais = 'ECU'
path = './IPCA_' + iso_pais + '_Files'


# Verifica se o caminho foi criado ou não
isExist = os.path.exists(path)

if not isExist:
    os.mkdir(path)
    print("\nNova pasta criada!")


# This cell will take a little time. The zip is a very heavy file.
arquivo_local = path + '/ECU.zip'


# Download dos arquivos
msg = request.urlretrieve(html, arquivo_local)
print(f'Arquivos baixados em: {msg}')
# -------------------- FIM DOWNLOAD DOS ARQUIVOS --------------------




# -------------------- DESCOMPACTAÇÃO DOS ARQUIVOS BAIXADOS --------------------
print('\nIniciando descompactação dos arquivos.')

def unzipFile(arquivo_local, path):

    # Unzip file
    if os.path.exists(arquivo_local):
        with zipfile.ZipFile(arquivo_local, 'r') as zip_ref:
            zip_ref.extractall(path)     
    else:
        print("Something Is wrong")


# Primeira descompactação
unzipFile(arquivo_local, path)
os.remove(arquivo_local)


# Segunda descompactação
arquivo_local = path + '/Tabulados y series historicas_CSV'
paths = [os.path.join(arquivo_local, name) for name in os.listdir(arquivo_local)]
unzipFile(paths[3], path)
rmtree(arquivo_local)

print('Arquivos descompactados com sucesso!')
# -------------------- FIM DESCOMPACTAÇÃO DOS ARQUIVOS BAIXADOS --------------------




# -------------------- TRANSFORMAÇÃO DOS DADOS --------------------
print('\nIniciando a transformação do dados.')

# Cria uma lista com os caminhos dos arquivos CSV
path_list = os.listdir(path)
path = path +'/'+ path_list[0]
path_datasets = [os.path.join(path, name) for name in os.listdir(path)]


# Dataframes temporários para realisar o merge
df_indice = transform_DF(path_datasets[0], 'IPC_Index')
df_ipc = transform_DF(path_datasets[1], 'IPC')
df_ipc12 = transform_DF(path_datasets[2], 'IPC12')
df_ipca = transform_DF(path_datasets[3], 'IPCA')


# Unir DataFrames
df = pd.merge(df_indice, df_ipc, how="left", on=["Period"])
df = pd.merge(df, df_ipca, how="left", on=["Period"])
df = pd.merge(df, df_ipc12, how="left", on=["Period"])             


# Criando a coluna Country com valor ECU
df.insert(0, 'Country', 'ECU')


# Reoder colunas
df = df[['Country','Period','IPC_Index','IPC','IPCA','IPC12']]


# Drop datas futuras com valores ainda inexistentes
df.drop(df.tail(12 - month).index, inplace=True)

print('Transformação dos dados finalizada com sucesso!')
# -------------------- FIM TRANSFORMAÇÃO DOS DADOS --------------------




# -------------------- SALVAR DF LOCALMENTE --------------------
print('\nSalvando dados localmente.')

final_caminho = "IPCA.xlsx"
sheet_name = iso_pais

if not os.path.isfile(final_caminho):
    df.to_excel(final_caminho, sheet_name=sheet_name)
    
with pd.ExcelWriter(final_caminho, 'openpyxl', mode='a',if_sheet_exists='replace') as writer:   
    if sheet_name in writer.book.sheetnames:
        idx = writer.book.sheetnames.index(sheet_name)        
        # remove [sheet_name]
        writer.book.remove(writer.book.worksheets[idx])
        # crie uma planilha vazia [sheet_name] usando o índice antigo
        writer.book.create_sheet(sheet_name, idx)        
    else:
        writer.book.create_sheet(sheet_name)
    
    writer.sheets = dict((ws.title, ws) for ws in writer.book.worksheets)      
    df.to_excel(writer, sheet_name, index=False)

print("IPCA " + sheet_name + " salvo com sucesso")
# -------------------- FIM SALVAR DF LOCALMENTE --------------------




# -------------------- SALVAR DF NO SQL SERVER --------------------
print('\nCarregando os dados no SQL Server.')

server = 'endereco_do_servidor'
database = 'nome_do_banco_de_dados'
username = 'nome_usuario'
password = 'minha_senha'

cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()

cursor.execute("DELETE FROM acct_etl.IndexIPCA WHERE Country = 'ECU'")

for index, row in df.iterrows():
    cursor.execute("INSERT INTO acct_etl.IndexIPCA (Country, Period, IPC_Index, IPC, IPCA, IPC12) values(?,?,?,?,?,?)",
                   row.Country, row.Period+'-01', row.IPC_Index, row.IPC, row.IPCA, row.IPC12)

cnxn.commit()
cnxn.close()

print('Tabela carregada no SQL Server com sucesso!')
# -------------------- FIM SALVAR DF NO SQL SERVER --------------------