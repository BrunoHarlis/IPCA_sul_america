from urllib import request
import pandas as pd
from datetime import date
from datetime import datetime
import os
import glob
import zipfile
import pyodbc


# -------------------- CRIAÇÃO PASTA --------------------

final_caminho = "IPCA.xlsx"
iso_pais = 'BRA'

# Cria pasta para arquivos auxiliares
caminho = './IPCA_' + iso_pais + '_Files'

# Verifica se o caminho espesificado existe
existe = os.path.exists(caminho)

if not existe: 
    os.makedirs(caminho)
    print("Nova parta criada!")

# -------------------- FIM CRIAÇÃO PASTA --------------------




# -------------------- DOWNLOAD ARQUIVOS --------------------

remote_url = 'https://ftp.ibge.gov.br/Precos_Indices_de_Precos_ao_Consumidor/IPCA/Serie_Historica/ipca_SerieHist.zip'
arquivo_local = caminho + '/BRA_IPCA.zip'
msg = request.urlretrieve(remote_url, arquivo_local)

# Verifique se arquivo for muito pequeno, provavelmente está com erro
statinfo = os.stat(arquivo_local)

if statinfo.st_size <= 9999:
    # Deletar arquivo
    os.remove(arquivo_local)
    print('Remove arquivo muito pequeno')
    print(msg) 
else:
    # Descompacta arquivo
    if os.path.exists(arquivo_local):    
        with zipfile.ZipFile(arquivo_local, 'r') as zip_ref:
            zip_ref.extractall(caminho)
    else:
        print("Erro ao descompactar...")

# -------------------- FIM DOWNLOAD ARQUIVOS --------------------




# -------------------- CRIAÇÃO DATAFRAME --------------------
# Importação dos arquivos baixados
nome_arquivo = max(glob.glob(caminho + "\*.xls"), key=os.path.getmtime)
sheet =  'Série Histórica IPCA'
df = pd.read_excel(io=nome_arquivo, sheet_name=sheet, skiprows = range(1, 8), usecols = "A:H")
df.columns = ['Ano','Mes','indice','No Mes','3 Meses','6 Meses', 'No Ano', '12 Meses']

# Limpeza e transformação dos dados
df = df.dropna(how='all',axis=0) 
df['Ano'].fillna(method='ffill', inplace=True )

df.drop(df[(df['Mes'].isnull())].index, inplace = True)
df.drop(df[(df['Ano'] == 'ANO')].index, inplace = True)
df.drop(df[(df['Mes'] == 'MÊS')].index, inplace = True)

lista_meses = {'JAN': 1, 'FEV': 2, 'MAR': 3, 'ABR' : 4, 'MAI' : 5, 'JUN' : 6, 'JUL' : 7, 'AGO' : 8, 'SET' : 9, 'OUT' : 10, 'NOV' : 11, 'DEZ' : 12}
df['Period'] = df.apply(lambda row: datetime(year=row.Ano, month=lista_meses[row.Mes], day=1).strftime('%Y-%m')  , axis=1)

df = df[['Ano','Mes','Period','indice','No Mes','3 Meses','6 Meses', 'No Ano', '12 Meses']]


df.insert(0, 'Country', 'BRA')
df = df.drop(columns='3 Meses')
df = df.drop(columns='6 Meses')
df = df.drop(columns='Ano')
df = df.drop(columns='Mes')
df.rename(columns = {'indice': 'IPC_Index', 'No Mes': 'IPC', 'No Ano' : 'IPCA', '12 Meses' : 'IPC12'}, inplace = True)

# Seleciona os ultimos 10 anos
qtd_anos = 10
data_inicio = datetime(date.today().year - qtd_anos, 1,1)
df = df.loc[pd.to_datetime(df['Period']) >= data_inicio]
# -------------------- FIM CRIAÇÃO DATAFRAME --------------------




# -------------------- SALVAR DF LOCALMENTE --------------------
sheet_name = iso_pais

if not os.path.isfile(final_caminho):
    df.to_excel(final_caminho, sheet_name=sheet_name)
    
with pd.ExcelWriter(final_caminho, 'openpyxl', mode='a',if_sheet_exists='replace') as writer:   
    if sheet_name in writer.book.sheetnames:
        idx = writer.book.sheetnames.index(sheet_name)        
        # remove [sheet_name]
        writer.book.remove(writer.book.worksheets[idx])
        # create an empty sheet [sheet_name] using old index
        writer.book.create_sheet(sheet_name, idx)        
    else:
        writer.book.create_sheet(sheet_name)
    
    writer.sheets = dict((ws.title, ws) for ws in writer.book.worksheets)      
    df.to_excel(writer, sheet_name, index=False)

print("IPCA " + sheet_name + " salvo com sucesso")
# -------------------- FIM SALVAR DF LOCALMENTE --------------------




# -------------------- SALVAR DF NO SQL SERVER --------------------
server = 'endereco_do_servidor'
database = 'Nome_do_Banco_de_dados'
username = 'nome_usuario'
password = 'minha_senha'

cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()

cursor.execute("DELETE FROM acct_etl.IndexIPCA WHERE Country = 'PER'")

for index, row in df.iterrows():
    cursor.execute("INSERT INTO acct_etl.IndexIPCA (Country, Period, IPC_Index, IPC, IPCA, IPC12) values(?,?,?,?,?,?)",
                   row.Country, row.Period+'-01', row.IPC_Index, row.IPC, row.IPCA, row.IPC12)

cnxn.commit()
cnxn.close()
# -------------------- FIM SALVAR DF NO SQL SERVER --------------------
