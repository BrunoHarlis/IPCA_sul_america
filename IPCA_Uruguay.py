from urllib import request
import pandas as pd
from datetime import date
from datetime import datetime
import os
import glob
import pyodbc


# -------------------- DOWNLOAD DOS ARQUIVOS --------------------
iso_pais = 'URU'

# Cria pasta para arquivos auxiliares
caminho = './IPCA_' + iso_pais + '_Files'

# Verifica se o caminho foi criado ou não
existe = os.path.exists(caminho)

if not existe: 
    os.makedirs(caminho)
    print("\nNova pasta criada!")

print('\nIniciando o download dos dados')

remote_url = 'https://www.ine.gub.uy/c/document_library/get_file?uuid=2e92084a-94ec-4fec-b5ca-42b40d5d2826&groupId=10181'

# Define o local onde os arquivos serão salvos 
arquivo_local = caminho + '/' + iso_pais + 'IPCA.xls'

# Download dos arquivos
msg = request.urlretrieve(remote_url, arquivo_local)
print(f'Arquivos baixados em: {msg}')

# Verifique se arquivo for muito pequeno, provavelmente está com erro
statinfo = os.stat(arquivo_local)

if statinfo.st_size <= 9999:
    # Deletar arquivo
    os.remove(arquivo_local)
    print('Removendo arquivo muito pequeno.')
# -------------------- FIM DOWNLOAD DOS ARQUIVOS --------------------




# -------------------- TRANSFORMAÇÃO DOS DADOS --------------------
print('\nIniciando a transformação do dados.')

nome_arquivo = max(glob.glob(caminho + "\*.xls"), key=os.path.getmtime)

sheet =  'IPC_Cua 1'
df = pd.read_excel(io=nome_arquivo, sheet_name=sheet, skiprows = range(1, 10), usecols = "A:E")

# Define nome das colunas
df.columns = ['Ano_Mes','IPC_Index','IPC','IPCA','IPC12']

# Exclui linhas em branco
df = df.dropna(how='all',axis=0) 
df = df[df['IPC_Index'].notna()]

df['Period'] = df.apply(lambda row: pd.to_datetime(row.Ano_Mes).strftime('%Y-%m')  , axis=1)


# Renomeia e ordena colunas 
df.insert(0, 'Country', iso_pais)

df = df.drop('Ano_Mes', 1)
df = df[['Country','Period','IPC_Index','IPC','IPCA','IPC12']]


# Filtra dados dos últimos 10 anos
qtd_anos = 10
data_inicio = datetime(date.today().year - qtd_anos, 1,1)
df = df.loc[pd.to_datetime(df['Period']) >= data_inicio]

print('Transformação dos dados finalizada com sucesso!')
# -------------------- FIM TRANSFORMAÇÃO DOS DADOS --------------------




# -------------------- SALVAR DF LOCALMENTE --------------------
print('\nSalvando dados localmente.')

final_caminho = "IPCA.xlsx"
sheet_name = 'PAR'

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

print("IPCA " + sheet_name + " salvo com sucesso!")
# -------------------- FIM SALVAR DF LOCALMENTE --------------------




# -------------------- SALVAR DF NO SQL SERVER --------------------
print('\nCarregando os dados no SQL Server.')

server = 'endereco_do_servidor'
database = 'nome_do_banco_de_dados'
username = 'nome_usuario'
password = 'minha_senha'

cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()

cursor.execute("DELETE FROM acct_etl.IndexIPCA WHERE Country = 'URU'")

for index, row in df.iterrows():
    cursor.execute("INSERT INTO acct_etl.IndexIPCA (Country, Period, IPC_Index, IPC, IPCA, IPC12) values(?,?,?,?,?,?)",
                   row.Country, row.Period+'-01', row.IPC_Index, row.IPC, row.IPCA, row.IPC12)

cnxn.commit()
cnxn.close()

print('Tabela carregada no SQL Server com sucesso!')
# -------------------- FIM SALVAR DF NO SQL SERVER --------------------