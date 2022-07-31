
from urllib import request
import pandas as pd
from datetime import date
from datetime import datetime
import os
import pyodbc


# -------------------- CRIAÇÃO PASTA --------------------
iso_pais = 'CHI'

# Cria pasta para arquivos auxiliares
caminho = './IPCA_' + iso_pais + '_Files'

# Verifica se o caminho espesificado existe
existe = os.path.exists(caminho)

if not existe: 
    os.makedirs(caminho)
    print(f'Nova pasta {caminho} criada!')
# -------------------- FIM CRIAÇÃO PASTA --------------------




# -------------------- DOWNLOAD ARQUIVOS --------------------
# Anterior a 2018   
remote_url_old = 'https://www.ine.cl/docs/default-source/%C3%ADndice-de-precios-al-consumidor/cuadros-estadisticos/base-2013/series-historicas-de-enero-2014-a-diciembre-2018/nuevo-formato-ipc-base-2013-xls.xlsx?sfvrsn=e22a63e6_2'

# Atual    
remote_url = 'https://www.ine.cl/docs/default-source/%C3%ADndice-de-precios-al-consumidor/cuadros-estadisticos/base-2018/series-de-tiempo/ipc.xls.xlsx?sfvrsn=c73e33d4_10'

# Define the local filename to save data
arquivo_local = caminho + '/' + iso_pais + '_IPCA.xlsx'
arquivo_local_antigo = caminho + '/' + iso_pais + '_IPCA_OLD.xlsx'

msg = request.urlretrieve(remote_url, arquivo_local)
msg2 = request.urlretrieve(remote_url_old, arquivo_local_antigo)

print(f'Download do arquivo {arquivo_local} concluido.')
print(f'Download do arquivo {arquivo_local_antigo} concluido.')


# Verifique se arquivo for muito pequeno, provavelmente está com erro
statinfo = os.stat(arquivo_local)

if statinfo.st_size <= 9999:
    # Deletar arquivo
    os.remove(arquivo_local)
    print('Remove arquivo muito pequeno')
    print(msg) 
# -------------------- FIM DOWNLOAD ARQUIVOS --------------------




# -------------------- CRIAÇÃO DATAFRAME --------------------
# Define qual arquivo será importado
nome_arquivo = arquivo_local
nome_arquivo_antigo = arquivo_local_antigo


sheet =  'IPC Base 2013=100'

df_antigo = pd.read_excel(io=nome_arquivo_antigo, sheet_name=sheet, skiprows = range(1, 2), usecols = "A:P", header=1)
df_antigo = df_antigo.loc[df_antigo['Glosa'] == 'IPC General']

# Exclui linhas em branco
df_antigo = df_antigo.dropna(how='all',axis=0) 
df_antigo = df_antigo.dropna(how='all',axis=1) 

df_antigo = df_antigo.drop(columns='Glosa')

df_antigo.rename(columns = {'Año':'Ano','Índice': 'IPC_Index'}, inplace = True)

df_antigo['Period'] = df_antigo.apply(lambda row: datetime(year=int(row.Ano), month=int(row.Mes), day=1).strftime('%Y-%m')  , axis=1)

df_antigo = df_antigo.drop(columns='Ano')
df_antigo = df_antigo.drop(columns='Mes')


df_antigo = df_antigo.rename(columns={df_antigo.columns[1]: 'IPC'})
df_antigo = df_antigo.rename(columns={df_antigo.columns[2]: 'IPCA'})
df_antigo = df_antigo.rename(columns={df_antigo.columns[3]: 'IPC12'})
#df = df_antigo.drop(columns={df.columns[4], df.columns[5], df.columns[6]})

df_antigo.insert(0, 'Country', 'CHI')
df_antigo = df_antigo[['Country','Period','IPC_Index','IPC','IPCA','IPC12']]


sheet =  'IPC Base 2018=100'

df_novo = pd.read_excel(io=nome_arquivo, sheet_name=sheet, skiprows = range(1, 3), usecols = "A:P", header=1)
df_novo = df_novo.loc[df_novo['Glosa'] == 'IPC General']

# Exclui linhas em branco
df_novo = df_novo.dropna(how='all',axis=0) 
df_novo = df_novo.dropna(how='all',axis=1) 

df_novo = df_novo.drop(columns='División')
df_novo = df_novo.drop(columns='Grupo')
df_novo = df_novo.drop(columns='Clase')
df_novo = df_novo.drop(columns='Subclase')
df_novo = df_novo.drop(columns='Glosa')
df_novo = df_novo.drop(columns='Ponderación')
df_novo = df_novo.drop(columns='Producto')

df_novo.rename(columns = {'Año':'Ano','Índice': 'IPC_Index'}, inplace = True)

df_novo['Period'] = df_novo.apply(lambda row: datetime(year=row.Ano, month=row.Mes, day=1).strftime('%Y-%m')  , axis=1)
df_novo = df_novo.drop(columns='Ano')
df_novo = df_novo.drop(columns='Mes')

df_novo = df_novo.rename(columns={df_novo.columns[1]: 'IPC'})
df_novo = df_novo.rename(columns={df_novo.columns[2]: 'IPCA'})
df_novo = df_novo.rename(columns={df_novo.columns[3]: 'IPC12'})
#df = df.drop(columns={df.columns[4], df.columns[5], df.columns[6]})

df_novo.insert(0, 'Country', 'CHI')
df_novo = df_novo[['Country','Period','IPC_Index','IPC','IPCA','IPC12']]

dfframes = [df_antigo, df_novo]
df = pd.concat(dfframes)


# Seleciona os ultimos 10 anos
qtd_anos = 10
startDate = datetime(date.today().year - qtd_anos, 1,1)

df = df.loc[pd.to_datetime(df['Period']) >= startDate]

# Substituindo todos os valores NAN por 0 (zero)
df.fillna(value= 0, inplace= True)
# -------------------- FIM CRIAÇÃO DATAFRAME --------------------




# -------------------- SALVAR DF LOCALMENTE --------------------
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
server = 'endereco_do_servidor'
database = 'nome_do_banco_de_dados'
username = 'nome_usuario'
password = 'minha_senha'

cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()

cursor.execute("DELETE FROM acct_etl.IndexIPCA WHERE Country = 'CHI'")

for index, row in df.iterrows():
    cursor.execute("INSERT INTO acct_etl.IndexIPCA (Country, Period, IPC_Index, IPC, IPCA, IPC12) values(?,?,?,?,?,?)",
                   row.Country, row.Period+'-01', row.IPC_Index, row.IPC, row.IPCA, row.IPC12)

cnxn.commit()
cnxn.close()

print('Tabela carregada no SQL Server com sucesso!')
# -------------------- FIM SALVAR DF NO SQL SERVER --------------------