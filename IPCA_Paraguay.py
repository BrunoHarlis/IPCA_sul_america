from typing import ClassVar
import pyodbc
import requests
import pandas as pd
from datetime import date, datetime
from bs4 import BeautifulSoup
import os


# -------------------- WEB SCRAPING DOS DADOS --------------------
# Define o intervalo anual que será usado (10 anos)
ano_fim = date.today().year
ano_inicio = ano_fim - 10

# Dataframe principal que receberá todos os dados
df = pd.DataFrame()

print('\nIniciando Web Scraping.')
while ano_inicio <= ano_fim:

    # Define a URL que será usada para raspagem dos dados e verifica se é válida
    # Caso invalida, avisa o erro, caso válida, realisa a raspagem
    url = requests.get(f'https://datosmacro.expansion.com/ipc-paises/paraguay?sector=IPC+General&sc=IPC-IG&anio={ano_inicio}')

    if url.status_code != 200:
        print(f"----- ERRO na URL do ano {ano_inicio} ------\n\n")
        break
    else:
        content = url.content
        soup = BeautifulSoup(content, 'html.parser')
        tabela = soup.find("table", id="tb1_()}")
        tabela_str = str(tabela)
        
        df_temp = pd.read_html(tabela_str, index_col= None)[0]
        df_temp = df_temp.drop(df_temp.index[[12,13]])
        df_temp[['MES', 'ANO']] = df_temp['Unnamed: 0'].str.split(' ', expand= True)

        df = pd.concat([df, df_temp], ignore_index=True)
    
    ano_inicio += 1

print('Web Scraping finalizado com sucesso!')
# -------------------- FIM WEB SCRAPING DOS DADOS --------------------




# -------------------- TRANSFORMAÇÃO DOS DADOS --------------------
print('\nIniciando a transformação do dados.')

# Criar coluna 'Period' com o ano e mês
df = df.astype({'ANO':'int64'})

arrayMeses ={'Enero': 1, 'Febrero': 2, 'Marzo': 3, 'Abril' : 4, 'Mayo' : 5, 'Junio' : 6,
            'Julio' : 7, 'Agosto' : 8, 'Septiembre' : 9, 'Octubre' : 10, 'Noviembre' : 11,
            'Diciembre' : 12}

df['Period'] = df.apply(lambda linha: datetime(year=linha.ANO, month=arrayMeses[linha.MES], day=1).strftime('%Y-%m'), axis=1)


# Exclui linhas duplicadas
df = df.drop_duplicates(subset = ['Unnamed: 0'], keep = 'first').reset_index()


# Exclui colunas irrelevantes do DF
df = df.drop(columns='index')
df = df.drop(columns='Interanual.1')
df = df.drop(columns='Acum. desde Enero.1')
df = df.drop(columns='Variación mensual.1')
df = df.drop(columns='Unnamed: 0')
df = df.drop(columns='MES')
df = df.drop(columns='ANO')

df.insert(0, 'Country', 'PAR')

# Renomeia colunas do DF 
df = df.rename(columns={'Interanual':'IPC12', 
                        'Acum. desde Enero':'IPCA', 
                        'Variación mensual':'IPC'})


# Remover símbolo de porcentagem e vírgula das colunas
def corrigir_nomes(numero):
    numero = numero.replace('%', '').replace(',', '.')
    return numero

df['IPC12'] = df['IPC12'].apply(corrigir_nomes)
df['IPCA'] = df['IPCA'].apply(corrigir_nomes)
df['IPC'] = df['IPC'].apply(corrigir_nomes)

# Converter tipo de dado das colunas
# df['Period'] = pd.to_datetime(df['Period'], format = '%Y-%m', infer_datetime_format=False)

df['IPC12'] = pd.to_numeric(df['IPC12'], errors= 'coerce')
df['IPCA'] = pd.to_numeric(df['IPCA'], errors='coerce')
df['IPC'] = pd.to_numeric(df['IPC'], errors='coerce')

# Ordenar DF por data
df = df.sort_values(by=['Period'])


# Adicionando coluna IPC_Index
'''
O valor da variável ipc_index é a base para calcular o index do DF do Paraguay.
Ela deve ser preenxida manualmente com o mês de Janeiro de 10 anos atrás.
O valor pode ser obtido baixando o pdf disponibilizado no site do Banco Central
do Paraguay (https://www.bcp.gov.py/informe-de-inflacion-mensual-i362)
'''
ipc_index = 124.5  # Valor do IPC Index de Janeiro de 2012
num_linhas_df = len(df.index)
array_index = [ipc_index]


# Tem que começar da segunda linha do DF pois a primeira é a BASE
for i in range(1, num_linhas_df):         
    if df.iloc[i]['Period'] == '2017-12': # BASE resetada em 2017-12
        ipc_index = 100
        array_index.append(ipc_index)
    else:
        valor1 = ipc_index/100
        valor2 = valor1 * (df.iloc[i]['IPC']/100)
        valor3 =  (valor1 + valor2) * 100
        ipc_index = round(valor3, 1)
        array_index.append(ipc_index)

df['IPC_Index'] = array_index


# Mudar sequência de colunas
df = df[['Country', 'Period', 'IPC_Index', 'IPC', 'IPCA', 'IPC12']]

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

cursor.execute("DELETE FROM acct_etl.IndexIPCA WHERE Country = 'PAR'")

for index, row in df.iterrows():
    cursor.execute("INSERT INTO acct_etl.IndexIPCA (Country, Period, IPC_Index, IPC, IPCA, IPC12) values(?,?,?,?,?,?)",
                   row.Country, row.Period+'-01', row.IPC_Index, row.IPC, row.IPCA, row.IPC12)

cnxn.commit()
cnxn.close()

print('Tabela carregada no SQL Server com sucesso!')
# -------------------- FIM SALVAR DF NO SQL SERVER --------------------