import requests
import pandas as pd
from datetime import date, datetime
from bs4 import BeautifulSoup
import glob
from urllib import request
import os
import pyodbc


# -------------------- WEB SCRAPING --------------------
# Define o intervalo anual que será usado (10 anos)
ano_fim = date.today().year
ano_inicio = ano_fim - 10

# Dataframe principal que receberá todos os dados
df = pd.DataFrame()

print('Iniciando Web Scraping ...')
while ano_inicio <= ano_fim:

    # Define a URL que será usada para raspagem dos dados e verifica se é válida
    # Caso invalida, avisa o erro, caso válida, realisa a raspagem
    url = requests.get(f'https://datosmacro.expansion.com/ipc-paises/argentina?sector=IPC+General&sc=IPC-IG&anio={ano_inicio}')

    if url.status_code != 200:
        print(f"----- ERRO na URL do ano {ano_inicio} ------\n\n")
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
# -------------------- FIM WEB SCRAPING --------------------




# -------------------- TRANSFORMAÇÃO DOS DADOS 1 --------------------
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

df.insert(0, 'Country', 'ARG')

# Renomeia colunas do DF 
df = df.rename(columns={'Interanual':'IPC12', 
                        'Acum. desde Enero':'IPCA', 
                        'Variación mensual':'IPC'})
# -------------------- FIM TRANSFORMAÇÃO DOS DADOS 1 --------------------




# -------------------- CRIAÇÃO PASTA --------------------
iso_pais = 'ARG'

# Cria pasta para arquivos auxiliares
caminho = './IPCA_' + iso_pais + '_Files'

# Verifica se o caminho espesificado existe
existe = os.path.exists(caminho)

if not existe: 
    os.makedirs(caminho)
    print(f'Nova pasta {caminho} criada!')
# -------------------- FIM CRIAÇÃO PASTA --------------------




# -------------------- DOWNLOAD DOS ARQUIVOS --------------------
remote_url = 'https://www.indec.gob.ar/ftp/cuadros/economia/'

# Criar tuplas com últimos meses pra criar URL
ano = date.today().year
mes = date.today().month
meses = ano * 12 + mes - 1 # Months since year 0 minus 1
tupla_ano_mes = [((meses - i) // 12, (meses - i) % 12 + 1) for i in range(3)]


# Monta string com os últimos meses pra buscar arquivos corretos
for data in tupla_ano_mes:
    ano_str = str(data[0])[2:] # ano em str
    mes_str = str(data[1]).zfill(2) # mes em str
    
    nome_arquivo = "sh_ipc_" + mes_str + "_" + ano_str +".xls"
    remote_url_file = remote_url + '/' + nome_arquivo
    local_file = caminho + '/' + 'IPCA_ARG' + '_' + ano_str + "_" + mes_str + '.xls'
        
    # Download remotamente e salva localmente
    msg = request.urlretrieve(remote_url_file, local_file)
    print(f'Download do arquivo {nome_arquivo} concluido.')
    
    # Verifique se arquivo for muito pequeno, provavelmente está com erro
    statinfo = os.stat(local_file)
    
    if statinfo.st_size <= 99999:
        # Deletar arquivo
        os.remove(local_file)
        print('Remove arquivo muito pequeno')
        print(msg)
# -------------------- FIM DOWNLOAD DOS ARQUIVOS --------------------


    

# -------------------- CRIAÇÃO COLUNA IPC Index --------------------
# Define qual arquivo será importado
arq_mais_recente =  max(glob.glob(caminho + '\IPCA_ARG*.xls'))
print(f'Abrindo arquivo localizado em: {arq_mais_recente}')

sheet3 =  'Índices IPC Cobertura Nacional'

# IPC_Index 
df_IPC_Index = pd.read_excel(io=arq_mais_recente, sheet_name=sheet3, skiprows = range(1, 5), header=1)
df_IPC_Index = df_IPC_Index.dropna(how='all',axis=1)
df_IPC_Index.drop(df_IPC_Index[(df_IPC_Index['Total nacional'] != 'Nivel general')].index, inplace = True, axis=0)
df_IPC_Index.rename(lambda t: pd.to_datetime(t).strftime('%Y-%m') if t != 'Total nacional' else t,axis='columns', inplace = True)
df_IPC_Index.drop( df_IPC_Index.index.to_list()[1:] , inplace=True,axis=0)
df_IPC_Index = pd.melt(df_IPC_Index, id_vars=['Total nacional'], var_name='Period', value_name='IPC_Index')
df_IPC_Index = df_IPC_Index.drop('Total nacional', axis=1)

df = pd.merge(df, df_IPC_Index, how="left", on=["Period"])
# -------------------- FIM CRIAÇÃO COLUNA IPC Index --------------------




# -------------------- TRANSFORMAÇÃO DOS DADOS 2 --------------------
# Substituindo todos os valores NaN por 0 (zero)
df.fillna(value= '0', inplace= True)

# Mudar sequência de colunas
df = df[['Country', 'Period', 'IPC_Index', 'IPC', 'IPCA', 'IPC12']]

# Remover símbolo de porcentagem e vírgula das colunas
def corrigir_nomes(numero):
    numero = numero.replace('--%', '0').replace('%', '').replace(',', '.')
    return numero

df['IPC12'] = df['IPC12'].apply(corrigir_nomes)
df['IPCA'] = df['IPCA'].apply(corrigir_nomes)
df['IPC'] = df['IPC'].apply(corrigir_nomes)

# Converter tipo de dado das colunas
# df['Period'] = pd.to_datetime(df['Period'], format = '%Y-%m', infer_datetime_format=False)

df['IPC12'] = pd.to_numeric(df['IPC12'], errors= 'coerce')
df['IPCA'] = pd.to_numeric(df['IPCA'], errors='coerce')
df['IPC'] = pd.to_numeric(df['IPC'], errors='coerce')
df['IPC_Index'] = pd.to_numeric(df['IPC_Index'], errors='coerce')

# Ordenar DF por data
df = df.sort_values(by=['Period'])
# -------------------- FIM TRANSFORMAÇÃO DOS DADOS 2 --------------------




# -------------------- SALVAR DF LOCALMENTE --------------------
final_caminho = 'IPCA.xlsx'
sheet_name = iso_pais

if not os.path.isfile(final_caminho):
    df.to_excel(
        final_caminho,
        sheet_name=sheet_name)

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

cursor.execute("DELETE FROM acct_etl.IndexIPCA WHERE Country = 'ARG'")

for index, row in df.iterrows():
    cursor.execute("INSERT INTO acct_etl.IndexIPCA (Country, Period, IPC_Index, IPC, IPCA, IPC12) values(?,?,?,?,?,?)",
                   row.Country, row.Period+'-01', row.IPC_Index, row.IPC, row.IPCA, row.IPC12)

cnxn.commit()
cnxn.close()

print('Tabela carregada no SQL Server com sucesso!')
# -------------------- FIM SALVAR DF NO SQL SERVER --------------------
