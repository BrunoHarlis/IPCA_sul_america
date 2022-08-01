from urllib import request
import pandas as pd
from datetime import date
from datetime import datetime
import os
import glob
import pyodbc


# -------------------- DOWNLOAD DOS ARQUIVOS --------------------
iso_pais = 'PER'
num_anos = 10

# Cria pasta para arquivos auxiliares
caminho = './IPCA_' + iso_pais + '_Files'

# Verifica se a pasta já existe
existe = os.path.exists(caminho)

if not existe: 
    os.makedirs(caminho)
    print("\nNova pasta criada!")


# Baixa arquivos
# Índice nacional
#remote_url = 'https://www.inei.gob.pe/media/MenuRecursivo/indices_tematicos/02_indice-precios_al_consumidor-nivel_nacional_2b_1.xlsx'
#Antigo: 'https://www.inei.gob.pe/media/MenuRecursivo/indices_tematicos/02_indice-precios_al_consumidor-nivel_nacional_8.xlsx'
remote_url = 'https://www.inei.gob.pe/media/MenuRecursivo/indices_tematicos/02_indice-precios_al_consumidor-nivel_nacional_2b_2.xlsx'

# Área metropolitada
#remote_url = 'https://www.inei.gob.pe/media/MenuRecursivo/indices_tematicos/01_indice-precios_al_consumidor-lm_2b_1.xlsx'
#Antigo: 'https://www.inei.gob.pe/media/MenuRecursivo/indices_tematicos/01_indice-precios_al_consumidor-lm_8.xlsx'  

print('\nIniciando o download dos dados')
arquivo_local = caminho + '/' + 'IPCA_' + iso_pais + '.xlsx'
msg = request.urlretrieve(remote_url, arquivo_local)


# Verifique se arquivo for muito pequeno, provavelmente está com erro
statinfo = os.stat(arquivo_local)

if statinfo.st_size <= 9999:
    # Deletar arquivo
    os.remove(arquivo_local)
    print('Arquivo muito pequelo. Provavelmente com erro.')
    print('Apagando arquivo.')

print('Fim do download')
# -------------------- FIM DOWNLOAD DOS ARQUIVOS --------------------




# -------------------- TRANSFORMAÇÃO DOS DADOS --------------------
print('\nIniciando a transformação do dados.')

# Define qual arquivo será importado
aux = caminho + '\IPCA_' + iso_pais + '*.xlsx'
nome_arquivo =  max(glob.glob(aux))


array_meses = {'Enero': 1, 'Febrero': 2, 'Marzo': 3, 'Abril' : 4, 'Mayo' : 5, 'Junio' : 6, 'Julio' : 7,
             'Agosto' : 8, 'Setiembre' : 9, 'Octubre' : 10, 'Noviembre' : 11, 'Diciembre' : 12}


# Extrai dados do arquivo
sheet =  'Base Dic.2021'
df = pd.read_excel(io=nome_arquivo, sheet_name=sheet, skiprows = range(1, 3), usecols = "A:F", header=1)

df.rename(columns = {'Año': 'ANO', 'Mes': 'MES', 'Índice' :'IPC Index',
                    'Mensual': 'IPC', 'Acumulada':'IPCA', 'Anual':'IPC12'}, inplace = True)

# Criação da coluna Period
df['ANO'].fillna(method='ffill', inplace=True )
df['Period'] = df.apply(lambda row: datetime(year=int(row.ANO), month=array_meses[row.MES], day=1).strftime('%Y-%m'), axis=1)
df.drop(['MES', 'ANO'],axis=1,inplace=True)

# Criação da coluna Country
df.insert(0, 'Country', iso_pais)

# Reorganizar colunas
df = df[['Country','Period','IPC Index','IPC','IPCA','IPC12']]

# Filtra DataFrame dos últimos 10 anos
data_inicio = datetime(date.today().year - num_anos, 1,1)
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

cursor.execute("DELETE FROM acct_etl.IndexIPCA WHERE Country = 'PER'")

for index, row in df.iterrows():
    cursor.execute("INSERT INTO acct_etl.IndexIPCA (Country, Period, IPC_Index, IPC, IPCA, IPC12) values(?,?,?,?,?,?)",
                   row.Country, row.Period+'-01', row.IPC_Index, row.IPC, row.IPCA, row.IPC12)

cnxn.commit()
cnxn.close()

print('Tabela carregada no SQL Server com sucesso!')
# -------------------- FIM SALVAR DF NO SQL SERVER --------------------