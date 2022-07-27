from urllib import request
import pandas as pd
from datetime import date
from datetime import datetime
import os
import glob
import pyodbc


# -------------------- CRIAÇÃO PASTA --------------------
iso_pais = 'BOL'

# Cria pasta para arquivos auxiliares
caminho = './IPCA_' + iso_pais + '_Files'

# Verifica se o caminho espesificado existe
existe = os.path.exists(caminho)

if not existe: 
    os.makedirs(caminho)
    print(f'Nova pasta {caminho} criada!')
# -------------------- FIM CRIAÇÃO PASTA --------------------




# -------------------- DOWNLOAD ARQUIVOS --------------------
remote_url = 'https://nube.ine.gob.bo/index.php/s/uvvQoitRsqYRgi5/download'
arquivo_local = caminho + '/' + 'IPCA_BOL.xlsx'
msg = request.urlretrieve(remote_url, arquivo_local)

# Define qual arquivo será importado
nome_arquivo =  max(glob.glob(caminho + '\IPCA_BOL*.xlsx'))
print(f'Download do arquivo {nome_arquivo} concluido.')

statinfo = os.stat(arquivo_local)

if statinfo.st_size <= 9999:
    # Deletar arquivo
    os.remove(arquivo_local)
    print('Remove arquivo muito pequeno')
    print(msg) 
# -------------------- FIM DOWNLOAD ARQUIVOS --------------------




# -------------------- TRANSFORMAÇÃO DOS DADOS --------------------
def transform_DF(nome_arquivo, name_column, sheet):

    df = pd.read_excel(io=nome_arquivo, sheet_name=sheet, skiprows = range(1, 4), usecols = "A:CH", header=1)


    # Exclui linhas
    df = df.dropna(how='all',axis=0) 
    df = df[:12]


    # Exclui colunas
    droplist = [i for i in df.columns if i != 'MES' and i < date.today().year - 10 ]
    df.drop(droplist,axis=1,inplace=True)


    # arredonda
    #df = df.loc[:, df.columns != 'MES'].applymap(lambda x: str(int(x)) if abs(x - int(x)) < 1e-6 else str(round(x,4)))


    # Criar coluna Period
    df = df.melt(id_vars='MES', var_name='YEAR', value_name= name_column)
    df['Period'] = df.apply(lambda row: datetime(year=row.YEAR, month=dict_meses[row.MES], day=1).strftime('%Y-%m'), axis=1)
    df.drop(['MES', 'YEAR'],axis=1,inplace=True)
    return df


dict_meses = {'Enero': 1, 'Febrero': 2, 'Marzo': 3, 'Abril' : 4, 'Mayo' : 5, 'Junio' : 6,
              'Julio' : 7, 'Agosto' : 8, 'Septiembre' : 9, 'Octubre' : 10, 'Noviembre' : 11, 'Diciembre' : 12}


# Dataframes temporários para realisar o merge
df_ipc = transform_DF(nome_arquivo, 'IPC', 'CUADRO Nº 1.2 VAR MENSUAL')
df_idx = transform_DF(nome_arquivo, 'IPC_Index', 'CUADRO Nº 1.1 ÍNDICE MENSUAL')
df_ipc12 = transform_DF(nome_arquivo, 'IPC12', 'CUADRO Nº 1.3 VAR ACUMULADA')
df_ipca = transform_DF(nome_arquivo, 'IPCA', 'CUADRO Nº 1.4 VAR 12 MESES')


# Juntar todos os Dataframes
df = pd.merge(df_idx, df_ipc, how="left", on=["Period"])
df = pd.merge(df, df_ipca, how="left", on=["Period"])
df = pd.merge(df, df_ipc12, how="left", on=["Period"])


# Criando a coluna Country com valor BOL
df.insert(0, 'Country', 'BOL')


#Reorganizar colunas
df = df[['Country','Period','IPC_Index','IPC','IPCA','IPC12']]


# Seleciona os ultimos 10 anos
qtd_anos = 10
data_inicio = datetime(date.today().year - qtd_anos, 1,1)
df = df.loc[pd.to_datetime(df['Period']) >= data_inicio]
# -------------------- FIM TRANSFORMAÇÃO DOS DADOS --------------------




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
database = 'bome_do_banco_de_dados'
username = 'nome_usuario'
password = 'minha_senha'

cnxn = pyodbc.connect('DRIVER={ODBC Driver 17 for SQL Server};SERVER='+server+';DATABASE='+database+';UID='+username+';PWD='+ password)
cursor = cnxn.cursor()

cursor.execute("DELETE FROM acct_etl.IndexIPCA WHERE Country = 'BOL'")

for index, row in df.iterrows():
    cursor.execute("INSERT INTO acct_etl.IndexIPCA (Country, Period, IPC_Index, IPC, IPCA, IPC12) values(?,?,?,?,?,?)",
                   row.Country, row.Period+'-01', row.IPC_Index, row.IPC, row.IPCA, row.IPC12)

cnxn.commit()
cnxn.close()

print('Tabela carregada no SQL Server com sucesso!')
# -------------------- FIM SALVAR DF NO SQL SERVER --------------------