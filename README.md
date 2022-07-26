# IPCA Sul América

Nesse projeto, será criada uma base de dados com o IPCA dos últimos 10 (dez) anos dos países sul americanos.

## O que é Índice Nacional de Preços ao Consumidor Amplo (IPCA)?
Índice de preços no consumidor é usado para observar tendências de inflação. É calculado com base no preço médio necessário para comprar um conjunto de bens de consumo e serviços num país, comparando com períodos anteriores.

## Etapas do projeto
* Os dados serão capturados de várias formas e fontes diferentes dependendo do país. Alguns países apresentam seus relatórios de em formatos mais fáceis de ser manipulados e outros não tão simples, portanto, em alguns casos os dados serão pegos através de  web scraping, outros através de aquivos CSV. 

* Posteriormente, os dados serão transformados usando a biblioteca Pandas. 

* Com os dados prontos, será realizada duas formas de armazenamento. Uma, salvando todos os dados em um único arquivo do Excel contendo abas para cada país, outra será carregando os dados em uma tabela do banco de dados SQL Server.

### Nota:
Para fins de didática e fácil compreensão, o ETL do IPCA de cada país será separado em scripts individuais, por exemplo: IPCA_Brasil, IPCA_Argentina, IPCA_Paraguay. 
