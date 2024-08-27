ELT de dados completo com Dashbord

Esse artigo é fruto de um projeto realizado durante o curso de BootCamp de Engenharia de dados da Udemy, onde foi realizado todo o processo de ELT para utilzação dos dados finais para realizar a criação de um dashbord para verificar o numero de vendas por concessionárias.

Primeiramente acho importante dizer que esse é um projeto inicial e que pode ser tomado outros tipos de caminhos e analises caso seja necessário ou de sua vontade.

Contexto
Segundo o contexto geral da proposta para a realização de todos os processos no decorrer desse projeto, o gerente da empresa NovaDriva, que é uma concessionaria de carros que possui filiais em vários estados brasileiros, solicita ao Engenheiro de dados uma demanda na qual ele precisa de algumas informações mais precisa, como(vendas por concessionarias, vendas por veiculo, vendas por vendedor e analise temporal), com essas informações e com algumas outras informações adiquiridas com o time de RH da empresa e outros setores, conseguimos planejar e dar inicio ao nosso projeto.

Problemas relatados pelo cliente
Na sua demanda o cliente fez algumas reclamações

dados não confiáveis
pouca frequência
dados em excel
Etapas do projeto
Apesar de no inicio desse artigo dar a entender que de certa forma só iremos realizar a elaboração do dashbord, na verdade vamos realizar todas as etapas do projeto, desde a parte inicial de ELT com conceitos de engenharia de dados, até a parte de criação de dashbord

Criação de uma MV Linux com Ubuntu na AWS
Construção de uma Dataware house utilizando SnowFlake
Pipeline de carga de dados utilizando Airflow
Transformar os dados dentro do DW utilizando DBT
Construir um Dashbord utilizando Looker Studio
Arquitetura Proposta da Solução
Resumidamente, o passo inicial é levar os dados que estão no banco de dados de operação da empresa, que no caso é o PostGreSql e levar esses dados para a camada analítica, porém os dados não podem ir diretamente para essa cama, primeiro precisam passar por um processo de transformação completo para garantir que o objetivo seja alcançado.

Extração dos dados
Como a ideia do projeto é utilizar os dados de todas as concessionarias, precisamos extrair esses valores do banco de dados da empresa, que no caso é o PostGreSql e leva-los para outro local, no caso será um DW que será criado utilizando Snowflake e o processo de extração será feito utilizando o Airflow.

Levando em consideração que já temos a conta AWS criada, e também sabemos realizar a criação da VM EC2 Linux com Ubuntu na AWS, e com as credenciais fornecidas pelo cliente iremos acessar o banco de dados da empresa pelo PostGreSql, iremos realizar algumas consultas no banco de dados para conhecer melhor a estruturação dos dados, após essa verificação iremos realizar inicializar nossa VM e acessa-la através do prompt de comando com a chave fornecidade pelo AWS.

Configurando o Docker
Já no VM Ubuntu, com comando Linux vamos fazer algumas alterações nas configurações utilizando o editor “nano” que é mais simples de ser utilizado.

Alteramos essa linha de código de ‘True’ para ‘False’ para remover as Dags de exemplos que aparecem no Airflow.

AIRFLOW__CORE__LOAD_EXAMPLES: ‘false’



https://github.com/user-attachments/assets/8b56c342-5325-4666-9dec-b16278d953b8

Após salvarmos e sairmos desse editor Nano, vamos dar o comando

sudo docker compose stop


https://github.com/user-attachments/assets/d26be48f-a915-42a3-81e2-a61bf4e2519f


para parar os conteiners do Docker, e após isso iremos inicializar pois as alterações no docker-compose só serão lidas quando ele é inicializado.

sudo docker compose up -d
para inicializar os containers



https://github.com/user-attachments/assets/e51e7d8a-d191-4f51-931e-e8108c951bab


e após isso verificar se o status esteja em health

sudo docker ps



https://github.com/user-attachments/assets/960e6d23-d22e-4275-a8db-0841c5b0d427

Agora já no Airflow só ira aparecer nossas Dags e não os demais exemplos que costumam aparecer inicialmente, permitindo um ambiente mais limpo para o trabalho.

Configurando o Snowflake e criando os objetos necessários
Nessa etapa vamos criar um banco de dados e um schema que seria uma divisão do banco de dados, como se fosse assuntos diferentes dentro do banco de dados e também iremos criar um Wharehouse de acordo com nossa necessidade de processamento.

Esse banco de dados que será criado, será utilizado como uma camada de stage, ou seja, um banco de dados temporários, que vem antes da camada analítica, e que o usuário final não tem acesso.

Criando o banco de dados — comando.

em Worksheets - + — SQL Worksheet para criar uma nova Worksheet

COMANDOS
create database novadrive;

create schema stage;

create warehouse DEFAULT_WH;

Após criar o banco de dados nova drive, a camada de stage e o warehouse DEFAULT_WH, iremos criar as 7 tabelas no Snowflake igual as que estão no banco de dados operacional da empresa, com o cursor do snowflake apontado para NOVADRIVE.STAGE executaremos o seguinte script.

CREATE TABLE veiculos (
id_veiculos INTEGER,
nome VARCHAR(255) NOT NULL,
tipo VARCHAR(100) NOT NULL,
valor DECIMAL(10, 2) NOT NULL,
data_atualizacao TIMESTAMP_LTZ,
data_inclusao TIMESTAMP_LTZ
);

CREATE TABLE estados (
id_estados INTEGER,
estado VARCHAR(100) NOT NULL,
sigla CHAR(2) NOT NULL,
data_inclusao TIMESTAMP_LTZ,
data_atualizacao TIMESTAMP_LTZ
);

CREATE TABLE cidades (
id_cidades INTEGER,
cidade VARCHAR(255) NOT NULL,
id_estados INTEGER NOT NULL,
data_inclusao TIMESTAMP_LTZ,
data_atualizacao TIMESTAMP_LTZ

);

CREATE TABLE concessionarias (
id_concessionarias INTEGER,
concessionaria VARCHAR(255) NOT NULL,
id_cidades INTEGER NOT NULL,
data_inclusao TIMESTAMP_LTZ,
data_atualizacao TIMESTAMP_LTZ
);

CREATE TABLE vendedores (
id_vendedores INTEGER,
nome VARCHAR(255) NOT NULL,
id_concessionarias INTEGER NOT NULL,
data_inclusao TIMESTAMP_LTZ,
data_atualizacao TIMESTAMP_LTZ
);

CREATE TABLE clientes (
id_clientes INTEGER,
cliente VARCHAR(255) NOT NULL,
endereco TEXT NOT NULL,
id_concessionarias INTEGER NOT NULL,
data_inclusao TIMESTAMP_LTZ,
data_atualizacao TIMESTAMP_LTZ
);

CREATE TABLE vendas (
id_vendas INTEGER,
id_veiculos INTEGER NOT NULL,
id_concessionarias INTEGER NOT NULL,
id_vendedores INTEGER NOT NULL,
id_clientes INTEGER NOT NULL,
valor_pago DECIMAL(10, 2) NOT NULL,
data_venda TIMESTAMP_LTZ,
data_inclusao TIMESTAMP_LTZ,
data_atualizacao TIMESTAMP_LTZ
);

e iremos executar esse script, logo após isso as 7 tabelas estarão criadas na camada de stage.

Criação da Dag no Airflow
Para começar a criar a a Dag, iremos utilizar o VSCode e após isso iremos copiar e colar o código na pasta da Dag no ambiente do Airflow.

Teremos duas task para cada tabela:

get_max_id que ira verificar no destino qual é o ultimo identificador ou a maior chave primaria que existe lá, por que no processo de carga incremental iremos carregar os dados baseados na ultima chave.
load_incremental_data irá carregar esses dados diferencias a partir da ultima chave que a função anterior pegou.
Iremos criar todo o processo de uma forma dinâmica, utilizando tambem o decorador task, iremos ter uma lista e criaremos um laço para percorrer as 7 tabelas, e dentro desse laço criaremos dinamicamente as 2 task para cada uma dessas tabelas.

No VSCode codificaremos a Dag.(Editor opcional)
from datetime import datetime, timedelta

from airflow.decorators import dag, task

from airflow.providers.postgres.hooks.postgres import PostgresHook

from airflow.providers.snowflake.hooks.snowflake import SnowflakeHook

default_argrs = {

‘owner’ : ‘airflow’,

‘depends_on_past’ : False,

‘start_date’ : datetime(2024,1,1),

‘email_failure’ : False,

‘email_on_retry’ : False,

‘retries’ : 0,

‘retry_delay’ : timedelta(minutes=5),

}

@dag(

dag_id=’postgres_to_snowflake’,

default_argrs=default_argrs,

description=’Load data incrementally from Postgres to Snowflake’,

schedule_interval=timedelta(days=1),

catchup=False

)

def postgres_to_snowflake_etl():

table_names = [‘veiculos’, ‘estados’, ‘cidades’, ‘concessionarias’, ‘vendedores’, ‘clientes’, ‘vendas’]

for table_name in table_names:

@task(task_id=’get_max_id_{table_name}’)

def get_max_primary_key(table_name: str):

with SnowflakeHook(snowflake_conn_id=’snowflake’).get_conn() as conn:

with conn.cursor() as cursor:

cursor.execute(f’SELECT MAX(ID_{table_name}) FROM {table_name}’)

max_id = cursor.featchone()[0]

return max_id if max_is is not None else 0

@task(task_id=f’load_date_{table_name}’)

def load_incremental_data(table_name:str, max_id: int):

with PostgresHook(postgress_conn_id=’postgress’).get_conn() as pg_conn:

with pg_conn.cursor() as pg_cursor:

primary_key(f’ID_{table_name}’)

pg_cursor.execute(f’SELECT column_name FROM information_schema.columns WHERE3 table_name = ‘{table_name}’’)

columns = [row[0] for row in pg_cursor.fetchall()]

columns_list_str = ‘, ‘.join(columns)

placeholders = ‘, ‘.join([‘%’] * len(columns))

pg_cursor.execute(f”SELECT {columns_list_str} FROM {table_name} WHERE {primary_key} > {max_id} “)

rows = pg_cursor.fetchall()

with SnowflakeHook(snowflake_conn_id=’snowflake’).get_conn() as sf_conn:

with sf_conn.cursor() as sf_cursor:

insert_query = f”INSERT INTO {table_name}({columns_list_str}) VALUES ({placeholders})”

for row in rows:

sf_cursor.execute(insert_query, row)

max_id = get_max_primary_key(table_name)

load_incremental_data(table_name, max_id)

postgres_to_snowflake_etl_dag = postgres_to_snowflake_etl()




https://github.com/user-attachments/assets/056c5c45-3a09-4510-85c3-051119c4f911




https://github.com/user-attachments/assets/434a88f2-b04a-4da3-a78a-90efe153b5d8


Criando as conexões

Após criar as Dags, criamos as conexões com o PostgreSql para extrair as informações de lá, e depois as conexões com o Snowflake, lembrando que esses detalhes não serão mostrados aqui pois são relativamente simples para quem já tem um conhecimento básico.

Rodando a Dag
Iremos conectar na nossa maquina virtual que foi criada anteriormente, iremos entrar dentro da pasta dag e criaremos um arquivo com o nome novadrive.py com o comando

touch novadrive.py
logo após iremos utilizar o editor nano para colar a dag que foi criada no VSCode, utilizando o comando

nano novadrive.py
esse será o resultado da dag colada dentro do arquivo criado anteriormente



https://github.com/user-attachments/assets/c56b5a0d-8c58-4d99-9720-296cf85ff978


após isso salvamos as alterações, e esperamos alguns minutos para a dag aparecer no painel do Airflow.



https://github.com/user-attachments/assets/5a49fa5f-9144-4037-80b5-6bbedfe8383c

Nessas configurações eles estão programados para rodar 1 vez por dia, e podem ser alteradas conforme sua necessidade.

Feito todo esse processo, vamos no Airflow e executamos a Dag, e aguardando o tempo de carregamento das task, esse será o resultado se não ocorrer nenhum erro.




https://github.com/user-attachments/assets/af2f2011-7819-447c-b5a2-53c2f2034b39

Testando a Carga Incremental
Agora vamos até o Snowflake e verificamos nossa camada de stage no nosso DW em novadrive.stage, executamos alguns comando SQL como

select * from vendas;
select count(*) from vendas;
para verificar se os dados foram carregados, e como mostra a imagem, o processo de carga foi executado com sucesso.



https://github.com/user-attachments/assets/9104fd87-9a98-4571-b3bc-2ca033dc0ddd

Transformando os dados com o DBT e Construindo uma camada analítica
Nessa etapa o nosso projeto de transformação vai ter algumas camadas que serão de stage, cama de dimensão(aspectos do negócio),camada de fatos(dados quantitativos),camada analítica,

Primeiro criamos nosso arquivo Source.yml dentro da pasta model.

source são as tabelas que não foram carregadas pelo Airflow.



https://github.com/user-attachments/assets/3a2f5097-e494-47f3-9e46-c8008424fe17


Para simplificar, nessa etapa vamos criar os demais arquivos respectivamente cada um em sua pasta, os códigos estarão disponível no github.

um dos exemplos é:

{{ config(materialized=’view’) }}

WITH source AS (

SELECT

id_cidades,

INITCAP(cidade) AS nome_cidade,

id_estados,

data_inclusao,

COALESCE(data_atualizacao, data_inclusao) AS data_atualizacao

FROM {{ source(‘sources’, ‘cidades’) }}

)

SELECT

id_cidades,

nome_cidade,

id_estados,

data_inclusao,

data_atualizacao

FROM source

que inicialmente segue o template Jinja, as demais configurações estarão disponivel no github: https://github.com/BrunoBorgess




https://github.com/user-attachments/assets/ad7f9f85-dd74-4277-bf26-bc449afb5886


esse é o resultado depois de todos os arquivos criados dentro de suas respectivas pastas, para criação de cada modelo podemos utilizar o comando

dbt run
Voltando ao Snowflake, verificamos que a criação dos modelos foi um sucesso, como mostra a imagem abaixo;




https://github.com/user-attachments/assets/961684ef-e2c4-49f2-9f39-4b445a2abdb9

podemos verificar no DBT a linhagem dos dados, desde seu inicio até o seu final.




https://github.com/user-attachments/assets/c8b0be4a-0d28-4a68-b524-629a67b4e946



Sobre as analises
Nessa etapa do nosso projeto, precisaremos construir as análises para que o cliente possa saber as vendas por concessionarias, vendas por modelo de veiculo, vendas por vendedor e vendas temporal.

que no final vai no gerar esse resultado, que poderão ser utilizados para fornecer informações analíticas que vão ser exibidas através de relatórios e dashbords, esse é o objetivo desse DW.





https://github.com/user-attachments/assets/557500b1-c821-436a-bf21-9aee509f5490



utilizamos a materialização como ‘table’, as analises vão ser baseadas na ‘fct_vendas’, e as dimensões como tabelas acessórias para ter mais informações. No geral agrupamos por concessionária e ordena pelo total de vendas, e com o valor médio, usando funções padrões do SQL.

Criaremos a pasta analysis, e criar as analises necessárias.



https://github.com/user-attachments/assets/774d7202-0a4c-4c31-a22d-7fbdc44294bf



https://github.com/user-attachments/assets/2ff02053-82b6-4bb5-a6c6-24d75ba9b652



https://github.com/user-attachments/assets/7cc97728-fd62-4a85-bba2-e4e86470aacf

Cada uma dessas analises está utilizando comando básico de SQL, nada que precise ser aprofundado nesse artigo. Após isso salvamos e rodamos nossas analises para concluir essa etapa do projeto, e no Snowflake verificamos que os dados já estão lá.



https://github.com/user-attachments/assets/8e6d7b79-7b01-4f81-82f4-973621f98a12


Realizando os testes no DBT

Falando um pouco sobre os testes do DBT, são importantes para validar a integridade dos dados, mas também validar regras de negócio, nada mais é do que se o teste retornar alguma linha, significa que o teste falhou, se a integridade estiver correta, é importante que o código não retorne nenhuma linha.

Na pasta teste criamos um arquivo test.sql, e colocamos nosso código para realizar o teste que será o código abaixo, executamos o teste com o comando ‘dbt test’




https://github.com/user-attachments/assets/db168e12-25f7-41a7-8025-469cbcfc81da


se o teste passar, exibirá o seguinte resultado:




https://github.com/user-attachments/assets/98306b76-51fb-4b24-b430-7b2c10c68488


ou seja, teste realizado com sucesso, o nosso código passou nesse teste.

Após isso, realizar o commit para o dev e agora fazer o merge, para mandar o que esta no Dev para o main, processo realizado com sucesso em nosso projeto.

agora vamos realizar o deploy criando o environments, que no caso não vem ao caso nesse artigo pois é um processo fácil e intuitivo.

criamos também o jobs ‘deploy jobs’.




https://github.com/user-attachments/assets/528bbedc-019f-4402-84be-78b8ec7e680b



Processo executado com sucesso, verificamos no Airflow e nossa camada analityc está lá.




https://github.com/user-attachments/assets/cbaa5771-8073-47f7-92a8-5453b089089a


Nessa etapa o nosso papel como engenheiro de dados foi realizado com sucesso, todo o processo de ELT foi realizado com sucesso, mas como um pedido extra do nosso cliente, vamos realizar a criação de apenas um dashbord de vendas por concessionarias por veiculo.

Criação de dashbord com Looker Studio
A diversas utilidades para que os dados sejam utilziados pelo usuário final, tanto relatórios, APIS, entre outros, nessa etapa iremos apenas demonstrar como ficou o dashbord criado com Looker Studio da google.

O foco não é mostrar como foi criado, pois de certa forma é fácil criar dashbord com Looker studio assim que os dados já foram tratados, mas é importante mostrar como o bom trabalho de um engenheiro de dados pode gerar grandes resultados para a tomada de decisão de uma empresa.

![20240827_152820(197)](https://github.com/user-attachments/assets/e42a3804-67a5-4c27-b91a-b7150a9f09ca)










