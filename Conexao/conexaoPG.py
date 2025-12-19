import random
import psycopg2
from psycopg2 import sql
from config import carregar_configuracoes
from datetime import datetime
import logging


class Postgres:
    def __init__(self):
        pass

    def criar_base_oficial(self, base_nome):
        config = carregar_configuracoes('config.ini')
        porta_pg = config['Database']['porta_pg']
        host_pg = config['Database']['host_pg']
        
        try:
            logging.info("Função de Criação da Base Oficial")

            # Gerar nome da base com data e número aleatório
            data_atual = datetime.now().strftime("%d_%m_%y")
            codigo_random = random.randint(1000, 9999)
            nome_final = f"{base_nome}_baseLimpa_{data_atual}_{codigo_random}"

            connection_params = {
                "host": host_pg,
                "database": "postgres",
                "user": "postgres",
                "password": "supertux",
                "port": porta_pg
            }

            connection = psycopg2.connect(**connection_params)
            cursor = connection.cursor()

            # Desabilitar autocommit para executar CREATE DATABASE
            connection.autocommit = True

            query = sql.SQL("""
                CREATE DATABASE {}
                WITH
                OWNER = chinchila
                ENCODING = 'UTF8'
                LC_COLLATE = 'pt_BR.UTF-8'
                LC_CTYPE = 'pt_BR.UTF-8'
                TABLESPACE = pg_default
                CONNECTION LIMIT = -1
                TEMPLATE = clean_chinchila;
            """).format(sql.Identifier(nome_final))
            
            cursor.execute(query)
            cursor.close()
            connection.close()

            logging.info(f"\033[92m✔ Base Oficial Alpha7 '{nome_final}' criada com sucesso!\033[0m")

            return nome_final  # Retorna o nome gerado

        except psycopg2.Error as e:
            logging.error(f"\033[91m✖ Erro ao criar a base de dados: {e}\033[0m")
            return None
        
    def conexaoBaseOficialChinchila(self,base_nome):
        config = carregar_configuracoes('config.ini')
        porta_pg = config['Database']['porta_pg']
        host_pg = config['Database']['host_pg']
        try:
            # Formatar o nome da base de dados
            db_name_oficial = f"{base_nome}"

            # Configurações de conexão
            conn_params = {
                "host": host_pg,
                "database": db_name_oficial,
                "user": "chinchila",
                "password": "chinchila",
                "port": porta_pg
            }

            # Estabelecer a conexão com o banco de dados
            conn = psycopg2.connect(**conn_params)
            return conn  

        except psycopg2.Error as e:
            logging.error(f"Ocorreu um erro ao conectar à base de dados: {e}")
            return None  
    
    def conexaoBaseOficialPostgres(self,base_nome):
        config = carregar_configuracoes('config.ini')
        porta_pg = config['Database']['porta_pg']
        host_pg = config['Database']['host_pg']
        try:
            # Formatar o nome da base de dados
            db_name_oficial = f"{base_nome}"

            # Configurações de conexão
            conn_params = {
                "host": host_pg,
                "database": db_name_oficial,
                "user": "postgres",
                "password": "supertux",
                "port": porta_pg
            }

            # Estabelecer a conexão com o banco de dados
            conn = psycopg2.connect(**conn_params)
            return conn  

        except psycopg2.Error as e:
            logging.error(f"Ocorreu um erro ao conectar à base de dados: {e}")
            return None  