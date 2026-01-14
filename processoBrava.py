import importlib
import logging
import sys
import pandas as pd
import psycopg2
from tabulate import tabulate

from config import carregar_configuracoes


class importacaoBrava:
    def __init__(self):
        pass

    def get_colunas_names(self, conn_chinchila):
        #Função para obter os nomes das colunas das tabelas especificadas, aqui podemos adicionar mais tabelas conforme necessário
        tabelas = ['fidelidadeclassificacao', 'crediario', 'itemcadernooferta','cadernooferta','planoremuneracao']
        resultados = {}

        try:
            with conn_chinchila.cursor() as cursor:
                cursor.execute("""
                    SELECT dblink_connect('conexao_baseorigem', 
                    'host=0953-02792 user=chinchila port=5432 password=chinchila dbname=farmaciasbrava_esc_20231204');
                """)

                # Query base com placeholder %s para o nome da tabela
                sql_template = """
                SELECT * FROM dblink('conexao_baseorigem', '
                    SELECT string_agg(
                        column_name || '' '' ||
                        (CASE 
                            WHEN data_type = ''character varying'' THEN ''varchar''
                            WHEN data_type = ''numeric'' THEN ''numeric (15,4)''
                            WHEN data_type = ''character'' THEN ''varchar''
                            WHEN data_type = ''timestamp without time zone'' THEN ''timestamp''
                            ELSE data_type
                        END),
                        '', '' ORDER BY ordinal_position
                    )
                    FROM information_schema.columns
                    WHERE table_schema = ''public''
                    AND table_name = ''{}''
                ') AS data(cols text);
                """

                for tabela in tabelas:
                    cursor.execute(sql_template.format(tabela))
                    row = cursor.fetchone()
                    if row and row[0]:
                        resultados[tabela] = row[0]

                cursor.execute("SELECT dblink_disconnect('conexao_baseorigem');")
                
            return resultados

        except psycopg2.Error as e:
            logging.error(f"Erro no banco de dados: {e}")
            try:
                with conn_chinchila.cursor() as cursor:
                    cursor.execute("SELECT dblink_disconnect('conexao_baseorigem');")
            except:
                pass
            return None

    def get_script_brava(self):
        sistema = 'brava_baselimpa'

        try:
            # Carregar configurações
            config = carregar_configuracoes('config.ini')
            caminho_scripts = config['Paths']['caminho_scripts_brava']

            # Adicionar caminho dos scripts ao sys.path
            if caminho_scripts and caminho_scripts not in sys.path:
                sys.path.append(caminho_scripts)

            modulo = importlib.import_module(sistema)
            logging.info(f"Módulo {sistema} importado com sucesso!")

            # Acessar os objetos no módulo importado
            queries = {obj_name: getattr(modulo, obj_name, None) for obj_name in [
            'id_crediario','id_cadernooferta','id_loja','id_agg_crediario','id_agg_caderno','classificacao_brava',
            'planopagamento_brava','crediario_brava','cadernooferta_brava','cadernooferta_brava_classificacao',
            'curvaabc_brava','gruporemarcacao_brava','gruporemarcacaoitem_brava','fidelidade_brava','fidelidade_classificacao_brava',
            'fidelidade_pontos_brava','planodeconta_delete','planodeconta_1','planodeconta_2','planodeconta_3','planodeconta_4',
            'planodeconta_5','planodeconta_final','comissao_bonificacao','bonificacao_item','classificacao_comissao','item_comissao','dias_estocagem','demanda_brava'
            ]}
            return queries

        except ModuleNotFoundError:
            logging.error(f"Módulo {sistema} não encontrado. Certifique-se de que o nome está correto e que o módulo existe.")
        except Exception as e:
            logging.inferroro(f"Ocorreu um erro ao importar o módulo: {e}", exc_info=True)

        return {}
    
    def get_ids_brava(self, conn_chinchila):
        try:
            logging.info("Iniciando Base Limpa!")
            with conn_chinchila.cursor() as source_cursor:
                
                queries = self.get_script_brava()

                source_cursor.execute("""
                    SELECT dblink_connect('conexao_baseorigem', 
                    'host=0953-02792 user=chinchila port=5432 password=chinchila dbname=farmaciasbrava_esc_20231204');
                """)
                
                source_cursor.execute(queries['id_crediario'])
                id_crediario = source_cursor.fetchall()
                colunas_id_crediario = [desc[0] for desc in source_cursor.description]
                df = pd.DataFrame(id_crediario, columns=colunas_id_crediario)
                resultado_id_crediario = tabulate(df, headers='keys', tablefmt='psql',showindex=False)
                logging.info(f"ID - CREDIARIO NOTINHA\n{resultado_id_crediario}")

                source_cursor.execute(queries['id_agg_crediario'])
                id_agg_crediario = source_cursor.fetchall()
                colunas_id_agg_crediario = [desc[0] for desc in source_cursor.description]
                df = pd.DataFrame(id_agg_crediario, columns=colunas_id_agg_crediario)
                resultado_id_agg_crediario = tabulate(df, headers='keys', tablefmt='psql',showindex=False)
                logging.info(f"CONTATENAÇÃO ID - CREDIARIO NOTINHA\n{resultado_id_agg_crediario}")

                source_cursor.execute(queries['id_cadernooferta'])
                id_cadernooferta = source_cursor.fetchall()
                colunas_id_cadernooferta = [desc[0] for desc in source_cursor.description]
                df = pd.DataFrame(id_cadernooferta, columns=colunas_id_cadernooferta)
                resultado_id_cadernooferta = tabulate(df, headers='keys', tablefmt='psql',showindex=False)
                logging.info(f"ID - CADERNO DE OFERTA NOTINHA\n{resultado_id_cadernooferta}")

                source_cursor.execute(queries['id_agg_caderno'])
                id_agg_caderno = source_cursor.fetchall()
                colunas_id_agg_caderno = [desc[0] for desc in source_cursor.description]
                df = pd.DataFrame(id_agg_caderno, columns=colunas_id_agg_caderno)
                resultado_id_agg_caderno = tabulate(df, headers='keys', tablefmt='psql',showindex=False)
                logging.info(f"CONTATENAÇÃO ID - CADERNO DE OFERTA NOTINHA\n{resultado_id_agg_caderno}")

                source_cursor.execute(queries['id_loja'])
                id_loja = source_cursor.fetchall()
                colunas_id_loja = [desc[0] for desc in source_cursor.description]
                df = pd.DataFrame(id_loja, columns=colunas_id_loja)
                resultado_id_loja = tabulate(df, headers='keys', tablefmt='psql',showindex=False)
                logging.info(f"ID - LOJA\n{resultado_id_loja}")

                source_cursor.execute("""
                        SELECT dblink_disconnect('conexao_baseorigem')
                    """)
                conn_chinchila.commit()
                
        except psycopg2.Error as e:
            logging.info(f"Ocorreu um erro: {e}")
        except Exception as e:
            logging.error(f"Ocorreu um erro inesperado: {e}")
            
    def processo_importacao(self,conn_chinchila,get_id_crediario,get_id_cadernooferta,get_id_loja,cols_fidelidade,cols_crediario,cols_item_oferta,cols_cadernooferta,cols_planoremuneracao):
        try:
            with conn_chinchila as source_conn:
                logging.info("Iniciando Base Limpa!")
                with source_conn.cursor() as source_cursor:
                    queries = self.get_script_brava()

                    source_cursor.execute("""
                        SELECT dblink_connect('conexao_baseorigem', 
                        'host=0953-02792 user=chinchila port=5432 password=chinchila dbname=farmaciasbrava_esc_20231204');
                    """)
                    
                    source_cursor.execute(queries['planopagamento_brava'].format(ids=get_id_crediario))
                    planopagamento_brava = source_cursor.fetchall()
                    for row in planopagamento_brava:
                        source_cursor.execute(row[0])
                    source_conn.commit()
                    logging.info("Query realizado com sucesso para planopagamento_brava")

                    source_cursor.execute(queries['crediario_brava'].format(ids=get_id_crediario,colunas=cols_crediario))
                    crediario_brava = source_cursor.fetchall()
                    for row in crediario_brava:
                        source_cursor.execute(row[0])
                    source_conn.commit()
                    logging.info("Query realizado com sucesso para crediario_brava")

                    source_cursor.execute(queries['cadernooferta_brava'].format(ids=get_id_cadernooferta,colunas=cols_cadernooferta))
                    cadernooferta_brava = source_cursor.fetchall()
                    for row in cadernooferta_brava:
                        source_cursor.execute(row[0])
                    source_conn.commit()
                    logging.info("Query realizado com sucesso para cadernooferta_brava")

                    source_cursor.execute(queries['cadernooferta_brava_classificacao'].format(ids=get_id_cadernooferta,colunas=cols_item_oferta))
                    cadernooferta_brava_classificacao = source_cursor.fetchall()
                    for row in cadernooferta_brava_classificacao:   
                        source_cursor.execute(row[0])
                    source_conn.commit()
                    logging.info("Query realizado com sucesso para cadernooferta_brava_classificacao")

                    source_cursor.execute(queries['curvaabc_brava'])
                    curva_results = source_cursor.fetchall()
                    for row in curva_results:
                        source_cursor.execute(row[0])
                    logging.info("Query curvaabc_brava executada com sucesso!")

                    source_cursor.execute(queries['fidelidade_brava'])
                    fidelidade_results = source_cursor.fetchall()
                    for row in fidelidade_results:
                        source_cursor.execute(row[0])
                    logging.info("Query fidelidade_brava executada com sucesso!")

                    source_cursor.execute(queries['fidelidade_classificacao_brava'].format(colunas=cols_fidelidade))
                    fidelidade_class_results = source_cursor.fetchall()
                    for row in fidelidade_class_results:
                        source_cursor.execute(row[0])
                    logging.info("Query fidelidade_classificacao_brava executada com sucesso!")

                    source_cursor.execute(queries['fidelidade_pontos_brava'])
                    fidelidade_ponto_results = source_cursor.fetchall()
                    for row in fidelidade_ponto_results:
                        source_cursor.execute(row[0])
                    logging.info("Query fidelidade_pontos_brava executada com sucesso!")

                    source_cursor.execute(queries['planodeconta_delete'])
                    planodelete_results = source_cursor.fetchall()
                    for row in planodelete_results:
                        source_cursor.execute(row[0])
                    logging.info("Query planodeconta_delete executada com sucesso!")

                    source_cursor.execute(queries['planodeconta_1'])
                    plano_results = source_cursor.fetchall()
                    for row in plano_results:
                        source_cursor.execute(row[0])

                    source_cursor.execute(queries['planodeconta_2'])
                    plano2_results = source_cursor.fetchall()
                    for row in plano2_results:
                        source_cursor.execute(row[0])

                    source_cursor.execute(queries['planodeconta_3'])
                    plano3_results = source_cursor.fetchall()
                    for row in plano3_results:
                        source_cursor.execute(row[0])

                    source_cursor.execute(queries['planodeconta_4'])
                    plano4_results = source_cursor.fetchall()
                    for row in plano4_results:
                        source_cursor.execute(row[0])

                    source_cursor.execute(queries['planodeconta_5'])
                    plano5_results = source_cursor.fetchall()
                    for row in plano5_results:
                        source_cursor.execute(row[0])

                    source_cursor.execute(queries['planodeconta_final'])
                    planofinal_results = source_cursor.fetchall()
                    for row in planofinal_results:
                        source_cursor.execute(row[0])
                    logging.info("Query Plano de Conta executada com sucesso!")

                    source_cursor.execute(queries['comissao_bonificacao'].format(colunas=cols_planoremuneracao))
                    comissao_results = source_cursor.fetchall()
                    for row in comissao_results:
                        source_cursor.execute(row[0])

                    source_cursor.execute(queries['bonificacao_item'])
                    bonificacao_item_results = source_cursor.fetchall()
                    for row in bonificacao_item_results:
                        source_cursor.execute(row[0])

                    source_cursor.execute(queries['classificacao_comissao'])
                    classificacao_comissao_results = source_cursor.fetchall()
                    for row in classificacao_comissao_results:
                        source_cursor.execute(row[0])

                    source_cursor.execute(queries['item_comissao'])
                    item_comissao_results = source_cursor.fetchall()
                    for row in item_comissao_results:
                        source_cursor.execute(row[0])
                    logging.info("Query Comissao, Bonificação executada com sucesso!")

                    source_cursor.execute(queries['dias_estocagem'].format(ids=get_id_loja))
                    dias_estocagem_results = source_cursor.fetchall()
                    for row in dias_estocagem_results:
                        source_cursor.execute(row[0])
                    logging.info("Query dias_estocagem executada com sucesso!")

                    source_cursor.execute(queries['demanda_brava'].format(ids=get_id_loja))
                    demanda_brava_results = source_cursor.fetchall()
                    for row in demanda_brava_results:
                        source_cursor.execute(row[0])
                    logging.info("Query demanda_brava executada com sucesso!")

                    source_cursor.execute("""
                        SELECT dblink_disconnect('conexao_baseorigem')
                    """)

                    source_conn.commit()
                    logging.info("Brava Importação Finalizado com sucesso!")

        except psycopg2.Error as e:
            logging.info(f"Ocorreu um erro: {e}")