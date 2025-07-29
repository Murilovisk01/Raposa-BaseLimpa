import importlib
import sys
from tkinter import END
import psycopg2
import pandas as pd
from tabulate import tabulate
from conexaoSSH import ConexaoSSH
from config import carregar_configuracoes

class importacaoBaseLimpa:
    def __init__(self):
        pass

    def importSistemaBaseLimpa(self,opcao_script):
        sistema = opcao_script

        try:
            # Carregar configurações
            config = carregar_configuracoes('config.ini')
            caminho_scripts = config['Paths']['caminho_scripts_baselimpa']

            # Adicionar caminho dos scripts ao sys.path
            if caminho_scripts and caminho_scripts not in sys.path:
                sys.path.append(caminho_scripts)

            modulo = importlib.import_module(sistema)
            print(f"Módulo {sistema} importado com sucesso!")

            # Acessar os objetos no módulo importado
            queries = {obj_name: getattr(modulo, obj_name, None) for obj_name in [
                'create_dblink_query','conexao_dblink' , 'limpza_base_destino_query', 'versao_bases_query', 'query_cest','query_cfop',
                'query_curvaabc', 'query_dcb', 'query_feriado',
                'query_estado', 'query_cidade', 'query_bairro', 'query_logradouro','query_pessoa',
                'query_endereco', 'query_perfilcofins', 'query_perfilicms',
                'query_perfilpis', 'query_totalizadorfiscal', 'query_fabricante', 'query_nbm',
                'query_ncm', 'query_gruporemarcacao', 'query_principioativo',
                'query_produto', 'query_classificacao', 'query_classificacaoproduto',
                'query_embalagem', 'query_formapagamento', 'query_icmsestadual', 'query_icmsinterestadual',
                'query_naturezaoperacao', 'query_naturezaoperacaoperfilicms', 'query_icmsproduto', 'query_contato', 'query_fornecedor',
                'query_embalagemfornecedor','query_validacao','query_ajuste_unidade','query_custo','alteracaoProdutoConfig',
                'ativarProdutos','configUnidadeNegocio','tarefaAgendanda','estoqueSetar','suspenderBusca',
                'limpezaLogSinc','cfopOperacaoFiscal1','cfopOperacaoFiscal2','cfopOperacaoFiscal3','query_proxima_semente',
                'alterarVinculoProdutoReferenciaNaoE','excluir_gerador','criaSequencia','alterar_etiqueta_duplicada','reinicia_jboss','remove_sequencia'
            ]}
            return queries

        except ModuleNotFoundError:
            print(f"Módulo {sistema} não encontrado. Certifique-se de que o nome está correto e que o módulo existe.")
        except Exception as e:
            print(f"Ocorreu um erro ao importar o módulo: {e}", exc_info=True)

        return {}

    def importSistemaBaseLimpaPoupaAqui(self,opcao_script):
        sistema = opcao_script

        try:
            # Carregar configurações
            config = carregar_configuracoes('config.ini')
            caminho_scripts = config['Paths']['caminho_scripts_baselimpa']

            # Adicionar caminho dos scripts ao sys.path
            if caminho_scripts and caminho_scripts not in sys.path:
                sys.path.append(caminho_scripts)

            modulo = importlib.import_module(sistema)
            print(f"Módulo {sistema} importado com sucesso!")

            # Acessar os objetos no módulo importado
            queries = {obj_name: getattr(modulo, obj_name, None) for obj_name in [
                'create_dblink_query','conexao_dblink' , 'limpza_base_destino_query', 'versao_bases_query', 'query_cest','query_cfop',
                'query_curvaabc', 'query_dcb', 'query_feriado',
                'query_estado', 'query_cidade', 'query_bairro', 'query_logradouro','query_pessoa',
                'query_endereco', 'query_perfilcofins', 'query_perfilicms',
                'query_perfilpis', 'query_totalizadorfiscal', 'query_fabricante', 'query_nbm',
                'query_ncm', 'query_gruporemarcacao', 'query_principioativo',
                'query_produto', 'query_classificacao', 'query_classificacaoproduto',
                'query_embalagem', 'query_formapagamento', 'query_icmsestadual', 'query_icmsinterestadual',
                'query_naturezaoperacao', 'query_naturezaoperacaoperfilicms', 'query_icmsproduto', 'query_contato', 'query_fornecedor',
                'query_embalagemfornecedor','query_restricaocheque','query_cadernooferta','query_cadernoofertafaixaparcelamento','query_combooferta','query_condicaopagamento','query_parcelacondicaopagamento',
                'query_condicaofornecedor','query_condicaofornecedoritem','query_itemcadernooferta','query_itemcadernoofertaquantidade','query_grupocompra',
                'query_grupocompraitem','query_planoremuneracao','query_planoremuneracaobonificacaoembalagem','query_planoremuneracaocomissaoclassificacao',
                'query_planoremuneracaocomissaoembalagem','query_custoproduto','query_classificacaocurvaabcunidadenegocio',
                'query_validacao','query_ajuste_unidade','alteracaoProdutoConfig',
                'ativarProdutos','configUnidadeNegocio','tarefaAgendanda','estoqueSetar','suspenderBusca',
                'limpezaLogSinc','cfopOperacaoFiscal1','cfopOperacaoFiscal2','cfopOperacaoFiscal3','query_proxima_semente',
                'alterarVinculoProdutoReferenciaNaoE','excluir_gerador','criaSequencia','alterar_etiqueta_duplicada','reinicia_jboss','remove_sequencia'
            ]}
            return queries

        except ModuleNotFoundError:
            print(f"Módulo {sistema} não encontrado. Certifique-se de que o nome está correto e que o módulo existe.")
        except Exception as e:
            print(f"Ocorreu um erro ao importar o módulo: {e}", exc_info=True)

        return {}

    # Dicionário mapeando as consultas para nomes legíveis
    query_names = {
        'limpza_base_destino_query': "Limpeza Base Destino",
        'versao_bases_query': "Versão das Bases",
        'query_cest': "Query CEST",
        'query_curvaabc': "Query Curva ABC",
        'query_dcb': "Query DCB",
        'query_feriado': "Query Feriado",
        'query_estado': "Query Estado",
        'query_cidade': "Query Cidade",
        'query_bairro': "Query Bairro",
        'query_logradouro': "Query Logradouro",
        'query_pessoa': "Query Pessoa",
        'query_endereco': "Query Endereço",
        'query_perfilcofins': "Query Perfil COFINS",
        'query_perfilicms': "Query Perfil ICMS",
        'query_perfilpis': "Query Perfil PIS",
        'query_totalizadorfiscal': "Query Totalizador Fiscal",
        'query_fabricante': "Query Fabricante",
        'query_nbm': "Query NBM",
        'query_ncm': "Query NCM",
        'query_gruporemarcacao': "Query Grupo Remarcação",
        'query_principioativo': "Query Princípio Ativo",
        'query_produto': "Query Produto",
        'query_classificacao': "Query Classificação",
        'query_classificacaoproduto': "Query Classificação Produto",
        'query_embalagem': "Query Embalagem",
        'query_formapagamento': "Query Forma de Pagamento",
        'query_icmsestadual': "Query ICMS Estadual",
        'query_icmsinterestadual': "Query ICMS Interestadual",
        'query_naturezaoperacao': "Query Natureza da Operação",
        'query_naturezaoperacaoperfilicms': "Query Perfil ICMS Natureza da Operação",
        'query_icmsproduto': "Query ICMS Produto",
        'query_contato': "Query Contato",
        'query_fornecedor': "Query Fornecedor",
        'query_embalagemfornecedor': "Query Embalagem Fornecedor",
        'query_restricaocheque': "Restrição Cheque",
        'query_cadernooferta': "Caderno de Oferta",
        'query_cadernoofertafaixaparcelamento': "Caderno de Oferta Faixa Parcelamento",
        'query_combooferta': "Combo de Oferta",
        'query_condicaopagamento': "Condição de Pagamento",
        'query_parcelacondicaopagamento': "Parcela Condição de Pagamento",
        'query_condicaofornecedor': "Condição de Fornecedor",
        'query_condicaofornecedoritem': "Condição de Fornecedor Item",
        'query_itemcadernooferta': "Item Caderno de Oferta",
        'query_itemcadernoofertaquantidade': "Item Caderno de Oferta Quantidade",
        'query_grupocompra': "Grupo de Compra",
        'query_grupocompraitem': "Grupo de Compra Item",
        'query_planoremuneracao': "Plano de Remuneração",
        'query_planoremuneracaobonificacaoembalagem': "Plano de Remuneração Bonificação Embalagem",
        'query_planoremuneracaocomissaoclassificacao': "Plano de Remuneração Comissão Classificação",
        'query_planoremuneracaocomissaoembalagem': "Plano de Remuneração Comissão Embalagem",
        'query_custoproduto': "Custo do Produto",
        'query_classificacaocurvaabcunidadenegocio': "Classificação Curva ABC Unidade de Negócio",
        'query_validacao': "Query Validação",
        'query_ajuste_unidade': "Ajuste de Unidade",
        'query_custo': "Query Custo",
        'alteracaoProdutoConfig': "Alteração Produto Configuração",
        'ativarProdutos': "Ativar Produtos",
        'configUnidadeNegocio': "Configuração Unidade de Negócio",
        'tarefaAgendanda': "Tarefa Agendada",
        'estoqueSetar': "Setar Estoque",
        'suspenderBusca': "Suspender Busca",
        'limpezaLogSinc': "Limpeza Log Sincronização",
        'query_proxima_semente': "Query Próxima Semente"
    }

    def baseLimpaPadrao(self,conn_chinchila,conn_postgres,opcao_script,origem):
        try:
        
            with conn_chinchila as source_conn, conn_postgres as source_conn_postgres:
                print("Iniciando Base Limpa!")

                with source_conn.cursor() as source_cursor, source_conn_postgres.cursor() as source_cursor_postgres:
                    queries = self.importSistemaBaseLimpa(opcao_script)

                    host = origem

                    ssh = ConexaoSSH(origem)
                    dbNomeOrigem = ssh.get_database_name(username='alpha7', password='supertux')
                    if not dbNomeOrigem:
                        print("Falha ao obter o nome da base de dados via SSH.")
                        return
                    print(f"Nome da base de dados obtido: {dbNomeOrigem}")
    
                    dblink_params = f"host={host} user=chinchila password=chinchila dbname={dbNomeOrigem}"

                    print(f"{dblink_params}")

                    source_cursor_postgres.execute(queries['create_dblink_query'])
                    print("Query create_dblink_query executada com sucesso!")
                    source_conn_postgres.commit()

                    source_cursor.execute(queries['conexao_dblink'],(dblink_params,))
                    print("Query conexao_dblink executada com sucesso!")

                    source_cursor.execute(queries['versao_bases_query'])
                    print("Query versao_bases_query executada com sucesso!")
                    versao = source_cursor.fetchone()[0]
                    print(f"\033[92m{versao}\033[0m")

                    queries_to_run = [
                        'limpza_base_destino_query','query_cest','query_cfop','query_curvaabc', 'query_dcb', 'query_feriado',
                        'query_estado', 'query_cidade', 'query_bairro', 'query_logradouro','query_pessoa',
                        'query_endereco', 'query_perfilcofins', 'query_perfilicms',
                        'query_perfilpis', 'query_totalizadorfiscal', 'query_fabricante', 'query_nbm',
                        'query_ncm', 'query_gruporemarcacao', 'query_principioativo',
                        'query_produto', 'query_classificacao', 'query_classificacaoproduto',
                        'query_embalagem', 'query_formapagamento', 'query_icmsestadual', 'query_icmsinterestadual',
                        'query_naturezaoperacao', 'query_naturezaoperacaoperfilicms', 'query_icmsproduto', 'query_contato', 'query_fornecedor',
                        'query_embalagemfornecedor'
                    ]
                    
                    for query_key in queries_to_run:
                        query = queries.get(query_key)
                        query_name = self.query_names.get(query_key, "Consulta Desconhecida")

                        if query:
                            try:
                                source_cursor.execute(query)
                                source_conn.commit()

                                print(f"Query realizado com sucesso para {query_name}")

                            except psycopg2.Error as e:
                                source_conn.rollback()
                                print(f"Erro ao executar a consulta {query_name}: {e}")
                                if print(f"Erro ao executar a consulta {query_name}: {e}\nCorrija o problema e clique em OK para continuar..."):
                                    return True

                    def imprimir_em_blocos(df, bloco=6):
                        colunas = df.columns.tolist()
                        for i in range(0, len(colunas), bloco):
                            df_temp = df[colunas[i:i+bloco]]
                            print(tabulate(df_temp, headers='keys', tablefmt='fancy_grid', showindex=False))
                            print("\n")

                    source_cursor.execute(queries['query_validacao'])
                    print("\n\033[92m✔ Query 'query_validacao' executada com sucesso!\033[0m")
                    validacao = source_cursor.fetchall()
                    colunas = [desc[0] for desc in source_cursor.description]

                    # DataFrame
                    df = pd.DataFrame(validacao, columns=colunas)

                    # Imprimir em blocos de 8 colunas
                    imprimir_em_blocos(df, bloco=12)

                    queries_to_run_2 = [
                        'query_ajuste_unidade','query_custo', 'alteracaoProdutoConfig', 
                        'configUnidadeNegocio', 'tarefaAgendanda', 'estoqueSetar', 'suspenderBusca',
                        'limpezaLogSinc','ativarProdutos',
                    ]
                    
                    for query_key_2 in queries_to_run_2:
                        query_2 = queries.get(query_key_2)
                        query_name = self.query_names.get(query_key_2, "Consulta Desconhecida")

                        if query_2:
                            try:
                                source_cursor.execute(query_2)
                                source_conn.commit()

                                # Log após o INSERT
                                print(f"Query realizado com sucesso para {query_name}")

                            except psycopg2.Error as e:
                                print(f"Erro ao executar a consulta {query_name}: {e}")

                    source_cursor.execute(queries['cfopOperacaoFiscal1'])
                    source_cursor.execute(queries['cfopOperacaoFiscal2'])
                    cfop = source_cursor.fetchall()
                    for row in cfop:
                        nq_cfop = row[0]
                        source_cursor.execute(nq_cfop)
                    source_cursor.execute(queries['cfopOperacaoFiscal3'])
                    print("Query realizado com sucesso para CFOP Operação")

                    source_cursor.execute(queries['alterarVinculoProdutoReferenciaNaoE'])
                    alterar = source_cursor.fetchall()
                    for row in alterar:
                        nq_alterar = row[0]
                        source_cursor.execute(nq_alterar)
                    print("Query realizado com sucesso para alterarVinculoProdutoReferenciaNaoE")

                    source_cursor.execute(queries['excluir_gerador'])
                    excluir_gerador = source_cursor.fetchall()
                    for row in excluir_gerador:
                        nq_excluir_gerador = row[0]
                        source_cursor.execute(nq_excluir_gerador)
                    print("Query realizado com sucesso para excluir_gerador")

                    source_cursor.execute(queries['criaSequencia'])
                    print("Query realizado com sucesso para criaSequencia")

                    source_cursor.execute(queries['alterar_etiqueta_duplicada'])
                    alterar_etiqueta_duplicada = source_cursor.fetchall()
                    for row in alterar_etiqueta_duplicada:
                        nq_alterar_etiqueta_duplicada = row[0]
                        source_cursor.execute(nq_alterar_etiqueta_duplicada)
                    print("Query realizado com sucesso para alterar_etiqueta_duplicada")

                    source_cursor.execute(queries['reinicia_jboss'])
                    print("Query realizado com sucesso para reinicia_jboss")

                    source_cursor.execute(queries['remove_sequencia'])
                    print("Query realizado com sucesso para remove_sequencia")
                    
                    source_cursor.execute(queries['query_proxima_semente'])
                    semente = source_cursor.fetchone()[0]
                    print(f"Próxima semente que deve ser rodada: {semente}")
                    source_conn.commit()

        except psycopg2.Error as e:
            print(f"Ocorreu um erro: {e}")

    def baseLimpaPoupaqui(self,conn_chinchila,conn_postgres,opcao_script,origem):
        try:
            with conn_chinchila as source_conn, conn_postgres as source_conn_postgres:
                print("Iniciando Base Limpa PoupAqui!")

                with source_conn.cursor() as source_cursor, source_conn_postgres.cursor() as source_cursor_postgres:
                    queries_poupaqui = self.importSistemaBaseLimpaPoupaAqui(opcao_script)
                    
                    host = origem
                    ssh = ConexaoSSH(origem)
                    dbNomeOrigem = ssh.get_database_name(username='alpha7', password='supertux')
                    if not dbNomeOrigem:
                        print("Falha ao obter o nome da base de dados via SSH.")
                        return
                    print(f"Nome da base de dados obtido: {dbNomeOrigem}")

                    dblink_params = f"host={host} user=chinchila password=chinchila dbname={dbNomeOrigem}"

                    source_cursor_postgres.execute(queries_poupaqui['create_dblink_query'])
                    print("Query create_dblink_query executada com sucesso!")
                    source_conn_postgres.commit()

                    source_cursor.execute(queries_poupaqui['conexao_dblink'],(dblink_params,))
                    print("Query conexao_dblink executada com sucesso!")

                    source_cursor.execute(queries_poupaqui['versao_bases_query'])
                    print("Query versao_bases_query executada com sucesso!")
                    versao = source_cursor.fetchone()[0]
                    print(f"{versao}")

                    queries_poupaqui_to_run = [
                        'limpza_base_destino_query','query_cest','query_cfop','query_curvaabc', 'query_dcb', 'query_feriado',
                        'query_estado', 'query_cidade', 'query_bairro', 'query_logradouro','query_pessoa',
                        'query_endereco', 'query_perfilcofins', 'query_perfilicms',
                        'query_perfilpis', 'query_totalizadorfiscal', 'query_fabricante', 'query_nbm',
                        'query_ncm', 'query_gruporemarcacao', 'query_principioativo',
                        'query_produto', 'query_classificacao', 'query_classificacaoproduto',
                        'query_embalagem', 'query_formapagamento', 'query_icmsestadual', 'query_icmsinterestadual',
                        'query_naturezaoperacao', 'query_naturezaoperacaoperfilicms', 'query_icmsproduto', 'query_contato', 'query_fornecedor','query_embalagemfornecedor',
                        'query_restricaocheque','query_cadernooferta','query_cadernoofertafaixaparcelamento','query_combooferta','query_condicaopagamento','query_parcelacondicaopagamento',
                        'query_condicaofornecedor','query_condicaofornecedoritem','query_itemcadernooferta','query_itemcadernoofertaquantidade','query_grupocompra',
                        'query_grupocompraitem','query_planoremuneracao','query_planoremuneracaobonificacaoembalagem','query_planoremuneracaocomissaoclassificacao',
                        'query_planoremuneracaocomissaoembalagem','query_custoproduto','query_classificacaocurvaabcunidadenegocio'
                    ]
                    
                    for query_key in queries_poupaqui_to_run:
                        queries_poupaqui_run = queries_poupaqui.get(query_key)
                        query_name = self.query_names.get(query_key, "Consulta Desconhecida")

                        if queries_poupaqui_run:
                            try:
                                source_cursor.execute(queries_poupaqui_run)
                                source_conn.commit()

                                print(f"Query realizado com sucesso para {query_name}")

                            except psycopg2.Error as e:
                                source_conn.rollback()
                                print(f"Erro ao executar a consulta {query_name}: {e}")
                                if print(f"Erro ao executar a consulta {query_name}: {e}\nCorrija o problema e clique em OK para continuar..."):
                                    return True
                                
                    def imprimir_em_blocos(df, bloco=6):
                        colunas = df.columns.tolist()
                        for i in range(0, len(colunas), bloco):
                            df_temp = df[colunas[i:i+bloco]]
                            print(tabulate(df_temp, headers='keys', tablefmt='fancy_grid', showindex=False))
                            print("\n")

                    source_cursor.execute(queries_poupaqui['query_validacao'])
                    print("\n\033[92m✔ Query 'query_validacao' executada com sucesso!\033[0m")
                    validacao = source_cursor.fetchall()
                    colunas = [desc[0] for desc in source_cursor.description]

                    # DataFrame
                    df = pd.DataFrame(validacao, columns=colunas)

                    # Imprimir em blocos de 8 colunas
                    imprimir_em_blocos(df, bloco=9)

                    queries_to_run_2 = [
                        'query_ajuste_unidade', 'alteracaoProdutoConfig', 'ativarProdutos',
                        'configUnidadeNegocio', 'tarefaAgendanda', 'estoqueSetar', 'suspenderBusca','limpezaLogSinc'
                    ]
                    
                    for query_key_2 in queries_to_run_2:
                        query_2_poupaqui = queries_poupaqui.get(query_key_2)
                        query_name = self.query_names.get(query_key_2, "Consulta Desconhecida")

                        if query_2_poupaqui:
                            try:
                                source_cursor.execute(query_2_poupaqui)
                                source_conn.commit()

                                # Log após o INSERT
                                print(f"Query realizado com sucesso para {query_name}")

                            except psycopg2.Error as e:
                                print(f"Erro ao executar a consulta {query_name}: {e}")

                    source_cursor.execute(queries_poupaqui['cfopOperacaoFiscal1'])
                    source_cursor.execute(queries_poupaqui['cfopOperacaoFiscal2'])
                    cfop = source_cursor.fetchall()
                    for row in cfop:
                        nq_cfop = row[0]
                        source_cursor.execute(nq_cfop)
                    source_cursor.execute(queries_poupaqui['cfopOperacaoFiscal3'])
                    print("Query realizado com sucesso para CFOP Operação")

                    source_cursor.execute(queries_poupaqui['alterarVinculoProdutoReferenciaNaoE'])
                    alterar = source_cursor.fetchall()
                    for row in alterar:
                        nq_alterar = row[0]
                        source_cursor.execute(nq_alterar)
                    print("Query realizado com sucesso para alterarVinculoProdutoReferenciaNaoE")

                    source_cursor.execute(queries_poupaqui['excluir_gerador'])
                    excluir_gerador = source_cursor.fetchall()
                    for row in excluir_gerador:
                        nq_excluir_gerador = row[0]
                        source_cursor.execute(nq_excluir_gerador)
                    print("Query realizado com sucesso para excluir_gerador")

                    source_cursor.execute(queries_poupaqui['criaSequencia'])
                    print("Query realizado com sucesso para criaSequencia")

                    source_cursor.execute(queries_poupaqui['alterar_etiqueta_duplicada'])
                    alterar_etiqueta_duplicada = source_cursor.fetchall()
                    for row in alterar_etiqueta_duplicada:
                        nq_alterar_etiqueta_duplicada = row[0]
                        source_cursor.execute(nq_alterar_etiqueta_duplicada)
                    print("Query realizado com sucesso para alterar_etiqueta_duplicada")

                    source_cursor.execute(queries_poupaqui['reinicia_jboss'])
                    print("Query realizado com sucesso para reinicia_jboss")

                    source_cursor.execute(queries_poupaqui['remove_sequencia'])
                    print("Query realizado com sucesso para remove_sequencia")
                    
                    source_cursor.execute(queries_poupaqui['query_proxima_semente'])
                    semente = source_cursor.fetchone()[0]
                    print(f"Próxima semente que deve ser rodada: {semente}")

                    source_conn.commit()

        except psycopg2.Error as e:
            print(f"Ocorreu um erro: {e}")
