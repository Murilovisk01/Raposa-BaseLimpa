import importlib
import logging
import sys
import psycopg2

from config import carregar_configuracoes


class importacaoBrava:

    tabelas_names = {'fidelidadeclassificacao','crediario','itemcadernooferta'}

    colunas_names = """
    WITH tmp AS (
    SELECT
    *
    FROM dblink('conexao_baseorigem', '
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
        ) AS cols
    FROM information_schema.columns
    WHERE table_schema = ''public''
    AND table_name = ''planopagamento''
    ') AS DATA (cols VARCHAR)
    )
    SELECT 
    *
    FROM tmp
    """
    def importSistemaBaseLimpa(self,opcao_script):
        sistema = opcao_script

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
            logging.error(f"Módulo {sistema} não encontrado. Certifique-se de que o nome está correto e que o módulo existe.")
        except Exception as e:
            logging.inferroro(f"Ocorreu um erro ao importar o módulo: {e}", exc_info=True)

        return {}

    def get_colunas_names(self):
        return self.colunas_names
    
    def get_tabelas_names(self):
        return self.tabelas_names
    
    def processo_importacao(self,conn_chinchila,conn_postgres,opcao_script,origem):
        try:
            with conn_chinchila as source_conn, conn_postgres as source_conn_postgres:
                logging.info("Iniciando Base Limpa!")
        except psycopg2.Error as e:
            logging.info(f"Ocorreu um erro: {e}")