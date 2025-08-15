import subprocess
import requests
import xml.etree.ElementTree as ET
import logging

class executarComando:
    def executarComando(self, destino, senha, comando):
        try:
            comando_ssh = f"sshpass -p '{senha}' ssh -tt alpha7@{destino} \"{comando}\""
            logging.info(f"\033[94m→ Executando: {comando}\033[0m")

            resultado = subprocess.run(comando_ssh, shell=True, text=True)

            if resultado.returncode == 0:
                logging.info("\033[92m✔ Comando executado com sucesso.\033[0m")
            else:
                logging.error(f"\033[91m✖ Comando retornou erro. Código: {resultado.returncode}\033[0m")

        except Exception as e:
            logging.error(f"\033[91m✖ Erro ao executar o comando: {e}\033[0m")

class atualizacaoServidor:
    def obter_nome_arquivo(self,versao_servidor_procurada):

        url_xml = "http://update.a7.net.br/atualizacoes.xml"
        # Faz o download do XML
        response = requests.get(url_xml)
        response.raise_for_status()  # levanta erro se não conseguir baixar
        
        # Converte o conteúdo para ElementTree
        root = ET.fromstring(response.content)

        # Percorre os pacotes
        for pacote in root.findall("pacote"):
            versao = pacote.find("versaoServidor")
            if versao is not None and versao.text == versao_servidor_procurada:
                nome_arquivo = pacote.find("nomeArquivo")
                if nome_arquivo is not None:
                    return nome_arquivo.text
        return None
    
class aplicar_semente_update_codigo:
    def aplicar_semente_e_codigo(self,conn_chinchila, semente: int, destino: str):
        """
        conn_chinchila:   conexão do app (ex.: conn_chinchila) p/ atualizar unidade de negócio
        semente:    inteiro informado pelo usuário
        destino:    string no formato "GRUPO-CODIGO" (ex.: "A7-1234")
        """
        valor_semente = int(semente) * 50_000_000

        # 1) Atualizar sequences e geradorid (transação 1 – admin)
        try:
            with conn_chinchila:
                with conn_chinchila.cursor() as cur:
                    # setval(seq_geradorid)
                    cur.execute(
                        "SELECT setval('public.seq_geradorid', %s, true);",
                        (valor_semente,)
                    )
                    # UPDATE geradorid
                    cur.execute("UPDATE geradorid SET valorid = ultimovalorid + 1;")
                    # resetar outras sequences
                    cur.execute("SELECT setval('public.seq_revisaolog', 1, true);")
                    cur.execute("SELECT setval('public.seq_transacao', 1, true);")

            logging.info("\033[92m✓ Semente e sequências atualizadas com sucesso (local)\033[0m")

        except Exception as e:
            logging.exception("Falha ao aplicar semente/seq localmente.")
            # conn_admin gerencia commit/rollback via context manager; aqui apenas propago
            raise

        # 2) Atualizar código do cliente (transação 2 – app)
        try:
            grupocliente, codigocliente = destino.split("-", 1)
        except ValueError:
            raise ValueError(
                f"Destino '{destino}' inválido. Esperado formato 'GRUPO-CODIGO' (ex.: '0000-1234')."
            )

        try:
            with conn_chinchila:
                with conn_chinchila.cursor() as cur:
                    cur.execute(
                        """
                        UPDATE unidadenegocio
                        SET grupoclientealpha7 = %s,
                            codigoclientealpha7 = %s
                        WHERE id = 1;
                        """,
                        (grupocliente, codigocliente)
                    )

            logging.info("\033[92m✓ Código do cliente atualizado em unidadenegocio (local)\033[0m")

        except Exception as e:
            logging.exception("Falha ao atualizar código do cliente (unidadenegocio).")
            raise