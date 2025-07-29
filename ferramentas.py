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