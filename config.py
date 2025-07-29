import configparser
import os

# Função para carregar configurações
def carregar_configuracoes(arquivo_config):
    config = configparser.ConfigParser()
    if os.path.exists(arquivo_config):
        config.read(arquivo_config)
        return config
    else:
        raise FileNotFoundError(f"Arquivo de configuração '{arquivo_config}' não encontrado.\nAdicione o Arquivo e reinicie o programa")

    
