import os
import subprocess
import logging
from config import carregar_configuracoes

class Backup:
    def __init__(self):
        pass

    def fazendoBackup(self, destino):
        config = carregar_configuracoes('config.ini')
        porta_pg = config['Database']['porta_pg']
        caminho_backup = config['Database']['caminho_backup']

        # Montar caminho completo do arquivo de backup
        caminho = os.path.join(caminho_backup, f"{destino}.backup")
        logging.info("Iniciando Backup")
        # Executar comando pg_dump
        try:
            subprocess.run(
                [
                    '/usr/lib/postgresql/14/bin/pg_dump',
                    '-h', 'localhost',
                    '-p', porta_pg,
                    '-U', 'chinchila',
                    '-F', 'c',
                    '-f', caminho,
                    destino  # nome da base
                ],
                check=True,
                env={"PGPASSWORD": "chinchila"},
                text=True
            )
            logging.info(f"\033[92m✔ Backup da base '{destino}' gerado com sucesso em: {caminho}\033[0m")
            return caminho
        except subprocess.CalledProcessError as e:
            logging.error(f"\033[91m✖ Erro ao gerar o backup da base '{destino}': {e}\033[0m")
            return None
    
    def conferirMd5sum(self,caminho):
        try:
            resultado = subprocess.run(
                ['md5sum', caminho],
                check=True,
                capture_output=True,
                text=True
            )
            hash_md5 = resultado.stdout.split()[0]
            logging.info(f"\033[92m✔ MD5 do arquivo '{caminho}': {hash_md5}\033[0m")
            return hash_md5

        except subprocess.CalledProcessError as e:
            logging.error(f"\033[91m✖ Erro ao calcular o MD5: {e}\033[0m")
            return None

class Transfer:
    def __init__(self):
        pass

    def baseTransfer(self, codigo_destino, caminho_backup):
        destino_path = f"alpha7@{codigo_destino}:/home/alpha7/shared"

        try:
            subprocess.run(
                ['scp', caminho_backup, destino_path],
                check=True
            )
            logging.info("\033[92m✔ Transferência feita com sucesso!\033[0m")
        except subprocess.CalledProcessError as e:
            logging.error(f"\033[91m✖ Erro na transferência via SCP: {e}\033[0m")
