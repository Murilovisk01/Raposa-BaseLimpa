import paramiko
import logging
import re
from sshtunnel import SSHTunnelForwarder
import psycopg2
import subprocess


class ConexaoSSH:
    def __init__(self, clientea7):
        self.clientea7 = clientea7

    password = 'supertux'

    def get_codigo_a7(self):
        return self.clientea7.split(' | ')[0]

    def get_nick_conexao(self):
        d = {192: u'A', 193: u'A', 194: u'A', 195: u'A', 196: u'A', 197: u'A',
             199: u'C', 200: u'E', 201: u'E', 202: u'E', 203: u'E', 204: u'I',
             205: u'I', 206: u'I', 207: u'I', 209: u'N', 210: u'O', 211: u'O',
             212: u'O', 213: u'O', 214: u'O', 216: u'O', 217: u'U', 218: u'U',
             219: u'U', 220: u'U', 221: u'Y', 224: u'a', 225: u'a', 226: u'a',
             227: u'a', 228: u'a', 229: u'a', 231: u'c', 232: u'e', 233: u'e',
             234: u'e', 235: u'e', 236: u'i', 237: u'i', 238: u'i', 239: u'i',
             241: u'n', 242: u'o', 243: u'o', 244: u'o', 245: u'o', 246: u'o',
             248: u'o', 249: u'u', 250: u'u', 251: u'u', 252: u'u', 253: u'y',
             255: u'y'}

        codigoa7 = self.get_codigo_a7()
        nome_host = self.clientea7.split(' | ')[1]
        nome_conexao = codigoa7 + " " + nome_host
        nome_conexao = nome_conexao.translate(d)
        nome_conexao = nome_conexao.strip()
        nome_conexao = re.sub('[()\\s\\-\'|.]+', '.', nome_conexao)
        nome_conexao = re.sub('\\.$', '', nome_conexao)
        nome_conexao = re.sub('/', '-', nome_conexao)

        return nome_conexao

    def conecta_ssh(self, username='alpha7', password=None, key_filename=None):
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            logging.info(f"Tentando conectar ao SSH {self.get_codigo_a7()}")
            if key_filename:
                ssh.connect(self.get_codigo_a7(), username=username, key_filename=key_filename, timeout=10)
            else:
                ssh.connect(self.get_codigo_a7(), username=username, password=password, timeout=10)
            
            stdin, stdout, stderr = ssh.exec_command('echo "Conectado com sucesso!"')
            print(stdout.read().decode())
            logging.info(f"Conectado com sucesso ao SSH {self.get_codigo_a7()}")
            
        except Exception as e:
            logging.error(f"Falha na conexão: {e}")
        finally:
            ssh.close()

    def get_database_name(self, username='alpha7', password=None, key_filename=None):
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            if key_filename:
                ssh.connect(self.get_codigo_a7(), username=username, key_filename=key_filename, timeout=10)
            else:
                ssh.connect(self.get_codigo_a7(), username=username, password=password, timeout=10)
            
            command = "grep 'CHINCHILA_DS_DATABASENAME' /etc/wildfly.conf | cut -d'=' -f2"
            stdin, stdout, stderr = ssh.exec_command(command)
            database_name = stdout.read().decode().strip()
            return database_name
            
        except Exception as e:
            logging.error(f"Falha na conexão: {e}")
            return None
        finally:
            ssh.close()
    
    def transfer_ssh(self, username='alpha7', password=None, key_filename=None):
        ssh = paramiko.SSHClient()
        ssh.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        try:
            logging.info(f"Tentando conectar ao SSH {self.get_codigo_a7()}")
            if key_filename:
                ssh.connect(self.get_codigo_a7(), username=username, key_filename=key_filename, timeout=10)
            else:
                ssh.connect(self.get_codigo_a7(), username=username, password=password, timeout=10)
            
            stdin, stdout, stderr = ssh.exec_command('echo "Conectado com sucesso!"')
            print(stdout.read().decode())
            logging.info(f"Conectado com sucesso ao SSH {self.get_codigo_a7()}")
            
            # Retornar o objeto ssh para reutilização
            return ssh
            
        except Exception as e:
            logging.error(f"Falha na conexão: {e}")
            return None
        
    def conecta_postgres_com_tunnel(self, codigo_a7, db_nome):
        tunnel = None
        try:
            tunnel = SSHTunnelForwarder(
                (codigo_a7, 22),
                ssh_username='alpha7',
                ssh_password='supertux',
                remote_bind_address=('localhost', 5432),
                local_bind_address=('localhost', 5436)
            )
            tunnel.start()

            connection_chinchila = psycopg2.connect(
                database=db_nome,
                user='chinchila',
                password='chinchila',
                host='localhost',
                port=tunnel.local_bind_port
            )

            cursor_chinchila = connection_chinchila.cursor()

            logging.info("Conexão com PostgreSQL através de túnel SSH bem-sucedida!")
            return  connection_chinchila, cursor_chinchila, tunnel
        
        except Exception as e:
            logging.error(f"Falha na conexão com PostgreSQL através de túnel SSH: {e}")
            if tunnel:
                tunnel.stop()
            return None, None, None
        
    def conecta_ssh_hamster(self):
        cmd = ['xfce4-terminal',
               # se não desejar que o título seja atualizado com o PWD
               # '--dynamic-title-mode=none',
               '--initial-title=' + self.clientea7,
               '-e', 'ssh -X alpha7@' + self.get_codigo_a7()]
        subprocess.Popen(cmd, stdout=subprocess.PIPE)


