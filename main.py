from conexaoPG import Postgres
from processoBaseLimpa import importacaoBaseLimpa
from ferramentas import atualizacaoServidor, executarComando
from transferBackup import Backup, Transfer
import time
import logging

# Configuração do logging
logging.basicConfig(
    filename='raposa_baselimpa.log',
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s'
)
console = logging.StreamHandler()
console.setLevel(logging.INFO)
formatter = logging.Formatter('%(asctime)s [%(levelname)s] %(message)s')
console.setFormatter(formatter)
logging.getLogger().addHandler(console)

def exibir_menu():
    while True:
        
        logging.info("\033[92mOlá, bem-vinda! Selecione uma das funções abaixo:\033[0m")
        logging.info("1 - Base Limpa Padrão")
        logging.info("2 - Base Limpa Padrão Produtos Ativo/Inativos")
        logging.info("3 - Base Limpa Poupaqui")
        logging.info("4 - Base Limpa Poupaqui Produtos Ativo/Inativos")
        logging.info("5 - Exit")

        opcao = input("Digite o número da opção desejada: ")

        if opcao == "1":
            inicio = time.time()
            logging.info("Você selecionou: Base Limpa Padrão")
            destino = codigoDestino()
            origem = codigoOrigem()
            logging.info(f"Base de Origem : {origem}  |  Base de Destino: {destino}")
            if not continuar():
                return

            # Instanciar o objeto de conexão e criar a base
            base_oficial = Postgres()
            db_name = base_oficial.criar_base_oficial(destino)
            conn_chinchila = base_oficial.conexaoBaseOficialChinchila(db_name)
            conn_postgres = base_oficial.conexaoBaseOficialPostgres(db_name)

            # Verificar se a conexão foi bem-sucedida
            if not conn_chinchila:
                logging.error("Erro na conexão com a base de destino.")
                return
            
            if not conn_postgres:
                logging.error("Erro na conexão com a base de destino.")
                return

            # Instanciar a classe que cuida da base limpa
            base_limpa = importacaoBaseLimpa()
            opcao_script = "baseLimpaPadrao" 

            # Executar a população da base limpa
            base_limpa.baseLimpaPadrao(conn_chinchila,conn_postgres, opcao_script, origem)

            if not continuar():
                return

            fazerBackup = Backup()
            arquivo_gerado = fazerBackup.fazendoBackup(db_name)

            if not arquivo_gerado:
                logging.error("\033[91m✖ Backup falhou. A transferência será ignorada.\033[0m")
                return
            
            fazerBackup.conferirMd5sum(arquivo_gerado)

            logging.info(f"Iniciando a transferencia para o Servidor de destino {destino}")
            transfeBackup = Transfer()
            transfeBackup.baseTransfer(destino, arquivo_gerado)

            sshComandos = executarComando()
            senha = 'supertux'
            sshComandos.executarComando(destino,senha,"sudo service wildfly stop")
            sshComandos.executarComando(destino,senha,"cd /home/alpha7/shared && wget http://a7.net.br/scherrer/restaurar_base2.sh")
            sshComandos.executarComando(destino,senha,"cd /home/alpha7/shared && chmod a+x restaurar_base2.sh;")
            sshComandos.executarComando(destino,senha,"cd /home/alpha7/shared && bash restaurar_base2.sh;")

            confirmar = input("Iniciando a atualização do servidor. Deseja continuar? (s/n): ").strip().lower()

            if confirmar == "s":
                versao = input("Qual versão deseja atualizar?: ")
                at_versao = atualizacaoServidor()
                nome_arquivo = at_versao.obter_nome_arquivo(versao)

                if nome_arquivo:
                    logging.info(f"Nome do arquivo para versão {versao}: {nome_arquivo}")
                    
                    sshComandos.executarComando(destino, senha, 
                        "cd /home/alpha7/shared && wget a7.net.br/scherrer/aplicarAtualizacao.sh;")
                    sshComandos.executarComando(destino, senha, 
                        f"cd /home/alpha7/shared && bash aplicarAtualizacao.sh {nome_arquivo};")
                    logging.info("Atualização concluída com sucesso!")
                else:
                    logging.warning(f"Versão {versao} não encontrada.")
            else:
                logging.warning("Atualização cancelada pelo usuário.")

            # Atualizando a semente no cliente, precisa prestar atenção para que coloque a versão certa
            logging.info("Atualizando a Semente")

            semente = input("Qual a semente que devemos rodar: ")
            db_nome = input("Qual nome da base de dados: ")

            sshComandos.executarComando(destino, senha, f"""
            psql -U chinchila -d {db_nome} -c "SELECT setval('public.seq_geradorid', {semente} * 50000000, true); UPDATE geradorid SET valorid=ultimovalorid + 1; SELECT setval('public.seq_revisaolog', 1, true); SELECT setval('public.seq_transacao', 1, true);"
            """)
            logging.info("\033[92mAtualizado a Semente com sucesso\033[0m")

            # Atualizando o codigo do cliente na base de dados
            logging.info("Atualizando o codigo do cliente na base de dados")
            grupocliente, codigocliente = destino.split("-")

            sshComandos.executarComando(destino, senha, f"""
            psql -U chinchila -d {db_nome} -c "UPDATE unidadenegocio SET grupoclientealpha7 = '{grupocliente}',codigoclientealpha7='{codigocliente} WHERE id = 1"
            """)

            sshComandos.executarComando(destino,senha,"sudo vim /etc/wildfly.conf")
            sshComandos.executarComando(destino,senha,"sudo service wildfly start")

            logging.info("processo concluído com sucesso!")
            fim = time.time()
            duracao = fim - inicio
            print(f"\033[96mTempo de execução: {duracao:.2f} segundos\033[0m")
            break

        elif opcao == "2":

            inicio = time.time()
            logging.info("Você selecionou: Base Limpa Padrão Produtos Ativo/Inativos")
            destino = codigoDestino()
            origem = codigoOrigem()
            logging.info(f"Base de Origem : {origem}  |  Base de Destino: {destino}")
            if not continuar():
                return

            # Instanciar o objeto de conexão e criar a base
            base_oficial = Postgres()
            db_name = base_oficial.criar_base_oficial(destino)
            conn_chinchila = base_oficial.conexaoBaseOficialChinchila(db_name)
            conn_postgres = base_oficial.conexaoBaseOficialPostgres(db_name)

            # Verificar se a conexão foi bem-sucedida
            if not conn_chinchila:
                logging.error("Erro na conexão com a base de destino.")
                return
            
            if not conn_postgres:
                logging.error("Erro na conexão com a base de destino.")
                return

            # Instanciar a classe que cuida da base limpa
            base_limpa = importacaoBaseLimpa()
            opcao_script = "baseLimpaPadraoProdutosInativos" 

            # Executar a população da base limpa
            base_limpa.baseLimpaPadrao(conn_chinchila,conn_postgres, opcao_script, origem)

            if not continuar():
                return

            fazerBackup = Backup()
            arquivo_gerado = fazerBackup.fazendoBackup(db_name)

            if not arquivo_gerado:
                logging.error("\033[91m✖ Backup falhou. A transferência será ignorada.\033[0m")
                return
            
            fazerBackup.conferirMd5sum(arquivo_gerado)

            logging.info(f"Iniciando a transferencia para o Servidor de destino {destino}")
            transfeBackup = Transfer()
            transfeBackup.baseTransfer(destino, arquivo_gerado)

            sshComandos = executarComando()
            senha = 'supertux'
            sshComandos.executarComando(destino,senha,"sudo service wildfly stop")
            sshComandos.executarComando(destino,senha,"cd /home/alpha7/shared && wget http://a7.net.br/scherrer/restaurar_base2.sh")
            sshComandos.executarComando(destino,senha,"cd /home/alpha7/shared && chmod a+x restaurar_base2.sh;")
            sshComandos.executarComando(destino,senha,"cd /home/alpha7/shared && bash restaurar_base2.sh;")

            confirmar = input("Iniciando a atualização do servidor. Deseja continuar? (s/n): ").strip().lower()

            if confirmar == "s":
                versao = input("Qual versão deseja atualizar?: ")
                at_versao = atualizacaoServidor()
                nome_arquivo = at_versao.obter_nome_arquivo(versao)

                if nome_arquivo:
                    logging.info(f"Nome do arquivo para versão {versao}: {nome_arquivo}")
                    
                    sshComandos.executarComando(destino, senha, 
                        "cd /home/alpha7/shared && wget a7.net.br/scherrer/aplicarAtualizacao.sh;")
                    sshComandos.executarComando(destino, senha, 
                        f"cd /home/alpha7/shared && bash aplicarAtualizacao.sh {nome_arquivo};")
                    logging.warning("Atualização concluída com sucesso!")
                else:
                    logging.warning(f"Versão {versao} não encontrada.")
            else:
                logging.info("Atualização cancelada pelo usuário.")

            # Atualizando a semente no cliente, precisa prestar atenção para que coloque a versão certa
            logging.info("Atualizando a Semente")

            semente = input("Qual a semente que devemos rodar: ")
            db_nome = input("Qual nome da base de dados: ")

            sshComandos.executarComando(destino, senha, f"""
            psql -U chinchila -d {db_nome} -c "SELECT setval('public.seq_geradorid', {semente} * 50000000, true); UPDATE geradorid SET valorid=ultimovalorid + 1; SELECT setval('public.seq_revisaolog', 1, true); SELECT setval('public.seq_transacao', 1, true);"
            """)
            logging.info("\033[92mAtualizado a Semente com sucesso\033[0m")

            # Atualizando o codigo do cliente na base de dados
            logging.info("Atualizando o codigo do cliente na base de dados")
            grupocliente, codigocliente = destino.split("-")

            sshComandos.executarComando(destino, senha, f"""
            psql -U chinchila -d {db_nome} -c "UPDATE unidadenegocio SET grupoclientealpha7 = '{grupocliente}',codigoclientealpha7='{codigocliente} WHERE id = 1"
            """)

            sshComandos.executarComando(destino,senha,"sudo vim /etc/wildfly.conf")
            sshComandos.executarComando(destino,senha,"sudo service wildfly start")

            logging.info("processo concluído com sucesso!")
            fim = time.time()
            duracao = fim - inicio
            logging.info(f"\033[96mTempo de execução: {duracao:.2f} segundos\033[0m")
            break

        elif opcao == "3":
            logging.info("Você selecionou: Base Limpa Poupaqui")
            
            inicio = time.time()
            destino = codigoDestino()
            origem = codigoOrigem()
            logging.info(f"Base de Origem : {origem}  |  Base de Destino: {destino}")
            if not continuar():
                return

            # Instanciar o objeto de conexão e criar a base
            base_oficial = Postgres()
            db_name = base_oficial.criar_base_oficial(destino)
            conn_chinchila = base_oficial.conexaoBaseOficialChinchila(db_name)
            conn_postgres = base_oficial.conexaoBaseOficialPostgres(db_name)

            # Verificar se a conexão foi bem-sucedida
            if not conn_chinchila:
                logging.error("Erro na conexão com a base de destino.")
                return
            
            if not conn_postgres:
                logging.error("Erro na conexão com a base de destino.")
                return

            # Instanciar a classe que cuida da base limpa
            base_limpa = importacaoBaseLimpa()
            opcao_script = "baseLimpaPoupAqui" 

            # Executar a população da base limpa
            base_limpa.baseLimpaPoupaqui(conn_chinchila,conn_postgres, opcao_script, origem)

            if not continuar():
                return

            fazerBackup = Backup()
            arquivo_gerado = fazerBackup.fazendoBackup(db_name)

            if not arquivo_gerado:
                logging.error("\033[91m✖ Backup falhou. A transferência será ignorada.\033[0m")
                return
            
            fazerBackup.conferirMd5sum(arquivo_gerado)

            logging.info(f"Iniciando a transferencia para o Servidor de destino {destino}")
            transfeBackup = Transfer()
            transfeBackup.baseTransfer(destino, arquivo_gerado)

            sshComandos = executarComando()
            senha = 'supertux'
            sshComandos.executarComando(destino,senha,"sudo service wildfly stop")
            sshComandos.executarComando(destino,senha,"cd /home/alpha7/shared && wget http://a7.net.br/scherrer/restaurar_base2.sh")
            sshComandos.executarComando(destino,senha,"cd /home/alpha7/shared && chmod a+x restaurar_base2.sh;")
            sshComandos.executarComando(destino,senha,"cd /home/alpha7/shared && bash restaurar_base2.sh;")

            confirmar = input("Iniciando a atualização do servidor. Deseja continuar? (s/n): ").strip().lower()

            if confirmar == "s":
                versao = input("Qual versão deseja atualizar?: ")
                at_versao = atualizacaoServidor()
                nome_arquivo = at_versao.obter_nome_arquivo(versao)

                if nome_arquivo:
                    logging.info(f"Nome do arquivo para versão {versao}: {nome_arquivo}")
                    
                    sshComandos.executarComando(destino, senha, 
                        "cd /home/alpha7/shared && wget a7.net.br/scherrer/aplicarAtualizacao.sh;")
                    sshComandos.executarComando(destino, senha, 
                        f"cd /home/alpha7/shared && bash aplicarAtualizacao.sh {nome_arquivo};")
                    logging.info("Atualização concluída com sucesso!")
                else:
                    logging.warning(f"Versão {versao} não encontrada.")
            else:
                logging.info("Atualização cancelada pelo usuário.")

                # Atualizando a semente no cliente, precisa prestar atenção para que coloque a versão certa
            logging.info("Atualizando a Semente")

            semente = input("Qual a semente que devemos rodar: ")
            db_nome = input("Qual nome da base de dados: ")

            sshComandos.executarComando(destino, senha, f"""
            psql -U chinchila -d {db_nome} -c "SELECT setval('public.seq_geradorid', {semente} * 50000000, true); UPDATE geradorid SET valorid=ultimovalorid + 1; SELECT setval('public.seq_revisaolog', 1, true); SELECT setval('public.seq_transacao', 1, true);"
            """)
            logging.info("\033[92mAtualizado a Semente com sucesso\033[0m")

            # Atualizando o codigo do cliente na base de dados
            logging.info("Atualizando o codigo do cliente na base de dados")
            grupocliente, codigocliente = destino.split("-")

            sshComandos.executarComando(destino, senha, f"""
            psql -U chinchila -d {db_nome} -c "UPDATE unidadenegocio SET grupoclientealpha7 = '{grupocliente}',codigoclientealpha7='{codigocliente} WHERE id = 1"
            """)

            sshComandos.executarComando(destino,senha,"sudo vim /etc/wildfly.conf")
            sshComandos.executarComando(destino,senha,"sudo service wildfly start")

            logging.info("processo concluído com sucesso!")
            fim = time.time()
            duracao = fim - inicio
            logging.info(f"\033[96mTempo de execução: {duracao:.2f} segundos\033[0m")
            break

        elif opcao == "4":

            inicio = time.time()
            logging.info("Você selecionou: Base Limpa Poupaqui Produtos Ativo/Inativos")
            destino = codigoDestino()
            origem = codigoOrigem()
            logging.info(f"Base de Origem : {origem}  |  Base de Destino: {destino}")
            if not continuar():
                return

            # Instanciar o objeto de conexão e criar a base
            base_oficial = Postgres()
            db_name = base_oficial.criar_base_oficial(destino)
            conn_chinchila = base_oficial.conexaoBaseOficialChinchila(db_name)
            conn_postgres = base_oficial.conexaoBaseOficialPostgres(db_name)

            # Verificar se a conexão foi bem-sucedida
            if not conn_chinchila:
                logging.error("Erro na conexão com a base de destino.")
                return
            
            if not conn_postgres:
                logging.error("Erro na conexão com a base de destino.")
                return

            # Instanciar a classe que cuida da base limpa
            base_limpa = importacaoBaseLimpa()
            opcao_script = "baseLimpaPoupAquiProdutosInativos" 

            # Executar a população da base limpa
            base_limpa.baseLimpaPoupaqui(conn_chinchila,conn_postgres, opcao_script, origem)

            if not continuar():
                return

            fazerBackup = Backup()
            arquivo_gerado = fazerBackup.fazendoBackup(db_name)

            if not arquivo_gerado:
                logging.error("\033[91m✖ Backup falhou. A transferência será ignorada.\033[0m")
                return
            
            fazerBackup.conferirMd5sum(arquivo_gerado)

            logging.info(f"Iniciando a transferencia para o Servidor de destino {destino}")
            transfeBackup = Transfer()
            transfeBackup.baseTransfer(destino, arquivo_gerado)

            sshComandos = executarComando()
            senha = 'supertux'
            sshComandos.executarComando(destino,senha,"sudo service wildfly stop")
            sshComandos.executarComando(destino,senha,"cd /home/alpha7/shared && wget http://a7.net.br/scherrer/restaurar_base2.sh")
            sshComandos.executarComando(destino,senha,"cd /home/alpha7/shared && chmod a+x restaurar_base2.sh;")
            sshComandos.executarComando(destino,senha,"cd /home/alpha7/shared && bash restaurar_base2.sh;")

            confirmar = input("Iniciando a atualização do servidor. Deseja continuar? (s/n): ").strip().lower()

            if confirmar == "s":
                versao = input("Qual versão deseja atualizar?: ")
                at_versao = atualizacaoServidor()
                nome_arquivo = at_versao.obter_nome_arquivo(versao)

                if nome_arquivo:
                    logging.info(f"Nome do arquivo para versão {versao}: {nome_arquivo}")
                    
                    sshComandos.executarComando(destino, senha, 
                        "cd /home/alpha7/shared && wget a7.net.br/scherrer/aplicarAtualizacao.sh;")
                    sshComandos.executarComando(destino, senha, 
                        f"cd /home/alpha7/shared && bash aplicarAtualizacao.sh {nome_arquivo};")
                    logging.info("Atualização concluída com sucesso!")
                else:
                    logging.warning(f"Versão {versao} não encontrada.")
            else:
                logging.warning("Atualização cancelada pelo usuário.")

            # Atualizando a semente no cliente, precisa prestar atenção para que coloque a versão certa
            logging.info("Atualizando a Semente")

            semente = input("Qual a semente que devemos rodar: ")
            db_nome = input("Qual nome da base de dados: ")

            sshComandos.executarComando(destino, senha, f"""
            psql -U chinchila -d {db_nome} -c "SELECT setval('public.seq_geradorid', {semente} * 50000000, true); UPDATE geradorid SET valorid=ultimovalorid + 1; SELECT setval('public.seq_revisaolog', 1, true); SELECT setval('public.seq_transacao', 1, true);"
            """)
            logging.info("\033[92mAtualizado a Semente com sucesso\033[0m")

            # Atualizando o codigo do cliente na base de dados
            logging.info("Atualizando o codigo do cliente na base de dados")
            grupocliente, codigocliente = destino.split("-")

            sshComandos.executarComando(destino, senha, f"""
            psql -U chinchila -d {db_nome} -c "UPDATE unidadenegocio SET grupoclientealpha7 = '{grupocliente}',codigoclientealpha7='{codigocliente} WHERE id = 1"
            """)

            sshComandos.executarComando(destino,senha,"sudo vim /etc/wildfly.conf")
            sshComandos.executarComando(destino,senha,"sudo service wildfly start")
            
            logging.info("processo concluído com sucesso!")
            fim = time.time()
            duracao = fim - inicio
            logging.info(f"\033[96mTempo de execução: {duracao:.2f} segundos\033[0m")
            break
        
        elif opcao == "5":
            logging.info("Saindo")
            break

        else:
            logging.info("Opção inválida. Tente novamente.")

def codigoDestino():
    return input("Digite o codigo de destino: ")

def codigoOrigem():
    return input("Digite o codigo de origem: ")

def continuar():
    while True:
        seguir = input("\033[96mValide as informações e deseja prosseguir? (S/N): \033[0m").strip().lower()

        if seguir == 's':
            logging.info("Continuando...")
            return True
        elif seguir == 'n':
            logging.info("Operação cancelada.")
            return False
        else:
            logging.info("Opção inválida. Digite apenas S ou N.")

# Executa o menu
exibir_menu()
