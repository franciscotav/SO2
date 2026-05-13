import sys
import os
import time
import threading
import logging

# Adicionar a diretoria principal ao path para importar 'common'
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from common import config
from common.ipc_manager import setup_server_manager, SharedMemoryManager

# Configuração do Logging
logger = logging.getLogger('Aeroporto')
logger.setLevel(logging.INFO)
fh = logging.FileHandler(os.path.join(os.path.dirname(__file__), 'aeroporto.log'))
fh.setFormatter(logging.Formatter('%(asctime)s - [%(levelname)s] - %(message)s'))
logger.addHandler(fh)
ch = logging.StreamHandler()
ch.setFormatter(logging.Formatter('%(asctime)s - [%(levelname)s] - %(message)s'))
logger.addHandler(ch)

def processa_logs(log_queue):
    """
    Thread dedicada a consumir a fila de logs enviada pelos clientes.
    """
    while True:
        try:
            msg = log_queue.get()
            if msg == "STOP":
                break
            logger.info(f"(Cliente) {msg}")
        except Exception:
            pass

def embarcar_passageiro(passageiro, sem_portoes, sem_agentes, d_estados):
    """
    Simula o processo de embarque para um passageiro.
    """
    p_id = passageiro['id']
    prioridade = passageiro['prioridade']
    chegada = passageiro['chegada']
    
    # Tempo varia pela prioridade: alta (1) é mais rápido
    tempo_embarque = 2 if prioridade == config.PRIORIDADES['ALTA'] else (3 if prioridade == config.PRIORIDADES['MEDIA'] else 4)
    
    espera_total = time.time() - chegada
    
    # Verificação estrita antes de começar (Race condition)
    if d_estados.get(p_id) == 'desistiu':
        logger.info(f"O Passageiro {p_id} desistiu imediatamente antes do embarque.")
        sem_portoes.release()
        sem_agentes.release()
        return

    logger.info(f"O Passageiro {p_id} (Prioridade {prioridade}) COMEÇOU o embarque. Esperou {espera_total:.1f}s.")
    
    # Simula embarque
    time.sleep(tempo_embarque)
    
    # Atualiza o estado (se não desistiu, na verdade já não desiste durante o embarque)
    if d_estados.get(p_id) != 'desistiu':
        d_estados[p_id] = 'embarcado'
    
    # Liberta recursos
    sem_portoes.release()
    sem_agentes.release()
    
    logger.info(f"O Passageiro {p_id} TERMINOU o embarque. Duração do embarque: {tempo_embarque}s.")

def main():
    logger.info("A iniciar o Servidor (Aeroporto)...")
    
    # Inicializa as estruturas na memória partilhada
    setup_server_manager(config.NUM_PORTOES, config.NUM_AGENTES)
    
    # Configura e inicia o Manager
    # '0.0.0.0' em vez de SERVER_IP para permitir que aceite de qualquer IP na LAN se testado externamente.
    # Mas usamos a config para ser consistente se lá estiver '0.0.0.0'
    server_ip = '0.0.0.0' if config.SERVER_IP == '127.0.0.1' else config.SERVER_IP
    manager = SharedMemoryManager(address=(server_ip, config.SERVER_PORT), authkey=config.AUTHKEY)
    server = manager.get_server()
    
    # Como não podemos usar `manager.start()` com `get_server()` ao mesmo tempo facilmente sem instanciar,
    # A maneira mais limpa é usar manager.start() que arranca um processo, ou server.serve_forever()
    # Para interagir com a lista no mesmo processo de forma assíncrona, vamos correr `server.serve_forever()` numa thread
    manager_thread = threading.Thread(target=server.serve_forever, daemon=True)
    manager_thread.start()
    
    logger.info(f"IPC Manager a correr em {server_ip}:{config.SERVER_PORT}...")
    
    # Precisamos de nos ligar ao nosso próprio manager para usar as proxies (recomendado)
    local_client = SharedMemoryManager(address=('127.0.0.1', config.SERVER_PORT), authkey=config.AUTHKEY)
    local_client.connect()
    
    fila = local_client.get_fila()
    lock_fila = local_client.get_lock()
    sem_portoes = local_client.get_portoes()
    sem_agentes = local_client.get_agentes()
    d_estados = local_client.get_estados()
    log_queue = local_client.get_logs()
    
    # Iniciar a thread que processa logs remotos
    t_log = threading.Thread(target=processa_logs, args=(log_queue,), daemon=True)
    t_log.start()
    
    logger.info(f"Aeroporto aberto: {config.NUM_PORTOES} Portões e {config.NUM_AGENTES} Agentes.")
    
    try:
        while True:
            # Verifica se há alguém na fila antes de bloquear nos semáforos (evita Inversão de Prioridade)
            lock_fila.acquire()
            q_len = len(fila)
            lock_fila.release()
            
            if q_len > 0:
                # Tenta adquirir recursos ANTES de retirar a pessoa da fila
                sem_portoes.acquire()
                sem_agentes.acquire()
                
                passageiro = None
                lock_fila.acquire()
                try:
                    if len(fila) > 0:
                        passageiro = fila.pop(0)
                finally:
                    lock_fila.release()
                    
                if passageiro:
                    p_id = passageiro['id']
                    
                    # Se o passageiro já desistiu antes de chegar a sua vez
                    if d_estados.get(p_id) == 'desistiu':
                        sem_portoes.release()
                        sem_agentes.release()
                        continue
                    
                    # Inicia o agente de embarque numa nova thread
                    t = threading.Thread(target=embarcar_passageiro, args=(passageiro, sem_portoes, sem_agentes, d_estados))
                    t.start()
                else:
                    # Fila ficou vazia entretanto, libertar recursos
                    sem_portoes.release()
                    sem_agentes.release()
            else:
                # Espera curta para não consumir muito CPU
                time.sleep(0.5)
                
    except KeyboardInterrupt:
        logger.info("A encerrar o aeroporto.")
    finally:
        logger.info("A desligar o Manager IPC e a libertar todos recursos.")
        # Como o servidor foi criado com get_server() e serve_forever() numa thread daemon,
        # ele morre automaticamente com o processo. Não existe método shutdown() neste modo.
        sys.exit(0)

if __name__ == "__main__":
    main()
