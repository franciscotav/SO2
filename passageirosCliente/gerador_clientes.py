import sys
import os
import time
import threading
import random

# Adicionar a diretoria principal ao path para importar 'common'
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from common import config
from common.ipc_manager import get_client_manager, SharedMemoryManager

def passageiro_process(p_id, prioridade, fila, lock_fila, d_estados, log_queue):
    """
    Simula o ciclo de vida de um passageiro (Thread).
    """
    chegada = time.time()
    passageiro_info = {
        'id': p_id,
        'prioridade': prioridade,
        'chegada': chegada
    }
    
    # Adicionar o passageiro à fila partilhada de forma segura (Lock)
    lock_fila.acquire()
    try:
        # A manipulação de listas no Manager exige reatribuição ou métodos próprios
        # Vamos copiar, modificar, e reatribuir ou usar os métodos
        # O list proxy suporta append, mas para o sort() funcionar bem com proxy,
        # muitas vezes é melhor converter localmente e reatribuir
        lista_local = list(fila)
        lista_local.append(passageiro_info)
        # Ordem: 1º Prioridade (ascendente, 1=alta), 2º Tempo Chegada (ascendente, FIFO)
        lista_local.sort(key=lambda x: (x['prioridade'], x['chegada']))
        
        # Atualiza a fila remota
        fila[:] = lista_local
    finally:
        lock_fila.release()
    
    # Atualiza o estado no dicionário partilhado
    d_estados[p_id] = 'esperando'
    str_prio = [k for k, v in config.PRIORIDADES.items() if v == prioridade][0]
    log_queue.put(f"Passageiro {p_id} ({str_prio}) entrou na fila.")
    print(f"[Cliente] Passageiro {p_id} ({str_prio}) entrou na fila.")
    
    # Aguardar até ser atendido
    while True:
        espera = time.time() - chegada
        
        # Verifica se passou o limite de tempo de espera
        if espera > config.TEMPO_LIMITE_ESPERA:
            # Desiste
            lock_fila.acquire()
            try:
                lista_local = list(fila)
                lista_local = [p for p in lista_local if p['id'] != p_id]
                fila[:] = lista_local
            finally:
                lock_fila.release()
                
            d_estados[p_id] = 'desistiu'
            log_queue.put(f"Passageiro {p_id} DESISTIU após {espera:.1f}s de espera na fila.")
            print(f"[Cliente] Passageiro {p_id} DESISTIU.")
            break
            
        # Verifica se já terminou o embarque
        if d_estados.get(p_id) == 'embarcado':
            print(f"[Cliente] Passageiro {p_id} concluiu o embarque com sucesso.")
            break
            
        time.sleep(0.5)

def gerar_clientes():
    print(f"A ligar ao Aeroporto em {config.SERVER_IP}:{config.SERVER_PORT}...")
    get_client_manager()
    manager = SharedMemoryManager(address=(config.SERVER_IP, config.SERVER_PORT), authkey=config.AUTHKEY)
    
    try:
        manager.connect()
    except Exception as e:
        print("Erro ao ligar ao servidor. Certifique-se que o Servidor está a correr e o IP está correto.")
        print("Detalhes:", e)
        return
        
    print("Ligado ao Aeroporto com sucesso!")
    
    fila = manager.get_fila()
    lock_fila = manager.get_lock()
    d_estados = manager.get_estados()
    log_queue = manager.get_logs()
    
    p_id = 1
    try:
        while True:
            # Sorteia prioridade
            prob = random.random()
            if prob < 0.2: # 20% Primeira Classe
                prioridade = config.PRIORIDADES['ALTA']
            elif prob < 0.5: # 30% Executiva
                prioridade = config.PRIORIDADES['MEDIA']
            else: # 50% Económica
                prioridade = config.PRIORIDADES['BAIXA']
                
            # Lança thread para o passageiro
            t = threading.Thread(target=passageiro_process, args=(p_id, prioridade, fila, lock_fila, d_estados, log_queue))
            t.start()
            
            p_id += 1
            
            # Simula alta procura: às vezes gera múltiplos rápido
            if random.random() < 0.1: # 10% de chance de surto
                print("--- SURTO DE PASSAGEIROS ---")
                for _ in range(random.randint(3, 6)):
                    t_surto = threading.Thread(target=passageiro_process, args=(p_id, config.PRIORIDADES['BAIXA'], fila, lock_fila, d_estados, log_queue))
                    t_surto.start()
                    p_id += 1
            
            # Espera tempo aleatório antes do próximo (1 a 4 segundos)
            time.sleep(random.uniform(1.0, 4.0))
            
    except KeyboardInterrupt:
        print("\nGerador de passageiros encerrado.")
        sys.exit(0)

if __name__ == "__main__":
    gerar_clientes()
