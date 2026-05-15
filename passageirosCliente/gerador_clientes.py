import sys
import os
import time
import threading
import random

# ── Rich: Formatação visual do terminal ──
from rich.console import Console
from rich.panel import Panel

# Adicionar a diretoria principal ao path para importar 'common'
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from common import config
from common.ipc_manager import get_client_manager, SharedMemoryManager

# ── Consola Rich para output formatado ──
console = Console()

def passageiro_process(p_id, prioridade, fila, lock_fila, d_estados, log_queue):
    """
    Simula o ciclo de vida de um passageiro (Thread).
    """
    try:
        chegada = time.time()
        passageiro_info = {
            'id': p_id,
            'prioridade': prioridade,
            'chegada': chegada
        }
        
        # Adicionar o passageiro à fila partilhada de forma segura (Lock)
        lock_fila.acquire()
        try:
            # O cliente apenas se adiciona ao fim da fila
            # O Servidor encarregar-se-á de ordenar por prioridade antes de chamar
            fila.append(passageiro_info)
        finally:
            lock_fila.release()
        
        # Atualiza o estado no dicionário partilhado
        d_estados[p_id] = 'esperando'
        str_prio = [k for k, v in config.PRIORIDADES.items() if v == prioridade][0]
        log_queue.put(f"Passageiro {p_id} ({str_prio}) entrou na fila.")
        console.print(f"  [cyan]✈[/cyan]  Passageiro [bold]{p_id}[/bold] ([magenta]{str_prio}[/magenta]) entrou na fila.")
        
        # Aguardar até ser atendido
        while True:
            estado = d_estados.get(p_id)
            
            # Se já terminou o embarque
            if estado == 'embarcado':
                console.print(f"  [bold green]✔[/bold green]  Passageiro [bold]{p_id}[/bold] concluiu o embarque com sucesso.")
                break
                
            # Se já está a embarcar (no portão), ignora o timeout de desistência
            if estado == 'embarcando':
                time.sleep(0.5)
                continue
                
            espera = time.time() - chegada
            
            # Verifica se passou o limite de tempo de espera (apenas se ainda estiver 'esperando')
            if espera > config.TEMPO_LIMITE_ESPERA:
                # Desiste — apenas marca o estado, o Servidor tratará de o remover da fila
                d_estados[p_id] = 'desistiu'
                log_queue.put(f"Passageiro {p_id} DESISTIU após {espera:.1f}s de espera na fila.")
                console.print(f"  [bold red]✘[/bold red]  Passageiro [bold]{p_id}[/bold] [red]DESISTIU[/red] após {espera:.1f}s.")
                break
                
            time.sleep(0.5)
    except (EOFError, ConnectionRefusedError):
        console.print(f"  [bold yellow]⚠[/bold yellow]  Ligação ao Aeroporto perdida (Passageiro {p_id}).")

def gerar_clientes():
    # ── Banner de arranque com Rich ──
    console.print(Panel.fit(
        "[bold cyan]✈  Gerador de Passageiros[/bold cyan]\n"
        f"[dim]Servidor: {config.SERVER_IP}:{config.SERVER_PORT}[/dim]",
        border_style="cyan"
    ))
    get_client_manager()
    manager = SharedMemoryManager(address=(config.SERVER_IP, config.SERVER_PORT), authkey=config.AUTHKEY)
    
    try:
        manager.connect()
    except Exception as e:
        console.print(f"[bold red]✘ Erro ao ligar ao servidor.[/bold red] Certifique-se que o Servidor está a correr e o IP está correto.")
        console.print(f"[dim]Detalhes: {e}[/dim]")
        return
        
    console.print("[bold green]✔ Ligado ao Aeroporto com sucesso![/bold green]")
    
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
            if random.random() < (config.TEMPO_ALTA_PROCURA / 100): # chance de surto
                console.print(f"\n  [bold yellow on red] ⚠  SURTO DE PASSAGEIROS ({config.TEMPO_ALTA_PROCURA}%) [/bold yellow on red]\n")
                for _ in range(random.randint(3, 6)):
                    t_surto = threading.Thread(target=passageiro_process, args=(p_id, config.PRIORIDADES['BAIXA'], fila, lock_fila, d_estados, log_queue))
                    t_surto.start()
                    p_id += 1
            
            # Espera tempo aleatório antes do próximo (1 a 4 segundos)
            time.sleep(random.uniform(1.0, 4.0))
            
    except KeyboardInterrupt:
        console.print("\n[bold cyan]Gerador de passageiros encerrado.[/bold cyan]")
        sys.exit(0)

if __name__ == "__main__":
    gerar_clientes()
