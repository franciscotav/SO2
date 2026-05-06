import multiprocessing.managers
import queue
import multiprocessing

class SharedMemoryManager(multiprocessing.managers.SyncManager):
    pass

# Estas variáveis vão guardar as instâncias reais no processo Servidor
_fila_embarque = None
_lock_fila = None
_sem_portoes = None
_sem_agentes = None
_d_estados = None
_log_queue = None

def get_fila_embarque():
    global _fila_embarque
    return _fila_embarque

def get_lock_fila():
    global _lock_fila
    return _lock_fila

def get_sem_portoes():
    global _sem_portoes
    return _sem_portoes

def get_sem_agentes():
    global _sem_agentes
    return _sem_agentes

def get_d_estados():
    global _d_estados
    return _d_estados

def get_log_queue():
    global _log_queue
    return _log_queue

def setup_server_manager(num_portoes, num_agentes):
    """
    Inicializa os objetos que ficarão em memória partilhada.
    Só deve ser chamado pelo Servidor!
    """
    global _fila_embarque, _lock_fila, _sem_portoes, _sem_agentes, _d_estados, _log_queue
    
    _fila_embarque = []  # Lista normal, será gerida através de Manager
    _lock_fila = multiprocessing.Lock()
    _sem_portoes = multiprocessing.Semaphore(num_portoes)
    _sem_agentes = multiprocessing.Semaphore(num_agentes)
    _d_estados = {}
    _log_queue = queue.Queue()

    # Registar funções que devolvem os objetos
    SharedMemoryManager.register('get_fila', callable=get_fila_embarque, proxytype=multiprocessing.managers.ListProxy)
    SharedMemoryManager.register('get_lock', callable=get_lock_fila)
    SharedMemoryManager.register('get_portoes', callable=get_sem_portoes)
    SharedMemoryManager.register('get_agentes', callable=get_sem_agentes)
    SharedMemoryManager.register('get_estados', callable=get_d_estados, proxytype=multiprocessing.managers.DictProxy)
    SharedMemoryManager.register('get_logs', callable=get_log_queue)

def get_client_manager():
    """
    Regista os métodos no cliente (sem inicializar os objetos reais, pois vêm da rede).
    """
    SharedMemoryManager.register('get_fila')
    SharedMemoryManager.register('get_lock')
    SharedMemoryManager.register('get_portoes')
    SharedMemoryManager.register('get_agentes')
    SharedMemoryManager.register('get_estados')
    SharedMemoryManager.register('get_logs')
