# Arquitetura do Sistema de Embarque — SO2 (Trabalho 2)

## Índice
1. [Visão Geral](#1-visão-geral)
2. [Estrutura de Ficheiros](#2-estrutura-de-ficheiros)
3. [Configuração Partilhada — `common/config.py`](#3-configuração-partilhada--commonconfigpy)
4. [Gestor de Memória Partilhada — `common/ipc_manager.py`](#4-gestor-de-memória-partilhada--commonipc_managerpy)
5. [Servidor (Aeroporto) — `aeroportoServidor/servidor.py`](#5-servidor-aeroporto--aeroportoservidorservidorpy)
6. [Cliente (Passageiros) — `passageirosCliente/gerador_clientes.py`](#6-cliente-passageiros--passageirosclientegerador_clientespy)
7. [Interface Visual — `common/display.py`](#7-interface-visual--commondisplaypy)
8. [Fluxo Completo Passo-a-Passo](#8-fluxo-completo-passo-a-passo)
9. [Mecanismos de Sincronização](#9-mecanismos-de-sincronização)
10. [Diagrama de Sequência](#10-diagrama-de-sequência)
11. [Como Executar (2 PCs)](#11-como-executar-2-pcs)

---

## 1. Visão Geral

O sistema simula o embarque de passageiros num aeroporto. Existem dois programas independentes:

| Componente | Papel | Ficheiro |
|---|---|---|
| **Servidor (Aeroporto)** | Gere a fila de embarque, aloca portões e agentes, regista logs | `aeroportoServidor/servidor.py` |
| **Cliente (Passageiros)** | Gera passageiros aleatoriamente, insere-os na fila, aguarda embarque ou desiste | `passageirosCliente/gerador_clientes.py` |

### Porquê `SyncManager` e não `shm_open`?

Internamente, cada chamada a um método do proxy é serializada, enviada pela rede, executada no servidor, e o resultado é devolvido — tudo de forma transparente.

### Tecnologias Utilizadas
- **Multiprocessing (SyncManager)**: Para memória partilhada distribuída.
- **Threading**: Para concorrência e processos de embarque em paralelo.
- **Rich**: Para a interface de terminal (Dashboard Real-time e logs coloridos).
- **Python 3.8+**: Linguagem base.

---

## 2. Estrutura de Ficheiros

```
SO2/
├── common/                          # Código partilhado entre Servidor e Cliente
│   ├── __init__.py                  # Torna 'common' num pacote Python importável
│   ├── config.py                    # Todas as constantes de configuração
│   ├── ipc_manager.py               # Definição do Manager e objetos partilhados
│   └── display.py                   # Dashboard em tempo real (Rich)
├── aeroportoServidor/               # Código do Servidor
│   ├── __init__.py
│   ├── servidor.py                  # Processo principal do aeroporto
│   └── aeroporto.log               # Ficheiro de log gerado automaticamente
├── passageirosCliente/              # Código do Cliente
│   ├── __init__.py
│   └── gerador_clientes.py         # Gerador de passageiros
├── .gitignore                       # Ignora .DS_Store, __pycache__, *.log
├── README.md                        # Este ficheiro de documentação
├── requirements.txt                 # Dependências do projeto (Rich)
└── Trabalho_2_2526.pdf              # Enunciado do trabalho
```

---

## 3. Configuração Partilhada — `common/config.py`

Este ficheiro contém **todas** as constantes usadas por ambos os componentes. Ao alterar um valor aqui, o comportamento muda em todo o sistema.

| Variável | Valor | Função |
|---|---|---|
| `SERVER_IP` | `'127.0.0.1'` | IP do servidor. Mudar para o IP da LAN quando se testa em 2 PCs. |
| `SERVER_PORT` | `50000` | Porta TCP usada pelo Manager para comunicação. |
| `AUTHKEY` | `b'so2_aeroporto_secreto'` | Chave de autenticação. O cliente e o servidor precisam da mesma chave para se ligarem. Impede ligações não autorizadas. |
| `NUM_PORTOES` | `3` | Nº de portões de embarque simultâneos (valor inicial do semáforo de portões). |
| `NUM_AGENTES` | `4` | Nº de agentes de embarque simultâneos (valor inicial do semáforo de agentes). |
| `TEMPO_LIMITE_ESPERA` | `15` | Segundos máximos que um passageiro espera antes de desistir. |
| `PRIORIDADES` | `{'ALTA': 1, 'MEDIA': 2, 'BAIXA': 3}` | Mapeamento de prioridades. O valor numérico mais baixo = mais prioritário. |

**Nota sobre `SERVER_IP`**: No servidor, o código converte automaticamente `'127.0.0.1'` para `'0.0.0.0'` (aceitar ligações de qualquer interface de rede). No cliente, este valor é usado diretamente como endereço de destino.

---

## 4. Gestor de Memória Partilhada — `common/ipc_manager.py`

Este é o módulo central de IPC (Inter-Process Communication). Define **que objetos existem na memória partilhada** e como são expostos pela rede.

### 4.1. Classe `SharedMemoryManager`

```python
class SharedMemoryManager(multiprocessing.managers.SyncManager):
    pass
```

Herda de `SyncManager`. Este é o "contentor" que vai alojar e servir os objetos partilhados. Tanto o servidor como o cliente instanciam esta classe, mas com papéis diferentes:
- **Servidor**: Cria uma instância, regista os objetos com `callable=...`, e chama `get_server().serve_forever()`.
- **Cliente**: Cria uma instância, regista os nomes (sem `callable`), e chama `.connect()`.

### 4.2. Variáveis Globais do Módulo

```python
_fila_embarque = None   # Lista Python []
_lock_fila = None       # multiprocessing.Lock
_sem_portoes = None     # multiprocessing.Semaphore
_sem_agentes = None     # multiprocessing.Semaphore
_d_estados = None       # Dicionário Python {}
_log_queue = None       # queue.Queue
```

Estas variáveis **só existem com valores reais no processo do Servidor**. No cliente, nunca são inicializadas diretamente — o cliente acede-as remotamente através dos proxies.

Cada variável tem uma função `get_*()` associada (ex: `get_fila_embarque()`) que simplesmente retorna a variável global. Estas funções são passadas como `callable` ao `register()`.

### 4.3. Função `setup_server_manager(num_portoes, num_agentes)`

**Chamada apenas pelo Servidor.** Faz duas coisas:

**Passo 1 — Cria os objetos reais:**
| Objeto | Tipo | Função |
|---|---|---|
| `_fila_embarque` | `list` (`[]`) | Fila de passageiros ordenada por prioridade |
| `_lock_fila` | `multiprocessing.Lock` | Exclusão mútua para aceder à fila |
| `_sem_portoes` | `multiprocessing.Semaphore(3)` | Conta e limita portões disponíveis |
| `_sem_agentes` | `multiprocessing.Semaphore(4)` | Conta e limita agentes disponíveis |
| `_d_estados` | `dict` (`{}`) | Mapeia `id_passageiro → estado` (`'esperando'`, `'embarcado'`, `'desistiu'`) |
| `_log_queue` | `queue.Queue` | Fila FIFO thread-safe para mensagens de log dos clientes |

**Passo 2 — Regista os métodos no Manager:**
```python
SharedMemoryManager.register('get_fila', callable=get_fila_embarque, proxytype=ListProxy)
```
Isto diz ao Manager: "quando alguém (local ou remoto) chamar `manager.get_fila()`, executa a função `get_fila_embarque()` e devolve o resultado empacotado num `ListProxy`". O `ListProxy` permite operações como `append()`, `pop()`, `len()`, e `fila[:] = ...` remotamente.

### 4.4. Função `get_client_manager()`

**Chamada apenas pelo Cliente.** Regista os mesmos nomes mas **sem `callable`**, pois o cliente não vai criar os objetos — vai recebê-los do servidor pela rede.

```python
SharedMemoryManager.register('get_fila')  # sem callable
```

---

## 5. Servidor (Aeroporto) — `aeroportoServidor/servidor.py`

### 5.1. Arranque e Logging (linhas 1–24)

O servidor configura o módulo `logging` do Python com **dois handlers**:
- **Dashboard Real-time (Rich)**: O servidor utiliza um motor de visualização (em `display.py`) que divide o terminal em dois painéis:
    - **Status (Fixo no topo)**: Mostra passageiros na fila e ocupação dos portões/agentes com barras de progresso.
    - **Logs (Rolante em baixo)**: Exibe os últimos eventos em tempo real com cores (ex: Verde = Embarque, Vermelho = Desistência).
- **Registo em Ficheiro**: Mantém o ficheiro `aeroporto.log` com texto simples para auditoria posterior.

### 5.2. Thread `processa_logs(log_queue)` (linhas 25–35)

Esta é uma thread dedicada que corre num ciclo infinito (`while True`). A sua única função é consumir mensagens da `log_queue` partilhada:

1. Chama `log_queue.get()` — este método **bloqueia** até haver uma mensagem disponível (não consome CPU à espera).
2. Quando recebe uma string, escreve-a no logger com o prefixo `(Cliente)`.
3. Se receber a string `"STOP"`, termina o ciclo.

**Porquê uma thread separada?** Porque os clientes (remotos) colocam mensagens na queue a qualquer momento. Se o servidor tentasse ler a queue no ciclo principal, ficaria bloqueado e não processaria embarques.

### 5.3. Função `embarcar_passageiro(...)` (linhas 37–79)

Esta função é executada numa **thread separada** para cada passageiro que embarca. Recebe:
- `passageiro`: dicionário `{'id': int, 'prioridade': int, 'chegada': float}`
- `sem_portoes`: proxy do semáforo de portões
- `sem_agentes`: proxy do semáforo de agentes
- `d_estados`: proxy do dicionário de estados

**Fluxo interno:**
1. Calcula o tempo de embarque baseado na prioridade:
   - Alta (1): **2 segundos**
   - Média (2): **3 segundos**
   - Baixa (3): **4 segundos**
2. Regista timestamps de chegada e embarque formatados (`HH:MM:SS`).
3. Calcula o tempo total de espera: `time.now() - chegada`
4. **Verificação rigorosa de estado (Race Condition):** Verifica estritamente se `d_estados[p_id] == 'desistiu'`.
5. Regista no log: "COMEÇOU o embarque" com hora de chegada e embarque explícitas.
6. `time.sleep(tempo_embarque)` — Simula o tempo de embarque
7. Atualiza `d_estados[p_id] = 'embarcado'`
8. `sem_portoes.release()` — Devolve 1 ao contador do semáforo
9. `sem_agentes.release()` — Devolve 1 ao contador do semáforo
10. Regista no log: "TERMINOU o embarque" com a hora de conclusão.

**Importante**: O `acquire()` dos semáforos é feito no ciclo principal (antes de lançar esta thread). O `release()` é feito aqui dentro (depois do embarque terminar ou em caso de desistência tardia). Isto garante que os recursos ficam reservados durante todo o embarque.

### 5.4. Função `main()` (linhas 80–179) — O Coração do Servidor

#### Fase 1 — Inicialização (linhas 81–100)

```
setup_server_manager(3, 4)     → Cria os 6 objetos partilhados
SharedMemoryManager(...)       → Cria o Manager TCP no endereço configurado
manager.get_server()           → Obtém o objeto servidor TCP interno
Thread(server.serve_forever)   → Corre o servidor TCP numa thread daemon
```

**Porquê numa thread daemon?** O `serve_forever()` é um ciclo infinito que aceita ligações de clientes. Se corresse no thread principal, bloquearia tudo. Ao correr como daemon, termina automaticamente quando o processo principal morre (Ctrl+C).

#### Fase 2 — Auto-conexão local (linhas 101–111)

```python
local_client = SharedMemoryManager(address=('127.0.0.1', config.SERVER_PORT), ...)
local_client.connect()
fila = local_client.get_fila()
```

**Porquê ligar-se a si mesmo?** Porque o Manager expõe os objetos através de **proxies**. Para usar os mesmos proxies que os clientes remotos usam (garantindo consistência), o servidor liga-se a si mesmo como se fosse mais um cliente. Assim, `fila` é um `ListProxy`, não a lista Python direta.

#### Fase 3 — Ciclo Principal de Despacho (linhas 128–172)

Este é o ciclo infinito que gere o fluxo do aeroporto:

```text
REPETE indefinidamente:
│
├─ 1. lock_fila.acquire()                    ← Verifica se a fila tem pessoas
│     ├─ q_len = len(fila)                   ← Vê o tamanho da fila
│     └─ lock_fila.release()
│
├─ 2. Visualização de Estado (periódica)      ← Imprime status a cada ~5 seg
│
├─ 3. Se a fila não estiver vazia:
│     ├─ sem_portoes.acquire()               ← BLOQUEIA até haver portão livre
│     ├─ sem_agentes.acquire()               ← BLOQUEIA até haver agente livre
│     │
│     ├─ lock_fila.acquire()                 ← Fase de Ordenação e Filtração
│     ├─ │  1. lista_local = list(fila)
│     ├─ │  2. Filtra desistentes            ← Remove quem desistiu no entretanto
│     ├─ │  3. Sort (Prioridade, Chegada)    ← Garante que o melhor entra primeiro
│     ├─ │  4. passageiro = pop(0)           ← Retira o escolhido
│     ├─ │  5. fila[:] = lista_local         ← Devolve a fila limpa e ordenada
│     ├─ lock_fila.release()
│     │
│     └─ Thread(embarcar_passageiro)         ← Lança embarque em paralelo
│
└─ 4. Se fila vazia:
      └─ time.sleep(0.5)                     ← Evita busy-waiting (poupa CPU)
```

**Detalhe Crítico: Centralização da Inteligência**
O servidor agora assume a responsabilidade total pela fila. Em vez de confiar que o cliente se ordena, o servidor re-ordena a lista e limpa "passageiros fantasmas" (desistentes) no momento exato em que os recursos ficam disponíveis. Isto resolve o problema da **Inversão de Prioridade** e garante que a memória partilhada é gerida de forma autoritária pelo servidor, permitindo que qualquer passageiro VIP que chegue enquanto o servidor espera por um portão seja atendido imediatamente assim que um recurso for libertado.

### 5.5. Encerramento Gracioso (Graceful Shutdown)
A função `main()` está envolvida num bloco `try/finally`. Como o servidor IPC foi criado através de `manager.get_server()` e está a correr numa `Thread(daemon=True)`, ele não corre num processo-filho isolado (o que exigiria `manager.shutdown()`). Sendo uma thread daemon, assim que o processo principal recebe o `Ctrl+C` (KeyboardInterrupt) e executa o `sys.exit(0)` no bloco `finally`, o sistema operativo liberta imediatamente a thread e as ligações TCP locais, garantindo um fecho limpo e sem sockets órfãos.

---

## 6. Cliente (Passageiros) — `passageirosCliente/gerador_clientes.py`

### 6.1. Função `passageiro_process(...)` (linhas 20–74)

Cada passageiro é uma **thread** que executa esta função. Recebe os proxies da fila, lock, estados e logs.

#### Fase 1 — Entrada na Fila (linhas 25–46)

```python
chegada = time.time()                          # Regista timestamp UNIX da chegada
passageiro_info = {'id': p_id, 'prioridade': prioridade, 'chegada': chegada}

lock_fila.acquire()                            # Pede acesso exclusivo à fila
fila.append(passageiro_info)                   # Adiciona-se ao fim da fila
lock_fila.release()                            # Liberta o lock
```

**Porquê apenas append()?** Agora que o servidor centraliza a ordenação, o cliente já não precisa de copiar a lista nem de a ordenar. Isto reduz o tempo de retenção do `Lock` e torna a comunicação muito mais eficiente. O servidor encarregar-se-á de colocar o passageiro na posição correta antes de o chamar para embarcar.

**Critério de ordenação:** `(prioridade, chegada)` — Ordena primeiro por prioridade (1 antes de 3), e em caso de empate, por ordem de chegada (FIFO). Assim, um passageiro de Primeira Classe que chegou às 12:05 fica à frente de um de Económica que chegou às 12:01.

Depois, marca `d_estados[p_id] = 'esperando'` e envia uma mensagem de log para a queue.

#### Fase 2 — Espera (Polling) (linhas 48–74)

O passageiro entra num ciclo de polling:

```
REPETE a cada 0.5 segundos:
│
├─ Calcula tempo de espera = agora - chegada
│
├─ Se espera > 15s (TEMPO_LIMITE_ESPERA):
│     ├─ d_estados[p_id] = 'desistiu'
│     ├─ Envia log de desistência
│     └─ TERMINA a thread
│
**Nota sobre remoção**: O cliente já não se remove fisicamente da lista `fila`. Ele apenas marca o seu estado como `'desistiu'` no dicionário partilhado. O servidor, ao processar a fila, ignora e remove automaticamente qualquer passageiro com este estado. Isto evita condições de corrida complexas na manipulação da lista.
├─ Se d_estados[p_id] == 'embarcado':
│     ├─ (O servidor mudou o estado durante o embarque)
│     └─ TERMINA a thread
│
└─ time.sleep(0.5)    ← Espera antes de voltar a verificar
```

**Porquê polling e não eventos?** Porque o `SyncManager` não suporta `Event` ou `Condition` de forma fiável através da rede. O polling a cada 0.5s é simples e eficiente o suficiente para esta simulação.

### 6.2. Função `gerar_clientes()` (linhas 75–131)

#### Ligação ao Servidor (linhas 76–98)

```python
get_client_manager()     # Regista os nomes dos métodos (sem callable)
manager = SharedMemoryManager(address=(SERVER_IP, SERVER_PORT), authkey=AUTHKEY)
manager.connect()        # Estabelece ligação TCP ao servidor
```

Se a ligação falhar (servidor não está a correr, IP errado, porta bloqueada), o `except` apanha o erro e mostra uma mensagem amigável.

#### Ciclo de Geração (linhas 100–127)

```
REPETE indefinidamente:
│
├─ Sorteia prioridade com distribuição:
│     ├─ 20% → ALTA (Primeira Classe)
│     ├─ 30% → MEDIA (Executiva)
│     └─ 50% → BAIXA (Económica)
│
├─ Lança Thread(passageiro_process) com essa prioridade
│
├─ Com 10% de probabilidade → SURTO:
│     └─ Lança 3 a 6 passageiros extra (todos BAIXA) de uma só vez
│
└─ time.sleep(1.0 a 4.0 segundos aleatórios)
```

**Surtos de alta procura:** Simulam os "cenários de alta demanda" pedidos no enunciado. Quando ocorre um surto, vários passageiros económicos chegam ao mesmo tempo, sobrecarregando a fila e podendo causar desistências se os recursos estiverem todos ocupados.

---

## 7. Interface Visual — `common/display.py`

Este módulo é responsável por desenhar a interface gráfica de terminal (Dashboard) do Servidor, isolando a lógica de desenho da lógica de negócios da aplicação.

### 7.1. Classe `AirportDisplay`
A classe gere um ecrã dividido em dois painéis dinâmicos usando a biblioteca `Rich.Live` e `Rich.Layout`:
- **Painel de Estado (Status)**: Localizado no topo, mostra o número atual de passageiros na fila e o estado de ocupação dos Portões e Agentes. Utiliza caracteres Unicode (`█` e `░`) e cores (Verde, Amarelo, Vermelho) para criar barras de progresso visuais simples de monitorizar.
- **Painel de Eventos (Logs)**: Localizado na parte inferior, exibe de forma rolante as últimas 30 mensagens de eventos (entradas na fila, inícios de embarque, conclusões ou desistências), coloridas dinamicamente de acordo com palavras-chave específicas (ex: Vermelho para desistências, Verde para embarques concluídos, Magenta para clientes).

### 7.2. Thread-Safety e Integração com Logging
Para funcionar de forma concorrente sem corromper o terminal:
1. **Controlo de Acesso Mutex (`threading.Lock`)**: Como o estado dos recursos e os logs são manipulados por múltiplas threads concorrentemente (thread principal do servidor, thread de logs remotos, e threads de embarque individual), as variáveis internas do display são protegidas por trincos locais (`_recursos_lock` e `_logs_lock`).
2. **`DisplayHandler` Personalizado**: Criámos um Handler de Logging personalizado que herda de `logging.Handler`. Em vez de imprimir para o `sys.stdout` padrão (o que iria danificar a formatação do dashboard), ele formata as mensagens e alimenta o buffer interno do `AirportDisplay.add_log()`, que por sua vez solicita o redesenho assíncrono do terminal a um máximo de 4 vezes por segundo (`refresh_per_second=4`), garantindo fluidez e reduzindo o consumo de CPU.

---

## 8. Fluxo Completo Passo-a-Passo

Exemplo concreto de um passageiro de Primeira Classe (Prioridade ALTA):

| Passo | Onde | O Que Acontece |
|---|---|---|
| 1 | Cliente | `gerar_clientes()` sorteia `prob=0.12` → ALTA. Cria `Thread(passageiro_process, id=5, prio=1)` |
| 2 | Cliente (Thread 5) | `chegada = time.time()` → regista `1714502400.0` |
| 3 | Cliente (Thread 5) | `lock_fila.acquire()` → espera se outra thread tem o lock |
| 4 | Cliente (Thread 5) | Adiciona o passageiro ao fim da fila (`fila.append(passageiro_info)`) |
| 5 | Cliente (Thread 5) | `lock_fila.release()` |
| 6 | Cliente (Thread 5) | `d_estados[5] = 'esperando'`, envia log → entra no ciclo de polling |
| 7 | Servidor (Main) | Verifica se a fila tem pessoas. Adquire recursos `sem_portoes.acquire()` e `sem_agentes.acquire()` |
| 8 | Servidor (Main) | `lock_fila.acquire()` -> filtra desistentes, ordena a fila por prioridade/chegada, faz `pop(0)` do passageiro 5, reescreve a fila e liberta o lock |
| 9 | Servidor (Main) | Cria `Thread(embarcar_passageiro, passageiro_5)` |
| 10 | Servidor (Thread Emb.) | Verifica se o passageiro já desistiu (no dict de estados). Se não, altera estado para `'embarcando'` |
| 11 | Servidor (Thread Emb.) | Calcula `tempo_embarque=2` (ALTA), regista "COMEÇOU" no log |
| 12 | Servidor (Thread Emb.) | `time.sleep(2)` — simula o embarque |
| 13 | Servidor (Thread Emb.) | `d_estados[5] = 'embarcado'` |
| 14 | Servidor (Thread Emb.) | `sem_portoes.release()` → liberta portão, `sem_agentes.release()` → liberta agente |
| 15 | Cliente (Thread 5) | No próximo poll (0.5s), lê `d_estados[5] == 'embarcado'` → imprime sucesso e termina |

---

## 9. Mecanismos de Sincronização

### 8.1. Lock da Fila (`multiprocessing.Lock`)

**Tipo:** Mutex (exclusão mútua binária).
**Protege:** A lista `_fila_embarque`.
**Usado por:** Servidor (para fazer `pop(0)`) e Clientes (para fazer `append` + `sort`, ou para se remover em caso de desistência).
**Garante:** Que dois processos/threads nunca modificam a fila ao mesmo tempo. Sem isto, dois passageiros a inserirem-se simultaneamente poderiam corromper a lista.

### 8.2. Semáforo de Portões (`multiprocessing.Semaphore(3)`)

**Tipo:** Semáforo de contagem.
**Valor inicial:** 3 (existem 3 portões).
**`acquire()`:** Decrementa o contador. Se já for 0, **bloqueia** até alguém fazer `release()`.
**`release()`:** Incrementa o contador.
**Efeito prático:** No máximo 3 embarques podem ocorrer em simultâneo (um por portão).

### 8.3. Semáforo de Agentes (`multiprocessing.Semaphore(4)`)

Idêntico ao de portões mas com valor 4. Na prática, como `NUM_AGENTES(4) > NUM_PORTOES(3)`, o bottleneck são os portões — haverá sempre pelo menos 1 agente livre. Alterar estes valores na config permite simular cenários diferentes.

### 8.4. `Queue` de Logs (`queue.Queue`)

**Thread-safe por defeito.** O `.put()` e `.get()` são atómicos. Múltiplos clientes podem enviar logs em simultâneo sem corromper a fila.

---

## 10. Diagrama de Sequência

```
PC 1 (Servidor)                                    PC 2 (Cliente)
─────────────────                                   ─────────────────
servidor.py                                         gerador_clientes.py
    │                                                    │
    ├─ setup_server_manager()                            │
    ├─ serve_forever() [Thread]                          │
    ├─ processa_logs() [Thread]                          │
    │                                                    │
    │◄───────────── TCP connect ────────────────────────►│
    │                                                    │
    │              [Ciclo Principal]                      │
    │                                                    ├─ Cria Passageiro Thread
    │                                                    │     │
    │◄─── lock.acquire() ───────────────────────────────│─────┤
    │◄─── fila.append(passageiro) ──────────────────────│─────┤
    │◄─── lock.release() ───────────────────────────────│─────┤
    │◄─── d_estados[id]='esperando' ────────────────────│─────┤
    │◄─── log_queue.put("entrou") ──────────────────────│─────┤
    │                                                    │     │
    │                                                    ├─ [ALT 1: Caso Feliz (Embarque)]
    │              [Ciclo Principal]                      │     ├─ poll: d_estados[id]? ('esperando')
    │     ├─ len(fila) > 0?                              │     │
    │     ├─ sem_portoes.acquire()                       │     │
    │     ├─ sem_agentes.acquire()                       │     │
    │     ├─ lock.acquire()                              │     │
    │     ├─ Filtra desistências e ordena a fila         │     │
    │     ├─ passageiro = fila.pop(0)                    │     │
    │     ├─ lock.release()                              │     │
    │     │                                              │     │
    │     ├─ Thread(embarcar)                            │     │
    │     │     ├─ d_estados[id]='embarcando' ───────────│────►│ (ignora timeout)
    │     │     ├─ sleep(tempo)                          │     │
    │     │     ├─ d_estados[id]='embarcado'─────────────│────►│
    │     │     ├─ sem_portoes.release()                 │     └─ "sucesso!" → Thread morre
    │     │     └─ sem_agentes.release()                 │
    │                                                    │
    │                                                    ├─ [ALT 2: Desistência por Timeout]
    │                                                    │     ├─ poll: tempo_espera > 15s?
    │                                                    │     ├─ d_estados[id]='desistiu' (IPC)
    │                                                    │     ├─ log_queue.put("desistiu") (IPC)
    │                                                    │     └─ Thread morre
    │     │                                              │
    │     ├─ [Servidor em paralelo (no ciclo):]          │
    │     ├─ Ao reordenar e limpar a fila, o Servidor    │
    │     ├─ descobre o estado 'desistiu' do passageiro, │
    │     ├─ remove-o e limpa a fila de forma segura.    │
```

---

## 11. Como Executar (2 PCs)

### Pré-requisitos
- Python 3.8+ instalado em ambos os PCs
- Ambos os PCs na **mesma rede WiFi/LAN**
- Nenhuma firewall a bloquear a porta 50000

### Passo 1 — Instalar Dependências
Ambos os computadores precisam da biblioteca `Rich` para a interface:
```bash
python3 -m pip install -r requirements.txt
```
*(No macOS, se der erro de "externally-managed-environment", use a flag `--break-system-packages`)*

### Passo 2 — Descobrir o IP do Servidor
No ficheiro `common/config.py`, no PC do **Cliente**, alterar:
```python
SERVER_IP = '192.168.1.100'  # ← IP real do PC servidor
```

### Passo 3 — Executar
1. **PC Servidor**: `python3 aeroportoServidor/servidor.py`
2. **PC Cliente**: `python3 passageirosCliente/gerador_clientes.py`

O servidor mostra os embarques. O cliente mostra as chegadas e desistências. Ambos partilham o estado em tempo real.

### Para testar no mesmo PC
Manter `SERVER_IP = '127.0.0.1'` e abrir **dois terminais** separados.
