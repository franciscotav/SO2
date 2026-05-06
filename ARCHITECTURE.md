# Arquitetura do Sistema de Embarque - SO2 (Trabalho 2)

## 1. Visão Geral
O sistema foi concebido para funcionar de forma distribuída (em 2 PCs diferentes), como exigido. Para permitir a **memória partilhada** e o uso de **semáforos** através da rede (já que a memória partilhada nativa do SO não funciona entre computadores diferentes), utilizámos o `multiprocessing.managers.SyncManager` nativo do Python. 

O `SyncManager` permite criar um Servidor que aloja os objetos partilhados na memória (Listas, Dicionários, Semáforos e Locks) e os expõe através de uma porta de rede. O Cliente liga-se a essa porta e manipula os objetos como se estivessem na sua própria memória local.

## 2. Componentes e Partilha de Memória
A comunicação assenta no ficheiro `common/ipc_manager.py`, que define os seguintes recursos partilhados:
- **Fila de Embarque (`list` partilhada)**: Guarda os passageiros que estão à espera.
- **Lock da Fila (`Lock`)**: Garante exclusão mútua quando os processos cliente adicionam passageiros ou desistem, e quando o servidor remove o passageiro de maior prioridade, evitando *Race Conditions*.
- **Semáforo de Portões (`Semaphore`)**: Limita o acesso simultâneo aos portões. Inicializado com o número de portões (ex: 3).
- **Semáforo de Agentes (`Semaphore`)**: Limita o número de embarques em simultâneo.
- **Dicionário de Estados (`dict` partilhado)**: Permite ao passageiro saber quando terminou de embarcar.
- **Fila de Logs (`Queue`)**: Usada para enviar mensagens de log centralizadas para o servidor.

## 3. Servidor (Aeroporto)
O `aeroportoServidor/servidor.py` tem as seguintes responsabilidades:
1. **Iniciar o IPC Manager**: Abre a porta na rede para receber os clientes.
2. **Gestor de Embarque**: Uma thread principal observa continuamente a Fila de Embarque. Quando há passageiros, tenta adquirir (via `acquire()`) um Portão e um Agente.
3. **Simulação de Embarque**: Cria uma thread (Agente) para o passageiro que processa o embarque (tempo varia com a prioridade). No fim, liberta os recursos (via `release()`) e atualiza o estado para "embarcado".
4. **Logger**: Uma thread dedicada a consumir a Fila de Logs e a escrever no ficheiro `aeroporto.log`.

## 4. Cliente (Passageiros)
O `passageirosCliente/gerador_clientes.py` atua da seguinte forma:
1. Liga-se ao IPC Manager do Servidor usando o IP e Porta definidos.
2. **Gerador**: Periodicamente (simulando também picos de tráfego), lança novas *Threads* para cada Passageiro.
3. **Ciclo de Vida do Passageiro**:
   - Calcula a sua prioridade e insere-se na Fila de Embarque, reordenando a lista (Prioridade Alta > Média > Baixa, desempate por tempo de chegada). Exige Lock.
   - Fica num ciclo de espera (*polling* eficiente) a verificar o Dicionário de Estados.
   - Se o tempo de espera exceder o limite (ex: 15s), o passageiro **desiste**, usa o Lock para se retirar da lista e termina.

## 5. Como Executar (Em 2 PCs)
1. **No PC do Servidor**:
   - Edite `common/config.py` e coloque `SERVER_IP = '0.0.0.0'` (ou o IP real da LAN).
   - Execute `python3 aeroportoServidor/servidor.py`.
2. **No PC do Cliente**:
   - Edite `common/config.py` e coloque `SERVER_IP = '<IP_DO_SERVIDOR>'`.
   - Execute `python3 passageirosCliente/gerador_clientes.py`.
