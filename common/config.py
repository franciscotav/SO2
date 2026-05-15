# Configurações partilhadas entre Cliente e Servidor

# Endereço e Porta para a memória partilhada na rede (Manager)
# Colocar o IP real da LAN do Servidor quando testar em 2 PCs. (Ex: '192.168.1.100')
# '0.0.0.0' no servidor permite receber conexões de qualquer IP.
SERVER_IP = '127.0.0.1' 
SERVER_PORT = 50000
AUTHKEY = b'so2_aeroporto_secreto'

# Parâmetros do Aeroporto
NUM_PORTOES = 3
NUM_AGENTES = 4

# Tempos e Limites
TEMPO_LIMITE_ESPERA = 15  # Segundos até o passageiro desistir
TEMPO_ALTA_PROCURA = 15   # Probabilidade maior de gerar muitos clientes

# Prioridades
# 1 - Alta (Primeira Classe / Urgente)
# 2 - Média (Classe Executiva)
# 3 - Baixa (Económica)
PRIORIDADES = {
    'ALTA': 1,
    'MEDIA': 2,
    'BAIXA': 3
}
