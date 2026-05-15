"""
common/display.py
─────────────────────────────────────────────────────────────────
Dashboard em tempo real para o Servidor do Aeroporto.
Usa Rich Live + Layout para mostrar:
  - Painel de Estado fixo no topo (fila, portões, agentes)
  - Log rolante em baixo (últimas mensagens)

Este módulo é completamente isolado da lógica de negócio.
Basta criar um AirportDisplay, chamar .start() e .add_log(msg).
─────────────────────────────────────────────────────────────────
"""

import threading
import logging
from collections import deque
from datetime import datetime

from rich.live import Live
from rich.layout import Layout
from rich.panel import Panel
from rich.table import Table
from rich.text import Text
from rich.console import Console
from rich.columns import Columns


# ── Paleta de cores por tipo de evento ──────────────────────────
_CORES = {
    'entrou':     'cyan',
    'COMEÇOU':    'green',
    'TERMINOU':   'bold green',
    'DESISTIU':   'bold red',
    'desistiu':   'red',
    'SURTO':      'bold yellow',
    'STATUS':     'dim',
    'Cliente':    'magenta',
    'default':    'white',
}

def _colorir(msg: str) -> str:
    """Aplica cor à mensagem com base em palavras-chave."""
    for chave, cor in _CORES.items():
        if chave in msg:
            return f"[{cor}]{msg}[/{cor}]"
    return f"[{_CORES['default']}]{msg}[/{_CORES['default']}]"


class AirportDisplay:
    """
    Gere o dashboard Rich em tempo real.
    Thread-safe: pode ser chamado de múltiplas threads simultaneamente.
    """

    def __init__(self, fila, lock_fila, num_portoes, num_agentes):
        self._fila = fila
        self._lock_fila = lock_fila
        self._num_portoes = num_portoes
        self._num_agentes = num_agentes

        # Contadores de recursos em uso (atualizados pelo servidor)
        self._portoes_em_uso = 0
        self._agentes_em_uso = 0
        self._recursos_lock = threading.Lock()

        # Deque thread-safe para os logs (máximo 30 linhas)
        self._logs: deque = deque(maxlen=30)
        self._logs_lock = threading.Lock()

        self._console = Console()
        self._live: Live | None = None

    # ── API pública ────────────────────────────────────────────

    def add_log(self, msg: str):
        """Adiciona uma mensagem ao painel de logs."""
        ts = datetime.now().strftime('%H:%M:%S')
        linha = f"[dim]{ts}[/dim]  {_colorir(msg)}"
        with self._logs_lock:
            self._logs.append(linha)
        self._refresh()

    def recurso_adquirido(self):
        """Chamar quando um portão+agente são alocados."""
        with self._recursos_lock:
            self._portoes_em_uso += 1
            self._agentes_em_uso += 1
        self._refresh()

    def recurso_libertado(self):
        """Chamar quando um portão+agente são libertados."""
        with self._recursos_lock:
            self._portoes_em_uso = max(0, self._portoes_em_uso - 1)
            self._agentes_em_uso = max(0, self._agentes_em_uso - 1)
        self._refresh()

    def start(self):
        """Inicia o dashboard. Chamar uma vez antes do loop principal."""
        self._live = Live(
            self._build_layout(),
            console=self._console,
            refresh_per_second=4,
            screen=False,       # Não limpa o terminal inteiro
            transient=False,
        )
        self._live.start()

    def stop(self):
        """Para o dashboard. Chamar no finally do servidor."""
        if self._live:
            self._live.stop()

    # ── Construção do layout ───────────────────────────────────

    def _refresh(self):
        if self._live:
            self._live.update(self._build_layout())

    def _build_layout(self) -> Layout:
        layout = Layout()
        layout.split_column(
            Layout(name="status", size=10),
            Layout(name="logs"),
        )
        layout["status"].update(self._build_status_panel())
        layout["logs"].update(self._build_logs_panel())
        return layout

    def _build_status_panel(self) -> Panel:
        # Ler tamanho da fila de forma segura
        try:
            self._lock_fila.acquire()
            q_len = len(self._fila)
        finally:
            self._lock_fila.release()

        with self._recursos_lock:
            portoes_livres = self._num_portoes - self._portoes_em_uso
            agentes_livres = self._num_agentes - self._agentes_em_uso

        # Barra de ocupação dos portões
        barra_portoes = self._barra(self._portoes_em_uso, self._num_portoes)
        barra_agentes = self._barra(self._agentes_em_uso, self._num_agentes)

        tabela = Table(show_header=False, box=None, padding=(0, 2), expand=True)
        tabela.add_column("Ícone", width=4)
        tabela.add_column("Métrica", style="bold white", min_width=22)
        tabela.add_column("Valor", style="bold yellow", min_width=6)
        tabela.add_column("Barra", min_width=20)

        tabela.add_row(
            "🧍",
            "Passageiros na fila",
            f"[bold {'red' if q_len > 5 else 'yellow' if q_len > 0 else 'green'}]{q_len}[/]",
            ""
        )
        tabela.add_row(
            "🚪",
            f"Portões ({portoes_livres}/{self._num_portoes} livres)",
            "",
            barra_portoes,
        )
        tabela.add_row(
            "👤",
            f"Agentes ({agentes_livres}/{self._num_agentes} livres)",
            "",
            barra_agentes,
        )

        return Panel(
            tabela,
            title="[bold cyan]✈  Aeroporto — Estado Atual[/bold cyan]",
            border_style="cyan",
        )

    def _build_logs_panel(self) -> Panel:
        with self._logs_lock:
            linhas = list(self._logs)

        texto = Text.from_markup("\n".join(linhas) if linhas else "[dim]Sem eventos ainda...[/dim]")

        return Panel(
            texto,
            title="[bold green]📋  Logs Recentes[/bold green]",
            border_style="green",
        )

    @staticmethod
    def _barra(em_uso: int, total: int) -> str:
        """Gera uma barra de progresso simples com caracteres Unicode."""
        if total == 0:
            return ""
        preenchido = int((em_uso / total) * 10)
        cor = "red" if preenchido >= 9 else "yellow" if preenchido >= 6 else "green"
        barra = "█" * preenchido + "░" * (10 - preenchido)
        return f"[{cor}]{barra}[/{cor}] {em_uso}/{total}"


# ── Handler de logging que alimenta o AirportDisplay ────────────

class DisplayHandler(logging.Handler):
    """
    Logging handler customizado.
    Em vez de escrever no terminal diretamente, envia
    cada mensagem para o AirportDisplay.add_log().
    """

    def __init__(self, display: AirportDisplay):
        super().__init__()
        self._display = display

    def emit(self, record: logging.LogRecord):
        try:
            msg = self.format(record)
            self._display.add_log(msg)
        except Exception:
            self.handleError(record)
