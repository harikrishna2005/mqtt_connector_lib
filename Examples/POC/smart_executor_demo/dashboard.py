import asyncio
from rich.live import Live
from rich.table import Table
from rich.panel import Panel
from rich.layout import Layout


class Dashboard:
    """
    A live Rich terminal dashboard displaying:
    - CPU EWMA
    - Memory usage %
    - Queue size
    - Queue usage %
    - Worker count
    """

    def __init__(self):
        self.cpu = 0.0
        self.mem = 0.0
        self.qsize = 0
        self.qusage = 0.0
        self.workers = 0

    def update_metrics(self, cpu, mem, qsize, qusage, workers):
        self.cpu = cpu
        self.mem = mem
        self.qsize = qsize
        self.qusage = qusage
        self.workers = workers

    def _build_layout(self):
        layout = Layout()

        top = Table(title="System Metrics")
        top.add_column("Metric")
        top.add_column("Value")

        top.add_row("CPU (EWMA %)", f"{self.cpu:.2f}%")
        top.add_row("Memory %", f"{self.mem:.2f}%")
        top.add_row("Queue Size", str(self.qsize))
        top.add_row("Queue Usage %", f"{self.qusage * 100:.2f}%")
        top.add_row("Workers", str(self.workers))

        layout.split(
            Layout(Panel(top, title="Smart Executor Dashboard"), name="main")
        )

        return layout

    async def run(self):
        with Live(self._build_layout(), refresh_per_second=4) as live:
            while True:
                live.update(self._build_layout())
                await asyncio.sleep(0.25)
