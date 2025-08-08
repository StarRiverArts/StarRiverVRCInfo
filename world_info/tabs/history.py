from __future__ import annotations

import tkinter as tk
from tkinter import ttk


class HistoryTab:
    def __init__(self, nb: ttk.Notebook, app) -> None:
        self.app = app
        self.frame = ttk.Frame(nb)
        nb.add(self.frame, text="歷史記錄")
        self._build()

    def _build(self) -> None:
        f = self.frame
        self.app.var_hist_world = tk.StringVar()
        self.app.box_hist_world = ttk.Combobox(
            f, textvariable=self.app.var_hist_world, values=list(self.app.history.keys())
        )
        self.app.box_hist_world.pack(fill=tk.X, pady=2)
        self.app.box_hist_world.bind("<<ComboboxSelected>>", self.app._draw_history)
        self.app.canvas = tk.Canvas(f, bg="white")
        self.app.canvas.pack(fill=tk.BOTH, expand=True)
        self.app._update_history_options()
