from __future__ import annotations

import tkinter as tk
from tkinter import ttk

from ..constants import METRIC_COLS


class ListTab:
    def __init__(self, nb: ttk.Notebook, app) -> None:
        self.app = app
        self.frame = ttk.Frame(nb)
        nb.add(self.frame, text="世界列表")
        self._build()

    def _build(self) -> None:
        columns = ["爬取日期"] + METRIC_COLS
        self.app.tree = ttk.Treeview(self.frame, show="headings")
        self.app.tree["columns"] = list(range(len(columns)))
        for idx, col in enumerate(columns):
            self.app.tree.heading(str(idx), text=col)
            self.app.tree.column(str(idx), width=80, anchor="center")
        vsb = ttk.Scrollbar(self.frame, orient="vertical", command=self.app.tree.yview)
        hsb = ttk.Scrollbar(self.frame, orient="horizontal", command=self.app.tree.xview)
        self.app.tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        self.app.tree.pack(side="left", fill=tk.BOTH, expand=True)
        vsb.pack(side="right", fill=tk.Y)
        hsb.pack(side="bottom", fill=tk.X)
