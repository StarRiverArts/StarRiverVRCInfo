from __future__ import annotations

import tkinter as tk
from tkinter import ttk


class ListTab:
    def __init__(self, nb: ttk.Notebook, app) -> None:
        self.app = app
        self.frame = ttk.Frame(nb)
        nb.add(self.frame, text="世界列表")
        self._build()

    def _build(self) -> None:
        columns = ("name", "visits", "id")
        self.app.tree = ttk.Treeview(self.frame, columns=columns, show="headings")
        self.app.tree.heading("name", text="Name")
        self.app.tree.heading("visits", text="Visits")
        self.app.tree.heading("id", text="World ID")
        self.app.tree.column("name", width=250)
        self.app.tree.column("visits", width=80, anchor="e")
        self.app.tree.column("id", width=200)
        vsb = ttk.Scrollbar(self.frame, orient="vertical", command=self.app.tree.yview)
        self.app.tree.configure(yscrollcommand=vsb.set)
        self.app.tree.pack(side="left", fill=tk.BOTH, expand=True)
        vsb.pack(side="right", fill=tk.Y)
