from __future__ import annotations

import tkinter as tk
from tkinter import ttk


class FilterTab:
    def __init__(self, nb: ttk.Notebook, app) -> None:
        self.app = app
        self.frame = ttk.Frame(nb)
        nb.add(self.frame, text="篩選")
        self._build()

    def _build(self) -> None:
        f = self.frame
        ttk.Label(f, text="Tag").grid(row=0, column=0, sticky="e")
        self.app.var_tag = tk.StringVar(value="all")
        self.app.box_tag = ttk.Combobox(f, textvariable=self.app.var_tag, values=["all"])
        self.app.box_tag.grid(row=0, column=1, padx=4, pady=2)

        ttk.Label(f, text="Sort").grid(row=1, column=0, sticky="e")
        self.app.var_sort = tk.StringVar(value="popular")
        ttk.Combobox(f, textvariable=self.app.var_sort, values=["latest", "popular"]).grid(row=1, column=1, padx=4, pady=2)

        ttk.Button(f, text="Apply", command=self.app._apply_filter).grid(row=2, column=0, columnspan=2, pady=4)
