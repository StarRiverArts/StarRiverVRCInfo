from __future__ import annotations

import tkinter as tk
from tkinter import ttk


class EntryTab:
    def __init__(self, nb: ttk.Notebook, app) -> None:
        self.app = app
        self.frame = ttk.Frame(nb)
        nb.add(self.frame, text="入口")
        self._build()

    def _build(self) -> None:
        f = self.frame
        row = 0
        ttk.Label(f, text="Cookie").grid(row=row, column=0, sticky="e")
        self.app.var_cookie = tk.StringVar()
        tk.Entry(f, textvariable=self.app.var_cookie, width=60).grid(row=row, column=1, padx=4, pady=2)
        row += 1

        ttk.Label(f, text="Username").grid(row=row, column=0, sticky="e")
        self.app.var_user = tk.StringVar()
        tk.Entry(f, textvariable=self.app.var_user).grid(row=row, column=1, padx=4, pady=2)
        row += 1

        ttk.Label(f, text="Password").grid(row=row, column=0, sticky="e")
        self.app.var_pass = tk.StringVar()
        tk.Entry(f, textvariable=self.app.var_pass, show="*").grid(row=row, column=1, padx=4, pady=2)
        row += 1

        ttk.Label(f, text="Search Keyword").grid(row=row, column=0, sticky="e")
        self.app.var_keyword = tk.StringVar()
        tk.Entry(f, textvariable=self.app.var_keyword).grid(row=row, column=1, padx=4, pady=2)
        row += 1

        ttk.Label(f, text="User ID").grid(row=row, column=0, sticky="e")
        self.app.var_userid = tk.StringVar()
        tk.Entry(f, textvariable=self.app.var_userid).grid(row=row, column=1, padx=4, pady=2)
        row += 1

        btn_frame = ttk.Frame(f)
        btn_frame.grid(row=row, column=0, columnspan=2, pady=4)
        ttk.Button(btn_frame, text="Search", command=self.app._on_search).grid(row=0, column=0, padx=2)
        ttk.Button(btn_frame, text="User Worlds", command=self.app._on_user).grid(row=0, column=1, padx=2)
        ttk.Button(btn_frame, text="Personal Search", command=self.app._search_personal).grid(row=0, column=2, padx=2)
        ttk.Button(btn_frame, text="Taiwan Search", command=self.app._search_taiwan).grid(row=0, column=3, padx=2)
