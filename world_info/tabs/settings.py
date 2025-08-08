from __future__ import annotations

import tkinter as tk
from tkinter import ttk


class SettingsTab:
    def __init__(self, nb: ttk.Notebook, app) -> None:
        self.app = app
        self.frame = ttk.Frame(nb)
        nb.add(self.frame, text="設定")
        self._build()

    def _build(self) -> None:
        f = self.frame
        row = 0
        ttk.Label(f, text="Cookie").grid(row=row, column=0, sticky="e")
        self.app.var_set_cookie = tk.StringVar()
        tk.Entry(f, textvariable=self.app.var_set_cookie, width=60).grid(row=row, column=1, padx=4, pady=2)
        row += 1

        ttk.Label(f, text="個人關鍵字").grid(row=row, column=0, sticky="e")
        self.app.var_personal_kw = tk.StringVar()
        tk.Entry(f, textvariable=self.app.var_personal_kw, width=40).grid(row=row, column=1, padx=4, pady=2)
        row += 1

        ttk.Label(f, text="台灣關鍵字").grid(row=row, column=0, sticky="e")
        self.app.var_taiwan_kw = tk.StringVar()
        tk.Entry(f, textvariable=self.app.var_taiwan_kw, width=40).grid(row=row, column=1, padx=4, pady=2)
        row += 1

        ttk.Label(f, text="黑名單").grid(row=row, column=0, sticky="e")
        self.app.var_blacklist = tk.StringVar()
        tk.Entry(f, textvariable=self.app.var_blacklist, width=40).grid(row=row, column=1, padx=4, pady=2)
        row += 1

        ttk.Label(f, text="玩家ID").grid(row=row, column=0, sticky="e")
        self.app.var_playerid_set = tk.StringVar()
        tk.Entry(f, textvariable=self.app.var_playerid_set).grid(row=row, column=1, padx=4, pady=2)
        row += 1

        ttk.Button(f, text="Save", command=self.app._save_settings).grid(row=row, column=0, columnspan=2, pady=4)
