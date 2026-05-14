"""TaiwanTab – dedicated page for Taiwan keyword-search worlds.

Shows the contents of TaiwanWorlds.xlsx in an editable table so the user can
review, edit, and delete bad records without touching the raw spreadsheet.
"""
from __future__ import annotations

import tkinter as tk
from tkinter import ttk

from ..constants import METRIC_COLS, TAIWAN_FILE
from .editable_table import EditableTableFrame


class TaiwanTab:
    def __init__(self, nb: ttk.Notebook, app) -> None:
        self.app = app
        self.frame = ttk.Frame(nb)
        nb.add(self.frame, text="台灣世界")
        self._build()

    def _build(self) -> None:
        f = self.frame

        # ── Toolbar ──────────────────────────────────────────────────
        toolbar = ttk.Frame(f)
        toolbar.pack(fill=tk.X, padx=6, pady=4)

        ttk.Button(
            toolbar, text="🔄 爬取台灣世界", command=self.app._search_taiwan
        ).pack(side="left", padx=4)
        ttk.Button(
            toolbar, text="📂 重新載入 Excel", command=self._reload
        ).pack(side="left", padx=4)

        self.app.var_taiwan_status = tk.StringVar(value="")
        ttk.Label(toolbar, textvariable=self.app.var_taiwan_status,
                  foreground="gray").pack(side="left", padx=8)

        # ── Editable table ────────────────────────────────────────────
        cols = ["爬取日期"] + METRIC_COLS
        self.app.taiwan_editor = EditableTableFrame(
            f,
            columns=cols,
            on_save=self._on_save,
        )
        self.app.taiwan_editor.pack(fill=tk.BOTH, expand=True, padx=4, pady=4)

        # Load existing file on startup
        self._reload()

    def _reload(self) -> None:
        self.app.taiwan_editor.load_xlsx(TAIWAN_FILE)
        n = len(self.app.taiwan_editor.get_rows())
        self.app.var_taiwan_status.set(f"已載入 {n} 筆（{TAIWAN_FILE.name}）")

    def _on_save(self, rows) -> None:
        self.app.taiwan_editor.save_xlsx(TAIWAN_FILE)
        self.app.var_taiwan_status.set(f"已儲存 {len(rows)} 筆至 {TAIWAN_FILE.name}")
