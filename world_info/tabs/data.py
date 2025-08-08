from __future__ import annotations

import tkinter as tk
from tkinter import ttk


class DataTab:
    def __init__(self, nb: ttk.Notebook, app) -> None:
        self.app = app
        self.frame = ttk.Frame(nb)
        nb.add(self.frame, text="資料")
        self._build()

    def _build(self) -> None:
        frame = ttk.Frame(self.frame)
        frame.pack(fill=tk.BOTH, expand=True)
        self.app.text_data = tk.Text(frame, wrap="word")
        vsb = ttk.Scrollbar(frame, orient="vertical", command=self.app.text_data.yview)
        self.app.text_data.configure(yscrollcommand=vsb.set)
        self.app.text_data.pack(side="left", fill=tk.BOTH, expand=True)
        vsb.pack(side="right", fill=tk.Y)
        ttk.Button(
            self.frame,
            text="Open Filter",
            command=lambda: self.app.nb.select(self.app.tab_filter.frame),
        ).pack(pady=4)
