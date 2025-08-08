from __future__ import annotations

import tkinter as tk
from tkinter import ttk
from pathlib import Path


class AboutTab:
    def __init__(self, nb: ttk.Notebook, app) -> None:
        self.app = app
        self.frame = ttk.Frame(nb)
        nb.add(self.frame, text="關於")
        self._build()

    def _build(self) -> None:
        frame = self.frame
        text = tk.Text(frame, wrap="word")
        vsb = ttk.Scrollbar(frame, orient="vertical", command=text.yview)
        text.configure(yscrollcommand=vsb.set)
        text.pack(side="left", fill=tk.BOTH, expand=True)
        vsb.pack(side="right", fill=tk.Y)
        try:
            readme = Path(__file__).resolve().parent.parent.parent / "README.md"
            text.insert("1.0", readme.read_text(encoding="utf-8"))
        except Exception as e:
            text.insert("1.0", f"無法讀取 README: {e}")
        text.configure(state=tk.DISABLED)
