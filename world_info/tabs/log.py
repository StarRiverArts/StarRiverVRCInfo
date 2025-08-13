from __future__ import annotations

import tkinter as tk
from tkinter import ttk
import logging


class TextHandler(logging.Handler):
    def __init__(self, text_widget: tk.Text) -> None:
        super().__init__()
        self.text_widget = text_widget

    def emit(self, record: logging.LogRecord) -> None:
        msg = self.format(record)
        if record.levelno >= logging.ERROR:
            tag = "error"
        elif record.levelno >= logging.WARNING:
            tag = "warning"
        else:
            tag = "info"
        self.text_widget.configure(state="normal")
        self.text_widget.insert(tk.END, msg + "\n", tag)
        self.text_widget.configure(state="disabled")
        self.text_widget.see(tk.END)


class LogTab:
    def __init__(self, nb: ttk.Notebook, app) -> None:
        self.app = app
        self.frame = ttk.Frame(nb)
        nb.add(self.frame, text="Log")
        self._build()
        self.handler = TextHandler(self.text)
        formatter = logging.Formatter(
            "%(asctime)s - %(levelname)s - %(message)s", "%H:%M:%S"
        )
        self.handler.setFormatter(formatter)

    def _build(self) -> None:
        self.text = tk.Text(self.frame, state="disabled", wrap="none")
        vsb = ttk.Scrollbar(self.frame, orient="vertical", command=self.text.yview)
        self.text.configure(yscrollcommand=vsb.set)
        self.text.pack(side="left", fill=tk.BOTH, expand=True)
        vsb.pack(side="right", fill=tk.Y)
        self.text.tag_config("error", foreground="red")
        self.text.tag_config("warning", foreground="orange")
