"""SearchResultsTab – merged keyword-search view (filter + raw data + world list).

Replaces the separate FilterTab, DataTab, and ListTab that were previously
scattered across three tabs.
"""
from __future__ import annotations

import tkinter as tk
from tkinter import ttk

from ..constants import METRIC_COLS


class SearchResultsTab:
    def __init__(self, nb: ttk.Notebook, app) -> None:
        self.app = app
        self.frame = ttk.Frame(nb)
        nb.add(self.frame, text="搜尋結果")
        self._build()

    def _build(self) -> None:
        f = self.frame

        # ── Filter bar ────────────────────────────────────────────────
        filter_bar = ttk.LabelFrame(f, text="篩選 / 排序")
        filter_bar.pack(fill=tk.X, padx=6, pady=(6, 2))

        ttk.Label(filter_bar, text="Tag").grid(row=0, column=0, sticky="e", padx=4)
        self.app.var_tag = tk.StringVar(value="all")
        self.app.box_tag = ttk.Combobox(
            filter_bar, textvariable=self.app.var_tag, values=["all"], width=24,
            state="readonly",
        )
        self.app.box_tag.grid(row=0, column=1, padx=4, pady=2)

        ttk.Label(filter_bar, text="排序").grid(row=0, column=2, sticky="e", padx=4)
        self.app.var_sort = tk.StringVar(value="visits")
        ttk.Radiobutton(filter_bar, text="拜訪人次", variable=self.app.var_sort,
                        value="visits").grid(row=0, column=3, padx=2)
        ttk.Radiobutton(filter_bar, text="最新發布", variable=self.app.var_sort,
                        value="latest").grid(row=0, column=4, padx=2)
        ttk.Button(filter_bar, text="套用", command=self.app._apply_filter).grid(
            row=0, column=5, padx=6
        )

        # ── Results notebook (List | Raw JSON) ───────────────────────
        result_nb = ttk.Notebook(f)
        result_nb.pack(fill=tk.BOTH, expand=True, padx=4, pady=4)

        # -- World list sub-tab
        list_frame = ttk.Frame(result_nb)
        result_nb.add(list_frame, text="世界列表")

        cols = ["爬取日期"] + METRIC_COLS
        self.app.tree = ttk.Treeview(list_frame, show="headings",
                                      columns=[str(i) for i in range(len(cols))])
        for i, col in enumerate(cols):
            self.app.tree.heading(str(i), text=col)
            self.app.tree.column(str(i), width=80, anchor="center", stretch=False)
        vsb = ttk.Scrollbar(list_frame, orient="vertical", command=self.app.tree.yview)
        hsb = ttk.Scrollbar(list_frame, orient="horizontal", command=self.app.tree.xview)
        self.app.tree.configure(yscrollcommand=vsb.set, xscrollcommand=hsb.set)
        self.app.tree.grid(row=0, column=0, sticky="nsew")
        vsb.grid(row=0, column=1, sticky="ns")
        hsb.grid(row=1, column=0, sticky="ew")
        list_frame.rowconfigure(0, weight=1)
        list_frame.columnconfigure(0, weight=1)

        # -- Raw JSON sub-tab
        json_frame = ttk.Frame(result_nb)
        result_nb.add(json_frame, text="原始 JSON")

        self.app.text_data = tk.Text(json_frame, wrap="none", font=("Consolas", 9))
        vsb2 = ttk.Scrollbar(json_frame, orient="vertical", command=self.app.text_data.yview)
        hsb2 = ttk.Scrollbar(json_frame, orient="horizontal", command=self.app.text_data.xview)
        self.app.text_data.configure(yscrollcommand=vsb2.set, xscrollcommand=hsb2.set)
        self.app.text_data.grid(row=0, column=0, sticky="nsew")
        vsb2.grid(row=0, column=1, sticky="ns")
        hsb2.grid(row=1, column=0, sticky="ew")
        json_frame.rowconfigure(0, weight=1)
        json_frame.columnconfigure(0, weight=1)
