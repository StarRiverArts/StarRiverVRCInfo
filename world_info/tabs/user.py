from __future__ import annotations

import tkinter as tk
from tkinter import ttk

from ..constants import METRIC_COLS, LEGEND_TEXT


class UserTab:
    def __init__(self, nb: ttk.Notebook, app) -> None:
        self.app = app
        self.frame = ttk.Frame(nb)
        nb.add(self.frame, text="個人世界")
        self._build()

    def _build(self) -> None:
        f = self.frame
        self.app.user_nb = ttk.Notebook(f)
        self.app.user_nb.pack(fill=tk.BOTH, expand=True)

        self.app.tab_dashboard = ttk.Frame(self.app.user_nb)
        self.app.tab_detail = ttk.Frame(self.app.user_nb)
        self.app.user_nb.add(self.app.tab_dashboard, text="儀表板")
        self.app.user_nb.add(self.app.tab_detail, text="詳細列表")

        self._build_dashboard_tab()

        self.app.detail_nb = ttk.Notebook(self.app.tab_detail)
        self.app.detail_nb.pack(fill=tk.BOTH, expand=True)

        self.app.tab_user_list = ttk.Frame(self.app.detail_nb)
        self.app.detail_nb.add(self.app.tab_user_list, text="所有世界")

        control = ttk.Frame(self.app.tab_user_list)
        control.pack(fill=tk.X)
        ttk.Button(control, text="Reload", command=self.app._load_local_tables).pack(side="left")

        tree_frame = ttk.Frame(self.app.tab_user_list)
        tree_frame.pack(fill=tk.BOTH, expand=True)
        self.app.user_tree = ttk.Treeview(tree_frame, show="headings")
        columns = ["爬取日期"] + METRIC_COLS
        self.app.user_tree["columns"] = list(range(len(columns)))
        for idx, col in enumerate(columns):
            self.app.user_tree.heading(str(idx), text=col)
            self.app.user_tree.column(str(idx), width=80, anchor="center")
        vsb = ttk.Scrollbar(tree_frame, orient="vertical", command=self.app.user_tree.yview)
        self.app.user_tree.configure(yscrollcommand=vsb.set)
        self.app.user_tree.pack(side="left", fill=tk.BOTH, expand=True)
        vsb.pack(side="right", fill=tk.Y)
        self.app.user_tree.bind("<<TreeviewSelect>>", self.app._on_select_user_world)

        self.app.current_world_id = None
        self.app.user_canvas = tk.Canvas(self.app.tab_user_list, bg="white")
        self.app.user_canvas.pack(fill=tk.BOTH, expand=True)
        self.app.user_canvas.bind(
            "<Configure>", lambda e: self.app._draw_user_chart(self.app.current_world_id)
        )
        ttk.Label(self.app.tab_user_list, text=LEGEND_TEXT).pack()

    def _build_dashboard_tab(self) -> None:
        f = self.app.tab_dashboard
        tree_frame = ttk.Frame(f)
        tree_frame.pack(fill=tk.X)
        self.app.dash_tree = ttk.Treeview(tree_frame, show="headings")
        self.app.dash_tree["columns"] = list(range(len(METRIC_COLS)))
        for idx, col in enumerate(METRIC_COLS):
            self.app.dash_tree.heading(str(idx), text=col)
            self.app.dash_tree.column(str(idx), width=80, anchor="center")
        self.app.dash_tree.pack(side="left", fill=tk.X, expand=True)
        dash_vsb = ttk.Scrollbar(tree_frame, orient="vertical", command=self.app.dash_tree.yview)
        dash_vsb.pack(side="right", fill=tk.Y)
        self.app.dash_tree.configure(yscrollcommand=dash_vsb.set)

        self.app.chart_canvas = tk.Canvas(f)
        chart_vsb = ttk.Scrollbar(f, orient="vertical", command=self.app.chart_canvas.yview)
        self.app.chart_canvas.configure(yscrollcommand=chart_vsb.set)
        self.app.chart_canvas.pack(side="left", fill=tk.BOTH, expand=True)
        chart_vsb.pack(side="right", fill=tk.Y)
        self.app.chart_container = ttk.Frame(self.app.chart_canvas)
        self.app.chart_window = self.app.chart_canvas.create_window(
            (0, 0), window=self.app.chart_container, anchor="nw"
        )
        self.app.chart_container.bind(
            "<Configure>",
            lambda e: self.app.chart_canvas.configure(
                scrollregion=self.app.chart_canvas.bbox("all")
            ),
        )
        self.app.chart_canvas.bind(
            "<Configure>",
            lambda e: (
                self.app.chart_canvas.itemconfigure(self.app.chart_window, width=e.width),
                self.app._arrange_dashboard_charts(e),
            ),
        )
        self.app.chart_frames = []
