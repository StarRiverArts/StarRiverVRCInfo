"""Tkinter GUI for fetching and filtering VRChat world data.

The interface provides several tabs:
- Entrance: input authentication (cookie or basic auth), a search keyword
  and creator user ID.
- Data: fetch worlds by keyword and display the raw JSON.
- Filter: filter the keyword results by tag and sort order.
- World List: show the filtered worlds in a simple list.
- User Worlds: fetch and display worlds created by a specific user.

The tool relies on functions in ``scraper/scraper.py``.  Results are saved
under that folder for reuse by other scripts.  Fetching a creator's worlds uses
Playwright to scrape the VRChat website, so the ``playwright`` package must be
installed and ``playwright install`` executed beforehand.
"""
from __future__ import annotations

import json
from pathlib import Path
import tkinter as tk
from tkinter import ttk, messagebox

from scraper.scraper import (
    fetch_worlds,
    _load_headers,
    load_history,
    update_history,
)

BASE = Path(__file__).resolve().parent
RAW_FILE = BASE / "scraper" / "raw_worlds.json"
USER_FILE = BASE / "scraper" / "user_worlds.json"


class WorldInfoUI(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title("World Info")
        self.geometry("800x600")

        self.headers = {}
        self.data: list[dict] = []
        self.user_data: list[dict] = []
        self.filtered: list[dict] = []
        self.history: dict[str, list[dict]] = load_history()

        self._build_tabs()

    # ------------------------------------------------------------------
    # UI construction
    def _build_tabs(self) -> None:
        self.nb = ttk.Notebook(self)
        self.nb.pack(fill=tk.BOTH, expand=True)

        self.tab_entry = ttk.Frame(self.nb)
        self.tab_data = ttk.Frame(self.nb)
        self.tab_filter = ttk.Frame(self.nb)
        self.tab_list = ttk.Frame(self.nb)
        self.tab_user = ttk.Frame(self.nb)
        self.tab_history = ttk.Frame(self.nb)

        self.nb.add(self.tab_entry, text="入口")
        self.nb.add(self.tab_data, text="資料")
        self.nb.add(self.tab_filter, text="篩選")
        self.nb.add(self.tab_list, text="世界列表")
        self.nb.add(self.tab_user, text="個人世界")
        self.nb.add(self.tab_history, text="歷史記錄")

        self._build_entry_tab()
        self._build_data_tab()
        self._build_filter_tab()
        self._build_list_tab()
        self._build_user_tab()
        self._build_history_tab()

    # ------------------------------------------------------------------
    # Entry tab widgets
    def _build_entry_tab(self) -> None:
        f = self.tab_entry
        row = 0
        ttk.Label(f, text="Cookie").grid(row=row, column=0, sticky="e")
        self.var_cookie = tk.StringVar()
        tk.Entry(f, textvariable=self.var_cookie, width=60).grid(row=row, column=1, padx=4, pady=2)
        row += 1

        ttk.Label(f, text="Username").grid(row=row, column=0, sticky="e")
        self.var_user = tk.StringVar()
        tk.Entry(f, textvariable=self.var_user).grid(row=row, column=1, padx=4, pady=2)
        row += 1

        ttk.Label(f, text="Password").grid(row=row, column=0, sticky="e")
        self.var_pass = tk.StringVar()
        tk.Entry(f, textvariable=self.var_pass, show="*").grid(row=row, column=1, padx=4, pady=2)
        row += 1

        ttk.Label(f, text="Search Keyword").grid(row=row, column=0, sticky="e")
        self.var_keyword = tk.StringVar()
        tk.Entry(f, textvariable=self.var_keyword).grid(row=row, column=1, padx=4, pady=2)
        row += 1

        ttk.Label(f, text="User ID").grid(row=row, column=0, sticky="e")
        self.var_userid = tk.StringVar()
        tk.Entry(f, textvariable=self.var_userid).grid(row=row, column=1, padx=4, pady=2)
        row += 1

        btn_frame = ttk.Frame(f)
        btn_frame.grid(row=row, column=0, columnspan=2, pady=4)
        ttk.Button(btn_frame, text="Search", command=self._on_search).grid(row=0, column=0, padx=2)
        ttk.Button(btn_frame, text="User Worlds", command=self._on_user).grid(row=0, column=1, padx=2)

    # ------------------------------------------------------------------
    # Data tab widgets
    def _build_data_tab(self) -> None:
        self.text_data = tk.Text(self.tab_data, wrap="word")
        self.text_data.pack(fill=tk.BOTH, expand=True)
        ttk.Button(self.tab_data, text="Open Filter", command=lambda: self.nb.select(self.tab_filter)).pack(pady=4)

    # Filter tab widgets
    def _build_filter_tab(self) -> None:
        f = self.tab_filter
        ttk.Label(f, text="Tag").grid(row=0, column=0, sticky="e")
        self.var_tag = tk.StringVar(value="all")
        self.box_tag = ttk.Combobox(f, textvariable=self.var_tag, values=["all"])
        self.box_tag.grid(row=0, column=1, padx=4, pady=2)

        ttk.Label(f, text="Sort").grid(row=1, column=0, sticky="e")
        self.var_sort = tk.StringVar(value="popular")
        ttk.Combobox(f, textvariable=self.var_sort, values=["latest", "popular"]).grid(row=1, column=1, padx=4, pady=2)

        ttk.Button(f, text="Apply", command=self._apply_filter).grid(row=2, column=0, columnspan=2, pady=4)

    # World list tab
    def _build_list_tab(self) -> None:
        columns = ("name", "visits", "id")
        self.tree = ttk.Treeview(self.tab_list, columns=columns, show="headings")
        self.tree.heading("name", text="Name")
        self.tree.heading("visits", text="Visits")
        self.tree.heading("id", text="World ID")
        self.tree.column("name", width=250)
        self.tree.column("visits", width=80, anchor="e")
        self.tree.column("id", width=200)
        vsb = ttk.Scrollbar(self.tab_list, orient="vertical", command=self.tree.yview)
        self.tree.configure(yscrollcommand=vsb.set)
        self.tree.pack(side="left", fill=tk.BOTH, expand=True)
        vsb.pack(side="right", fill=tk.Y)

    # User worlds tab
    def _build_user_tab(self) -> None:
        self.text_user = tk.Text(self.tab_user, wrap="word")
        self.text_user.pack(fill=tk.BOTH, expand=True)

    def _build_history_tab(self) -> None:
        f = self.tab_history
        self.var_hist_world = tk.StringVar()
        self.box_hist_world = ttk.Combobox(f, textvariable=self.var_hist_world, values=list(self.history.keys()))
        self.box_hist_world.pack(fill=tk.X, pady=2)
        self.box_hist_world.bind("<<ComboboxSelected>>", self._draw_history)
        self.canvas = tk.Canvas(f, bg="white")
        self.canvas.pack(fill=tk.BOTH, expand=True)
        self._update_history_options()

    # ------------------------------------------------------------------
    # Actions
    def _load_auth_headers(self) -> None:
        cookie = self.var_cookie.get() or None
        user = self.var_user.get() or None
        pw = self.var_pass.get() or None
        self.headers = _load_headers(cookie, user, pw)

    def _on_search(self) -> None:
        self._load_auth_headers()
        keyword = self.var_keyword.get().strip()
        if not keyword:
            messagebox.showerror("Error", "Keyword required")
            return
        try:
            self.data = fetch_worlds(keyword=keyword, limit=50, headers=self.headers)
            with open(RAW_FILE, "w", encoding="utf-8") as f:
                json.dump(self.data, f, ensure_ascii=False, indent=2)
            update_history(self.data)
            self.history = load_history()
            self._update_history_options()
            self.text_data.delete("1.0", tk.END)
            self.text_data.insert(tk.END, json.dumps(self.data, ensure_ascii=False, indent=2))
            self._update_tag_options()
            self.nb.select(self.tab_data)
        except RuntimeError as e:  # pragma: no cover - runtime only
            messagebox.showerror("HTTP Error", str(e))
        except Exception as e:  # pragma: no cover - runtime only
            messagebox.showerror("Error", str(e))

    def _on_user(self) -> None:
        self._load_auth_headers()
        user_id = self.var_userid.get().strip()
        if not user_id:
            messagebox.showerror("Error", "User ID required")
            return
        try:
            self.user_data = fetch_worlds(user_id=user_id, limit=50, headers=self.headers)
            with open(USER_FILE, "w", encoding="utf-8") as f:
                json.dump(self.user_data, f, ensure_ascii=False, indent=2)
            update_history(self.user_data)
            self.history = load_history()
            self._update_history_options()
            self.text_user.delete("1.0", tk.END)
            self.text_user.insert(tk.END, json.dumps(self.user_data, ensure_ascii=False, indent=2))
            self.nb.select(self.tab_user)
        except RuntimeError as e:  # pragma: no cover - runtime only
            messagebox.showerror("HTTP Error", str(e))
        except Exception as e:  # pragma: no cover - runtime only
            messagebox.showerror("Error", str(e))

    def _update_tag_options(self) -> None:
        tags = set()
        for w in self.data:
            for t in w.get("tags", []):
                tags.add(t)
        self.box_tag["values"] = ["all"] + sorted(tags)
        self.var_tag.set("all")

    def _apply_filter(self) -> None:
        worlds = list(self.data)
        tag = self.var_tag.get()
        if tag != "all":
            worlds = [w for w in worlds if tag in w.get("tags", [])]
        if self.var_sort.get() == "latest":
            worlds.sort(key=lambda w: w.get("publicationDate", ""), reverse=True)
        else:
            worlds.sort(key=lambda w: w.get("visits", 0), reverse=True)
        self.filtered = worlds
        for item in self.tree.get_children():
            self.tree.delete(item)
        for w in self.filtered:
            name = w.get("name") or w.get("世界名稱")
            visits = w.get("visits") or w.get("瀏覽人次")
            world_id = w.get("id") or w.get("世界ID")
            self.tree.insert("", tk.END, values=(name, visits, world_id))
        self.nb.select(self.tab_list)

    def _update_history_options(self) -> None:
        self.box_hist_world["values"] = list(self.history.keys())
        if self.history:
            self.var_hist_world.set(list(self.history.keys())[0])
            self._draw_history()

    def _draw_history(self, event=None) -> None:
        world_id = self.var_hist_world.get()
        data = self.history.get(world_id, [])
        self.canvas.delete("all")
        if not data:
            return
        width = int(self.canvas.winfo_width() or 600)
        height = int(self.canvas.winfo_height() or 300)
        pad = 40
        times = [d["timestamp"] for d in data]
        min_t = min(times)
        max_t = max(times)
        if max_t == min_t:
            max_t += 1
        scale_x = width - 2 * pad
        scale_y = height - 2 * pad

        def xy(idx, val, max_val):
            x = pad + (times[idx] - min_t) / (max_t - min_t) * scale_x
            y = height - pad - min(val, max_val) / max_val * scale_y
            return x, y

        colors = {
            "visits": "blue",
            "favorites": "green",
            "heat": "red",
            "popularity": "purple",
        }
        limits = {
            "visits": 5000,
            "favorites": 5000,
            "heat": 10,
            "popularity": 10,
        }
        for key, color in colors.items():
            points = [xy(i, d.get(key, 0), limits[key]) for i, d in enumerate(data)]
            for a, b in zip(points, points[1:]):
                self.canvas.create_line(a[0], a[1], b[0], b[1], fill=color)
        # axes
        self.canvas.create_line(pad, height - pad, width - pad, height - pad)
        self.canvas.create_line(pad, pad, pad, height - pad)


def main() -> None:  # pragma: no cover - simple runtime entry
    app = WorldInfoUI()
    app.mainloop()


if __name__ == "__main__":
    main()
