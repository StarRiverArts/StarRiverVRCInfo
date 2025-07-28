"""Tkinter GUI for fetching and filtering VRChat world data.

The interface provides several tabs:
- Entrance: input authentication (cookie or basic auth), a search keyword
  and creator user ID.
- Data: fetch worlds by keyword and display the raw JSON.
- Filter: filter the keyword results by tag and sort order.
- World List: show the filtered worlds in a simple list.
- User Worlds: fetch and display worlds created by a specific user.

The tool relies on functions in ``scraper/scraper.py``.  Results are saved
under that folder for reuse by other scripts.
"""
from __future__ import annotations

import json
from pathlib import Path
import tkinter as tk
from tkinter import ttk, messagebox

from scraper.scraper import fetch_worlds, _load_headers

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

        self.nb.add(self.tab_entry, text="入口")
        self.nb.add(self.tab_data, text="資料")
        self.nb.add(self.tab_filter, text="篩選")
        self.nb.add(self.tab_list, text="世界列表")
        self.nb.add(self.tab_user, text="個人世界")

        self._build_entry_tab()
        self._build_data_tab()
        self._build_filter_tab()
        self._build_list_tab()
        self._build_user_tab()

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
        self.listbox = tk.Listbox(self.tab_list)
        self.listbox.pack(fill=tk.BOTH, expand=True)

    # User worlds tab
    def _build_user_tab(self) -> None:
        self.text_user = tk.Text(self.tab_user, wrap="word")
        self.text_user.pack(fill=tk.BOTH, expand=True)

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
        self.listbox.delete(0, tk.END)
        for w in self.filtered:
            name = w.get("name") or w.get("世界名稱")
            visits = w.get("visits") or w.get("瀏覽人次")
            self.listbox.insert(tk.END, f"{name} ({visits})")
        self.nb.select(self.tab_list)


def main() -> None:  # pragma: no cover - simple runtime entry
    app = WorldInfoUI()
    app.mainloop()


if __name__ == "__main__":
    main()
