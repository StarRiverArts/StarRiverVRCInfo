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

        # ── VRChat 登入區塊 ─────────────────────────────────────────────
        login_box = ttk.LabelFrame(f, text="VRChat 登入")
        login_box.pack(fill=tk.X, padx=8, pady=(8, 4))

        row = 0
        ttk.Label(login_box, text="帳號 (Email)").grid(
            row=row, column=0, sticky="e", padx=4, pady=2
        )
        self.app.var_user = tk.StringVar()
        tk.Entry(login_box, textvariable=self.app.var_user, width=36).grid(
            row=row, column=1, sticky="w", padx=4, pady=2
        )
        row += 1

        ttk.Label(login_box, text="密碼").grid(
            row=row, column=0, sticky="e", padx=4, pady=2
        )
        self.app.var_pass = tk.StringVar()
        tk.Entry(login_box, textvariable=self.app.var_pass, show="*", width=36).grid(
            row=row, column=1, sticky="w", padx=4, pady=2
        )
        row += 1

        btn_login_frame = ttk.Frame(login_box)
        btn_login_frame.grid(row=row, column=0, columnspan=2, pady=4)
        ttk.Button(btn_login_frame, text="登入 VRChat", command=self.app._on_login).pack(
            side="left", padx=4
        )
        ttk.Button(
            btn_login_frame,
            text="清除登入",
            command=self.app._on_logout,
        ).pack(side="left", padx=4)
        row += 1

        # 登入狀態標籤
        self.app.var_login_status = tk.StringVar(value="⬜ 尚未登入")
        self.app.lbl_login_status = ttk.Label(
            login_box,
            textvariable=self.app.var_login_status,
            foreground="gray",
        )
        self.app.lbl_login_status.grid(
            row=row, column=0, columnspan=2, pady=(0, 4)
        )
        row += 1

        # ── 雙因素驗證（隱藏，登入需要時才顯示）───────────────────────
        self.app._twofa_frame = ttk.LabelFrame(login_box, text="雙因素驗證 (2FA)")
        self.app._twofa_frame.grid(
            row=row, column=0, columnspan=2, sticky="ew", padx=4, pady=4
        )
        self.app._twofa_frame.grid_remove()   # 預設隱藏

        ttk.Label(self.app._twofa_frame, text="驗證碼").pack(side="left", padx=4)
        self.app.var_twofa_code = tk.StringVar()
        tk.Entry(
            self.app._twofa_frame,
            textvariable=self.app.var_twofa_code,
            width=10,
        ).pack(side="left", padx=4)

        self.app.var_twofa_method = tk.StringVar(value="totp")
        ttk.Label(self.app._twofa_frame, text="方式").pack(side="left")
        self.app._twofa_method_box = ttk.Combobox(
            self.app._twofa_frame,
            textvariable=self.app.var_twofa_method,
            values=["totp", "otp", "emailotp"],
            width=10,
            state="readonly",
        )
        self.app._twofa_method_box.pack(side="left", padx=4)
        ttk.Button(
            self.app._twofa_frame,
            text="送出驗證碼",
            command=self.app._on_verify_2fa,
        ).pack(side="left", padx=4)

        # ── 備用 Cookie 區塊（手動貼上） ────────────────────────────────
        cookie_box = ttk.LabelFrame(f, text="備用：手動貼上 Cookie")
        cookie_box.pack(fill=tk.X, padx=8, pady=4)

        ttk.Label(cookie_box, text="Cookie").grid(
            row=0, column=0, sticky="e", padx=4, pady=2
        )
        self.app.var_cookie = tk.StringVar()
        tk.Entry(cookie_box, textvariable=self.app.var_cookie, width=60).grid(
            row=0, column=1, sticky="w", padx=4, pady=2
        )
        ttk.Button(
            cookie_box,
            text="套用",
            command=self.app._apply_manual_cookie,
        ).grid(row=0, column=2, padx=4)

        # ── 搜尋區塊 ────────────────────────────────────────────────────
        search_box = ttk.LabelFrame(f, text="搜尋")
        search_box.pack(fill=tk.X, padx=8, pady=4)

        ttk.Label(search_box, text="關鍵字").grid(
            row=0, column=0, sticky="e", padx=4, pady=2
        )
        self.app.var_keyword = tk.StringVar()
        tk.Entry(search_box, textvariable=self.app.var_keyword, width=40).grid(
            row=0, column=1, sticky="w", padx=4, pady=2
        )

        ttk.Label(search_box, text="User ID").grid(
            row=1, column=0, sticky="e", padx=4, pady=2
        )
        self.app.var_userid = tk.StringVar()
        tk.Entry(search_box, textvariable=self.app.var_userid, width=40).grid(
            row=1, column=1, sticky="w", padx=4, pady=2
        )

        btn_frame = ttk.Frame(search_box)
        btn_frame.grid(row=2, column=0, columnspan=2, pady=6)
        ttk.Button(btn_frame, text="Keyword 搜尋", command=self.app._on_search).pack(
            side="left", padx=4
        )
        ttk.Button(btn_frame, text="使用者世界", command=self.app._on_user).pack(
            side="left", padx=4
        )
        ttk.Button(btn_frame, text="個人搜尋", command=self.app._search_personal).pack(
            side="left", padx=4
        )
        ttk.Button(btn_frame, text="台灣搜尋", command=self.app._search_taiwan).pack(
            side="left", padx=4
        )
