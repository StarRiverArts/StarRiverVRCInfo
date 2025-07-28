import json
from pathlib import Path
import tkinter as tk
from tkinter import ttk, messagebox

BASE = Path(__file__).resolve().parent
RAW_FILE = BASE / 'scraper' / 'raw_worlds.json'
REVIEW_FILE = BASE / 'scraper' / 'reviewed_worlds.json'


class ReviewUI(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title('World Review')
        self.geometry('600x400')
        self.worlds = self._load_worlds()
        self.reviews = self._load_reviews()
        self.index = 0
        self._build_widgets()
        self._show_world()

    def _load_worlds(self):
        if RAW_FILE.exists():
            with open(RAW_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        messagebox.showerror('Error', f'Missing {RAW_FILE}')
        return []

    def _load_reviews(self):
        if REVIEW_FILE.exists():
            with open(REVIEW_FILE, 'r', encoding='utf-8') as f:
                return json.load(f)
        return {}

    def _build_widgets(self) -> None:
        self.label_name = ttk.Label(self, text='', font=('Arial', 14))
        self.label_name.pack(pady=4)
        self.text_desc = tk.Text(self, height=10, wrap='word')
        self.text_desc.pack(fill=tk.BOTH, expand=True, padx=5)
        frame = ttk.Frame(self)
        frame.pack(pady=4)
        ttk.Button(frame, text='Approve', command=self._approve).grid(row=0, column=0, padx=2)
        ttk.Button(frame, text='Reject', command=self._reject).grid(row=0, column=1, padx=2)
        ttk.Button(frame, text='Skip', command=self._next).grid(row=0, column=2, padx=2)
        self.status_label = ttk.Label(frame, text='')
        self.status_label.grid(row=0, column=3, padx=10)

    def _show_world(self) -> None:
        if self.index >= len(self.worlds):
            messagebox.showinfo('Review', 'No more worlds')
            self.label_name.config(text='')
            self.text_desc.delete('1.0', tk.END)
            self.status_label.config(text='')
            return
        w = self.worlds[self.index]
        self.label_name.config(text=f"{w.get('name')} by {w.get('author')}")
        desc = (
            f"ID: {w.get('worldId')}\n"
            f"Visits: {w.get('visits', 0)}\n"
            f"Tags: {', '.join(w.get('tags', []))}\n\n"
            f"{w.get('description', '')}"
        )
        self.text_desc.delete('1.0', tk.END)
        self.text_desc.insert(tk.END, desc)
        status = self.reviews.get(w['worldId'], 'pending')
        self.status_label.config(text=status)

    def _save(self):
        with open(REVIEW_FILE, 'w', encoding='utf-8') as f:
            json.dump(self.reviews, f, ensure_ascii=False, indent=2)

    def _approve(self):
        if self.index < len(self.worlds):
            w = self.worlds[self.index]
            self.reviews[w['worldId']] = 'approved'
            self._save()
            self.index += 1
            self._show_world()

    def _reject(self):
        if self.index < len(self.worlds):
            w = self.worlds[self.index]
            self.reviews[w['worldId']] = 'rejected'
            self._save()
            self.index += 1
            self._show_world()

    def _next(self):
        self.index += 1
        self._show_world()


def main() -> None:
    app = ReviewUI()
    app.mainloop()


if __name__ == '__main__':
    main()
