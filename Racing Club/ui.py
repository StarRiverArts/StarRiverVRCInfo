# Simple Tkinter UI for Racing Club data management

from __future__ import annotations

import random
import tkinter as tk
from tkinter import ttk, filedialog, messagebox


class RacingUI(tk.Tk):
    def __init__(self) -> None:
        super().__init__()
        self.title("Racing Club")
        self.geometry("600x400")

        self.sheet_url_var = tk.StringVar()
        self.local_path_var = tk.StringVar()

        notebook = ttk.Notebook(self)
        notebook.pack(fill=tk.BOTH, expand=True)

        # Tab 0: Data Load
        frame_load = ttk.Frame(notebook)
        notebook.add(frame_load, text="Load Data")
        self._build_load_tab(frame_load)

        # Tab 1: Manual Entry
        frame_manual = ttk.Frame(notebook)
        notebook.add(frame_manual, text="Manual Entry")
        ttk.Label(frame_manual, text="Input racing records here").pack(padx=10, pady=10)

        # Tab 2: Track Fastest (Driver)
        frame_track_driver = ttk.Frame(notebook)
        notebook.add(frame_track_driver, text="Track Fastest")
        ttk.Label(frame_track_driver, text="Fastest driver per track").pack(padx=10, pady=10)

        # Tab 3: Track Fastest (Vehicle)
        frame_track_vehicle = ttk.Frame(notebook)
        notebook.add(frame_track_vehicle, text="Track Vehicle")
        ttk.Label(frame_track_vehicle, text="Fastest vehicle per track").pack(padx=10, pady=10)

        # Tab 4: Vehicle Categories
        frame_vehicle_cat = ttk.Frame(notebook)
        notebook.add(frame_vehicle_cat, text="Vehicle Categories")
        ttk.Label(frame_vehicle_cat, text="Manage vehicles and categories").pack(padx=10, pady=10)

        # Tab 5: Driver Career
        frame_driver = ttk.Frame(notebook)
        notebook.add(frame_driver, text="Driver Career")
        ttk.Label(frame_driver, text="Driver best per track").pack(padx=10, pady=10)

        # Tab 6: Championship
        frame_champ = ttk.Frame(notebook)
        notebook.add(frame_champ, text="Championship")
        self._build_championship_tab(frame_champ)

    def _build_load_tab(self, frame: ttk.Frame) -> None:
        ttk.Label(frame, text="Google Sheet URL:").grid(row=0, column=0, sticky="w", pady=4)
        ttk.Entry(frame, textvariable=self.sheet_url_var, width=50).grid(row=0, column=1, sticky="ew", pady=4)
        ttk.Label(frame, text="Local File Path:").grid(row=1, column=0, sticky="w", pady=4)
        ttk.Entry(frame, textvariable=self.local_path_var, width=50).grid(row=1, column=1, sticky="ew", pady=4)
        ttk.Button(frame, text="Browse", command=self._browse_file).grid(row=1, column=2, padx=4)
        ttk.Button(frame, text="Load", command=self._load_data).grid(row=2, column=1, pady=10)
        frame.columnconfigure(1, weight=1)

    def _browse_file(self) -> None:
        path = filedialog.askopenfilename(title="Select CSV file")
        if path:
            self.local_path_var.set(path)

    def _load_data(self) -> None:
        url = self.sheet_url_var.get()
        path = self.local_path_var.get()
        messagebox.showinfo("Load", f"Pretend loading from\nURL: {url}\nFile: {path}")

    def _build_championship_tab(self, frame: ttk.Frame) -> None:
        self.champ_players: list[str] = []
        entry = ttk.Entry(frame)
        entry.grid(row=0, column=0, padx=4, pady=4)
        ttk.Button(frame, text="Add Player", command=lambda: self._add_player(entry)).grid(row=0, column=1, padx=4)
        self.listbox = tk.Listbox(frame, height=8)
        self.listbox.grid(row=1, column=0, columnspan=2, sticky="nsew", padx=4, pady=4)
        ttk.Button(frame, text="Randomize", command=self._randomize_bracket).grid(row=2, column=0, columnspan=2, pady=4)
        frame.columnconfigure(0, weight=1)
        frame.rowconfigure(1, weight=1)

    def _add_player(self, entry: ttk.Entry) -> None:
        name = entry.get().strip()
        if name:
            self.champ_players.append(name)
            self.listbox.insert(tk.END, name)
            entry.delete(0, tk.END)

    def _randomize_bracket(self) -> None:
        if len(self.champ_players) < 2:
            messagebox.showwarning("Championship", "Need at least two players")
            return
        random.shuffle(self.champ_players)
        bracket = " vs \n".join(
            f"{self.champ_players[i]} vs {self.champ_players[i+1]}"
            for i in range(0, len(self.champ_players) - 1, 2)
        )
        messagebox.showinfo("Bracket", bracket)


if __name__ == "__main__":
    app = RacingUI()
    app.mainloop()
