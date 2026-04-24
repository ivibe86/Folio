from __future__ import annotations

import os
import shutil
import sys
import textwrap


class UI:
    RESET = "\033[0m"
    BOLD = "\033[1m"
    DIM = "\033[2m"

    BLACK = "\033[30m"
    RED = "\033[31m"
    GREEN = "\033[32m"
    YELLOW = "\033[33m"
    BLUE = "\033[34m"
    MAGENTA = "\033[35m"
    CYAN = "\033[36m"
    WHITE = "\033[37m"

    BRIGHT_RED = "\033[91m"
    BRIGHT_GREEN = "\033[92m"
    BRIGHT_BLUE = "\033[94m"
    BRIGHT_MAGENTA = "\033[95m"
    BRIGHT_CYAN = "\033[96m"

    def __init__(self) -> None:
        self.enabled = self._supports_color()
        self.theme = self._resolve_theme()
        self.palette = self._build_palette()

    def _supports_color(self) -> bool:
        if os.getenv("NO_COLOR"):
            return False
        if sys.platform.startswith("win"):
            return True
        return hasattr(sys.stdout, "isatty") and sys.stdout.isatty()

    def _resolve_theme(self) -> str:
        requested = os.getenv("FOLIO_SETUP_THEME", "auto").strip().lower()
        if requested in {"plain", "light", "dark"}:
            return requested
        return "auto"

    def _build_palette(self) -> dict[str, str]:
        # Default "auto" intentionally avoids washed-out cyan/yellow body text
        # so it stays readable on both light and dark terminals.
        palettes = {
            "auto": {
                "brand": self.BLUE,
                "brand_sub": self.MAGENTA,
                "step": self.MAGENTA,
                "info": self.BLUE,
                "success": self.GREEN,
                "warning": self.RED,
                "error": self.RED,
                "panel": self.BLUE,
                "muted": self.BLUE,
                "key": self.BLUE,
                "value": self.MAGENTA,
            },
            "light": {
                "brand": self.BLUE,
                "brand_sub": self.MAGENTA,
                "step": self.MAGENTA,
                "info": self.BLUE,
                "success": self.GREEN,
                "warning": self.RED,
                "error": self.RED,
                "panel": self.BLUE,
                "muted": self.BLUE,
                "key": self.BLUE,
                "value": self.MAGENTA,
            },
            "dark": {
                "brand": self.BRIGHT_CYAN,
                "brand_sub": self.BRIGHT_MAGENTA,
                "step": self.BRIGHT_BLUE,
                "info": self.CYAN,
                "success": self.BRIGHT_GREEN,
                "warning": self.YELLOW,
                "error": self.BRIGHT_RED,
                "panel": self.BRIGHT_BLUE,
                "muted": self.DIM,
                "key": self.DIM,
                "value": self.WHITE,
            },
            "plain": {
                "brand": "",
                "brand_sub": "",
                "step": "",
                "info": "",
                "success": "",
                "warning": "",
                "error": "",
                "panel": "",
                "muted": "",
                "key": "",
                "value": "",
            },
        }
        return palettes[self.theme]

    def color(self, text: str, *styles: str) -> str:
        styles = tuple(style for style in styles if style)
        if not self.enabled or not styles:
            return text
        return "".join(styles) + text + self.RESET

    def banner(self) -> None:
        # Folio "F" logo — braille unicode art at 80% size (42×24 chars).
        # Pipeline: threshold at source → upscale ×3 → contrast boost → downscale.
        # Preserves thin outline strokes without background bleed.
        mark = [
            "⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⢀⣠⣤⣶⣶⣾⣷⣾⣶⣶⣶⣶⣶⠄",
            "⠀⠀⠀⠀⠀⠀⠀⠀⠀⠀⣠⣾⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿⣿",
            "⠀⠀⠀⠀⠀⠀⠀⠀⠀⣼⣿⣿⡿⠋⠁⠀⠀⠀⠀⢀⣼⣿⣿⠃",
            "⠀⠀⠀⠀⠀⠀⠀⢀⣼⣿⣿⣏⣤⣤⣤⣤⣤⣤⣶⣿⣟⡽⠃",
            "⠀⠀⠀⠀⢀⣴⠿⠋⡿⢿⣟⣸⣋⣋⣉⣉⣙⣦⡤⠟⠋",
            "⠀⠀⠀⣰⠟⢀⡴⢺⡇⢸⡏⠉⠉⠉⠉⠉⠁",
            "⠀⠀⢰⠃⣰⠋⠀⠸⡇⢸⡇",
            "⠀⠀⡏⢠⡏⠀⠀⠀⡇⢸⡇⣀⣠⣤⣤⣤⣤⣤⣤⣤",
            "⠀⠀⡇⢸⡇⠀⠀⠀⡇⠸⠟⠋⣉⣉⣉⣉⣉⠉⠉⣽",
            "⠀⠀⡇⢸⡇⠀⠀⠀⣇⡴⠚⠉⠉⠉⠉⠉⣩⠇⢠⠃",
            "⠀⠀⡇⢸⠇⠀⠀⠀⠉⢀⣀⣀⣀⣀⣤⠾⠃⣠⠋",
            "⠀⠀⡷⠋⠀⠀⣤⠾⠛⠛⠉⠉⠉⠉⣁⡤⠞⠁",
            "⠀⠀⠁⠀⡰⠋⢁⡤⡖⢛⡏⠉⠉⠉⠁",
            "⠀⠀⠀⡜⠁⡴⠋⠀⡟⢹⡇",
            "⠀⠀⣸⠁⡼⠁⠀⠀⡇⢸⡇",
            "⠀⠀⡇⢠⡇⠀⠀⢰⠃⢸⠁",
            "⠀⠀⡇⢸⡇⠀⣠⠏⢀⠏",
            "⠀⠀⡇⢸⣧⠞⠁⣠⠏",
            "⠀⠀⣇⣀⣀⠴⠚⠁",
            "⠀⠀⠉⠉",
        ]
        print()
        for line in mark:
            print(self.color(f"  {line}", self.palette["brand"], self.BOLD))
        print()
        print(self.color("  Folio Setup", self.palette["brand"], self.BOLD))
        print(self.color("  Personal Finance Studio", self.palette["brand_sub"]))
        print()

    def step(self, num: int, title: str) -> None:
        line = "─" * 58
        print()
        print(self.color(f"  ╭{line}╮", self.palette["step"]))
        print(self.color(f"  │ Step {num}: {title}".ljust(61) + "│", self.palette["step"], self.BOLD))
        print(self.color(f"  ╰{line}╯", self.palette["step"]))
        print()

    def info(self, text: str) -> None:
        print(self.color(f"  • {text}", self.palette["info"], self.BOLD if self.theme != "dark" else ""))

    def success(self, text: str) -> None:
        print(self.color(f"  ✓ {text}", self.palette["success"], self.BOLD))

    def warning(self, text: str) -> None:
        print(self.color(f"  ! {text}", self.palette["warning"], self.BOLD))

    def error(self, text: str) -> None:
        print(self.color(f"  ✕ {text}", self.palette["error"], self.BOLD))

    def muted(self, text: str) -> None:
        print(self.color(f"  {text}", self.palette["muted"]))

    def kv(self, label: str, value: str) -> None:
        label_text = f"  {label:<16}"
        print(
            f"{self.color(label_text, self.palette['key'])} "
            f"{self.color(value, self.palette['value'], self.BOLD if self.palette['value'] else '')}"
        )

    def panel(self, title: str, lines: list[str], color: str | None = None) -> None:
        panel_color = color or self.palette["panel"]
        terminal_width = shutil.get_terminal_size(fallback=(100, 24)).columns
        max_inner_width = max(24, min(terminal_width - 6, 92))

        wrapped_lines: list[str] = []
        for line in lines:
            line = line.rstrip()
            if not line:
                wrapped_lines.append("")
                continue
            wrapped_lines.extend(
                textwrap.wrap(
                    line,
                    width=max_inner_width,
                    break_long_words=False,
                    break_on_hyphens=False,
                )
                or [""]
            )

        width = max([len(title) + 3, *(len(line) for line in wrapped_lines)] + [24])
        print(self.color(f"  ╭─ {title} " + "─" * max(0, width - len(title) - 1) + "╮", panel_color))
        for line in wrapped_lines:
            print(self.color(f"  │ {line.ljust(width)} │", panel_color))
        print(self.color(f"  ╰" + "─" * (width + 2) + "╯", panel_color))


ui = UI()
