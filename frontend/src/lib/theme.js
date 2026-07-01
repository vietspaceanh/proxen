// Theme system: CSS-variable palettes (ported from static/theme.js) plus
// shadcn-standard palettes (zinc/slate/stone). Each palette defines raw vars
// (used by charts via cssVar) AND we derive shadcn --color-* tokens at runtime.

export const THEMES = {
  kanagawa: {
    label: "Kanagawa",
    vars: {
      "--bg": "#1f1f28", "--bg-elevated": "#2a2a37", "--bg-input": "#16161d", "--border": "#52526e",
      "--text": "#dcd7ba", "--text-muted": "#938aa9", "--accent": "#7e9cd8", "--accent-2": "#957fb8",
      "--success": "#98bb6c", "--warning": "#e6c384", "--danger": "#c34043", "--orange": "#e9844a",
      "--chart-grid": "rgba(220,215,186,0.08)",
    },
  },
  "catppuccin-mocha": {
    label: "Catppuccin Mocha",
    vars: {
      "--bg": "#1e1e2e", "--bg-elevated": "#313244", "--bg-input": "#181825", "--border": "#6c7086",
      "--text": "#cdd6f4", "--text-muted": "#a6adc8", "--accent": "#89b4fa", "--accent-2": "#cba6f7",
      "--success": "#a6e3a1", "--warning": "#f9e2af", "--danger": "#f38ba8", "--orange": "#fab387",
      "--chart-grid": "rgba(205,214,244,0.08)",
    },
  },
  "catppuccin-latte": {
    label: "Catppuccin Latte",
    vars: {
      "--bg": "#eff1f5", "--bg-elevated": "#f6f8fa", "--bg-input": "#eef1f5", "--border": "#9aa0b5",
      "--text": "#4c4f69", "--text-muted": "#6c6f85", "--accent": "#1e66f5", "--accent-2": "#8839ef",
      "--success": "#40a02b", "--warning": "#df8e1d", "--danger": "#d20f39", "--orange": "#fe640b",
      "--chart-grid": "rgba(76,79,105,0.10)",
    },
  },
  "github-dark": {
    label: "GitHub Dark",
    vars: {
      "--bg": "#0d1117", "--bg-elevated": "#161b22", "--bg-input": "#010409", "--border": "#484f58",
      "--text": "#e6edf3", "--text-muted": "#8b949e", "--accent": "#58a6ff", "--accent-2": "#bc8cff",
      "--success": "#3fb950", "--warning": "#d29922", "--danger": "#f85149", "--orange": "#db6d28",
      "--chart-grid": "rgba(230,237,243,0.08)",
    },
  },
  dark: {
    label: "Dark",
    vars: {
      "--bg": "#121212", "--bg-elevated": "#1e1e1e", "--bg-input": "#0a0a0a", "--border": "#4d4d4d",
      "--text": "#e0e0e0", "--text-muted": "#a0a0a0", "--accent": "#64b5f6", "--accent-2": "#ba68c8",
      "--success": "#81c784", "--warning": "#ffb74d", "--danger": "#e57373", "--orange": "#ff9800",
      "--chart-grid": "rgba(224,224,224,0.08)",
    },
  },
  light: {
    label: "Light",
    vars: {
      "--bg": "#fafafa", "--bg-elevated": "#ffffff", "--bg-input": "#f0f0f0", "--border": "#b0b0b0",
      "--text": "#212121", "--text-muted": "#757575", "--accent": "#1976d2", "--accent-2": "#7b1fa2",
      "--success": "#388e3c", "--warning": "#f57c00", "--danger": "#d32f2f", "--orange": "#ef6c00",
      "--chart-grid": "rgba(33,33,33,0.10)",
    },
  },
  // ── shadcn-standard palettes ──────────────────────────────────────
  zinc: {
    label: "Zinc",
    vars: {
      "--bg": "#09090b", "--bg-elevated": "#18181b", "--bg-input": "#27272a", "--border": "#61616b",
      "--text": "#fafafa", "--text-muted": "#a1a1aa", "--accent": "#fafafa", "--accent-2": "#d4d4d8",
      "--success": "#22c55e", "--warning": "#eab308", "--danger": "#ef4444", "--orange": "#f97316",
      "--chart-grid": "rgba(250,250,250,0.08)",
    },
  },
  slate: {
    label: "Slate",
    vars: {
      "--bg": "#020617", "--bg-elevated": "#0f172a", "--bg-input": "#1e293b", "--border": "#64748b",
      "--text": "#f1f5f9", "--text-muted": "#94a3b8", "--accent": "#38bdf8", "--accent-2": "#818cf8",
      "--success": "#22c55e", "--warning": "#eab308", "--danger": "#ef4444", "--orange": "#f97316",
      "--chart-grid": "rgba(241,245,249,0.08)",
    },
  },
  stone: {
    label: "Stone",
    vars: {
      "--bg": "#0c0a09", "--bg-elevated": "#1c1917", "--bg-input": "#292524", "--border": "#78716c",
      "--text": "#fafaf9", "--text-muted": "#a8a29e", "--accent": "#d6d3d1", "--accent-2": "#a8a29e",
      "--success": "#22c55e", "--warning": "#eab308", "--danger": "#ef4444", "--orange": "#f97316",
      "--chart-grid": "rgba(250,250,249,0.08)",
    },
  },
};

// Derive shadcn --color-* tokens from a raw palette so components themed
// via Tailwind utilities (bg-background, text-foreground, …) follow the
// active palette at runtime.
function shadcnTokens(v) {
  return {
    "--color-background": v["--bg"],
    "--color-foreground": v["--text"],
    "--color-card": v["--bg-elevated"],
    "--color-card-foreground": v["--text"],
    "--color-popover": v["--bg-elevated"],
    "--color-popover-foreground": v["--text"],
    "--color-primary": v["--accent"],
    "--color-primary-foreground": v["--bg"],
    "--color-secondary": v["--bg-input"],
    "--color-secondary-foreground": v["--text"],
    "--color-muted": v["--bg-input"],
    "--color-muted-foreground": v["--text-muted"],
    "--color-accent": v["--bg-input"],
    "--color-accent-foreground": v["--text"],
    "--color-destructive": v["--danger"],
    "--color-destructive-foreground": v["--text"],
    "--color-border": v["--border"],
    "--color-input": v["--border"],
    "--color-ring": v["--accent"],
    "--color-warning": v["--warning"],
    "--color-warning-foreground": v["--bg"],
  };
}

export function cssVar(name) {
  return getComputedStyle(document.documentElement).getPropertyValue(name).trim();
}

export function applyTheme(name) {
  const theme = THEMES[name] || THEMES["kanagawa"];
  const root = document.documentElement;
  for (const [k, val] of Object.entries(theme.vars)) root.style.setProperty(k, val);
  for (const [k, val] of Object.entries(shadcnTokens(theme.vars))) root.style.setProperty(k, val);
  root.setAttribute("data-theme", name);
  localStorage.setItem("proxen-theme", name);
}

export function initTheme() {
  applyTheme(localStorage.getItem("proxen-theme") || "kanagawa");
}
