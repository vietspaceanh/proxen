import { createRoot } from "react-dom/client";
import { App } from "./App.jsx";
import "./styles.css";

import { initTheme } from "./lib/theme.js";
initTheme();

createRoot(document.getElementById("app")).render(<App />);
