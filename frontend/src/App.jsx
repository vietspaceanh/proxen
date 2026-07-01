import { useEffect, useState } from "react";
import { Tabs, TabsList, TabsTrigger } from "./components/ui/tabs";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "./components/ui/select";
import { Monitor } from "./components/Monitor.jsx";
import { Manage } from "./components/Manage.jsx";
import { Analysis } from "./components/Analysis.jsx";
import { THEMES, applyTheme } from "./lib/theme.js";
import { Toaster } from "./components/ui/sonner";

const TAB_ROUTES = { monitor: "/", manage: "/manage", analysis: "/analysis" };
const PATH_TO_TAB = Object.fromEntries(Object.entries(TAB_ROUTES).map(([k, v]) => [v, k]));

export function App() {
  const [tab, setTab] = useState(PATH_TO_TAB[location.pathname] || "monitor");
  const [mounted, setMounted] = useState(() => {
    const t = PATH_TO_TAB[location.pathname] || "monitor";
    return { monitor: t === "monitor", manage: t === "manage", analysis: t === "analysis" };
  });

  useEffect(() => {
    const onPop = () => setTab(PATH_TO_TAB[location.pathname] || "monitor");
    addEventListener("popstate", onPop);
    return () => removeEventListener("popstate", onPop);
  }, []);
  const [stats, setStats] = useState({});
  const [theme, setTheme] = useState(localStorage.getItem("proxen-theme") || "kanagawa");

  // WebSocket: full-state push, auto-reconnect.
  useEffect(() => {
    let ws;
    let timer;
    function connect() {
      const proto = location.protocol === "https:" ? "wss:" : "ws:";
      const key = localStorage.getItem("proxen-admin-key") || "";
      const qs = key ? "?admin_key=" + encodeURIComponent(key) : "";
      ws = new WebSocket(proto + "//" + location.host + "/ws" + qs);
      ws.onmessage = (e) => { try { setStats(JSON.parse(e.data)); } catch (_) {} };
      ws.onclose = () => { timer = setTimeout(connect, 2000); };
      ws.onerror = () => { try { ws.close(); } catch (_) {} };
    }
    connect();
    return () => { clearTimeout(timer); try { ws.close(); } catch (_) {} };
  }, []);

  const onTab = (v) => {
    setTab(v);
    setMounted((m) => ({ ...m, [v]: true }));
    history.pushState(null, "", TAB_ROUTES[v] || "/");
  };

  const onTheme = (v) => {
    setTheme(v);
    applyTheme(v);
  };

  return (
    <Tabs value={tab} onValueChange={onTab} className="h-screen overflow-hidden flex-col gap-0">
      <Toaster />
      <header className="flex items-center justify-between gap-4 px-6 h-[3.25rem] border-b border-border bg-card shrink-0 flex-wrap">
        <div className="flex items-center gap-5">
          <h1 className="text-[0.95rem] font-semibold tracking-tight m-0">
            <span className="text-primary">proxen</span> dashboard
          </h1>
          <TabsList className="h-8">
            <TabsTrigger value="monitor" className="px-3.5">Monitor</TabsTrigger>
            <TabsTrigger value="analysis" className="px-3.5">Analysis</TabsTrigger>
            <TabsTrigger value="manage" className="px-3.5">Manage</TabsTrigger>
          </TabsList>
        </div>
        <div className="flex items-center gap-2">
          <span className="mono text-muted-foreground text-[0.78rem]">theme</span>
          <Select value={theme} onValueChange={onTheme}>
            <SelectTrigger className="w-[140px] cursor-pointer">
              <SelectValue>{THEMES[theme]?.label}</SelectValue>
            </SelectTrigger>
            <SelectContent position="popper">
              {Object.entries(THEMES).map(([k, t]) => <SelectItem key={k} value={k}>{t.label}</SelectItem>)}
            </SelectContent>
          </Select>
        </div>
      </header>
      <main className="flex-1 min-h-0 overflow-hidden">
        {mounted.monitor && <div className={tab === "monitor" ? "h-full overflow-hidden" : "hidden"}><Monitor stats={stats} theme={theme} /></div>}
        {mounted.analysis && <div className={tab === "analysis" ? "h-full overflow-y-auto" : "hidden"}><Analysis stats={stats} theme={theme} active={tab === "analysis"} /></div>}
        {mounted.manage && <div className={tab === "manage" ? "h-full overflow-auto" : "hidden"}><Manage /></div>}
      </main>
    </Tabs>
  );
}
