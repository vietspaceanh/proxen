import { useEffect, useLayoutEffect, useRef, useState } from "react";
import { Card, CardContent, CardHeader, CardTitle } from "./ui/card";
import { Button } from "./ui/button";
import { Badge } from "./ui/badge";
import { Table, TableBody, TableCell, TableHead, TableHeader, TableRow } from "./ui/table";
import { Input } from "./ui/input";
import { Field as UIField, FieldLabel, FieldError } from "./ui/field";
import { Select, SelectContent, SelectItem, SelectTrigger, SelectValue } from "./ui/select";
import { DropdownMenu, DropdownMenuContent, DropdownMenuItem, DropdownMenuLabel, DropdownMenuTrigger } from "./ui/dropdown-menu";
import { Tooltip, TooltipContent, TooltipProvider, TooltipTrigger } from "./ui/tooltip";
import { Dialog, DialogContent, DialogFooter, DialogHeader, DialogTitle } from "./ui/dialog";
import { api, randomKey, fmtAgo, fmtCompact, parseLimits, buildLimits, buildModelBody } from "../lib/format.js";
import { toast } from "./ui/sonner";
import { Wand2, ClipboardCopyIcon, CheckIcon, RefreshCw, GripVertical } from "lucide-react";

// ─── upstream model endpoints ─────────────────────────────────────────
const getAvailableModels = (name) =>
  api("GET", `/api/management/upstreams/${encodeURIComponent(name)}/available-models`)
    .then((r) => r.data || []);

const fetchProviderModels = (name) =>
  api("POST", `/api/management/upstreams/${encodeURIComponent(name)}/fetch-models`)
    .then((r) => r.data || []);

// ─── small form helpers ──────────────────────────────────────────────

function LimitRow({ label, children }) {
  return (
    <div className="flex items-center gap-2.5 mb-2.5">
      <span className="text-muted-foreground text-[0.72rem] font-medium uppercase tracking-wide w-[76px] shrink-0">{label}</span>
      <div className="flex items-center gap-2 flex-1">{children}</div>
    </div>
  );
}

function Check({ checked, onChange, children }) {
  return (
    <label className="inline-flex items-center gap-1.5 text-[0.83rem] cursor-pointer">
      <input type="checkbox" checked={checked} onChange={(e) => onChange(e.target.checked)} style={{ accentColor: "var(--accent)" }} />
      {children}
    </label>
  );
}

// Transition `open→false` before unmounting so Radix Presence can clean up
// animation state. Without this, rapid open/close corrupts CSS animation state,
// leaving the dialog stuck at low opacity. 150ms matches the animation duration.
function FormDialog({ title, onClose, children, footer, className }) {
  const [open, setOpen] = useState(true);
  return (
    <Dialog open={open} onOpenChange={(o) => { if (!o) { setOpen(false); setTimeout(onClose, 150); } }}>
      <DialogContent className={className}>
        <DialogHeader><DialogTitle>{title}</DialogTitle></DialogHeader>
        <div className="grid gap-3 py-1 overflow-y-auto max-h-[calc(80vh-8rem)] scrollbar-thin">{children}</div>
        {footer && <DialogFooter>{footer}</DialogFooter>}
      </DialogContent>
    </Dialog>
  );
}

function RateLimitsFields({ limits, onChange }) {
  const suffix = <span className="text-muted-foreground text-[0.76rem] shrink-0">hrs</span>;
  return (
    <>
      <div className="text-muted-foreground text-[0.82rem] font-semibold uppercase tracking-wide mt-3">Rate Limits</div>
      <LimitRow label="Inflight"><Input type="number" placeholder="—" value={limits.inflight} onChange={(e) => onChange({ ...limits, inflight: e.target.value })} /></LimitRow>
      <LimitRow label="Requests"><Input type="number" placeholder="max" value={limits.req} onChange={(e) => onChange({ ...limits, req: e.target.value })} /><span className="text-muted-foreground text-[0.76rem]">per</span><Input type="number" step="0.5" min="0" placeholder="hrs" className="w-[90px]" value={limits.reqWin} onChange={(e) => onChange({ ...limits, reqWin: e.target.value })} />{suffix}</LimitRow>
      <LimitRow label="Tokens"><Input type="number" placeholder="max" value={limits.tok} onChange={(e) => onChange({ ...limits, tok: e.target.value })} /><span className="text-muted-foreground text-[0.76rem]">per</span><Input type="number" step="0.5" min="0" placeholder="hrs" className="w-[90px]" value={limits.tokWin} onChange={(e) => onChange({ ...limits, tokWin: e.target.value })} />{suffix}</LimitRow>
    </>
  );
}

// ─── Route editor ────────────────────────────────────────────────────

function RouteEditor({ route, index, upstreams, availableModels, onProviderChange, onModelChange, onRemove, onToggle, onFetch, fetching, dragIndex, onDragStart, onDragOver, onDragEnd }) {
  const providerModels = availableModels[route.upstream_name] || [];
  const canDrag = useRef(false);
  return (
    <div
      className={`border border-border rounded-lg p-3 mb-2 transition ${dragIndex === index ? "opacity-40" : ""}`}
      data-route-key={route.rid}
      draggable={route.enabled !== false}
      onMouseDown={(e) => { canDrag.current = !!e.target.closest("[data-drag-handle]"); }}
      onDragStart={(e) => { if (!canDrag.current) { e.preventDefault(); return; } onDragStart(e, index); }}
      onDragOver={onDragOver}
      onDragEnd={onDragEnd}
    >
      <div className="flex items-center justify-between mb-2">
        <div className="flex items-center gap-1.5">
          {route.enabled !== false && (
            <span data-drag-handle className="cursor-grab active:cursor-grabbing text-muted-foreground/50 hover:text-foreground">
              <GripVertical className="size-3.5" />
            </span>
          )}
          <span className="text-muted-foreground text-[0.72rem] font-medium uppercase tracking-wide">
            #{index + 1} {index === 0 ? "(primary)" : route.enabled === false ? "(disabled)" : "(fallback)"}
          </span>
        </div>
        {index > 0 && (
          <div className="inline-flex border border-border rounded-md overflow-hidden shrink-0">
            <Button variant="ghost" size="xs" onClick={onToggle}>{route.enabled === false ? "enable" : "disable"}</Button>
            <Button variant="ghost" size="xs" className="text-destructive" onClick={onRemove}>remove</Button>
          </div>
        )}
      </div>
      <div className={`grid grid-cols-2 gap-2.5 ${route.enabled === false ? "opacity-50" : ""}`}>
        <UIField>
          <FieldLabel>Provider</FieldLabel>
          <Select value={route.upstream_name || undefined} onValueChange={(v) => onProviderChange(index, v)}>
            <SelectTrigger className="w-full"><SelectValue placeholder="select provider">{route.upstream_name}</SelectValue></SelectTrigger>
            <SelectContent position="popper">
              {upstreams.map((u) => <SelectItem key={u.name} value={u.name}>{u.name}</SelectItem>)}
            </SelectContent>
          </Select>
        </UIField>
        <UIField>
          <FieldLabel>Upstream Model ID</FieldLabel>
          <div className="flex gap-1.5">
            <Select value={route.upstream_model_id || undefined} onValueChange={(v) => onModelChange(index, v)} disabled={!route.upstream_name}>
              <SelectTrigger className="flex-1"><SelectValue placeholder={route.upstream_name ? "select model" : "select provider first"}>{route.upstream_model_id}</SelectValue></SelectTrigger>
              <SelectContent position="popper">
                {providerModels.map((m) => <SelectItem key={m.id} value={m.id}>{m.id}</SelectItem>)}
                {providerModels.length === 0 && route.upstream_name && (
                  <div className="px-1.5 py-1 text-[0.75rem] italic text-muted-foreground/80">No models in cache</div>
                )}
              </SelectContent>
            </Select>
            <Button variant="outline" size="icon" className="shrink-0" disabled={!route.upstream_name || fetching} onClick={() => onFetch(route.upstream_name)} title={fetching ? "Fetching..." : "Fetch models from provider"}>
              <RefreshCw className={fetching ? "animate-spin" : ""} />
            </Button>
          </div>
        </UIField>
      </div>
    </div>
  );
}

function ModelRow({ model, selected }) {
  const routes = model.routes || [];
  const primary = routes.find((r) => r.sort_order === 0) || routes[0];
  const fallbacks = routes.filter((r) => r !== primary);
  return (
    <TableRow className="group/row">
      <TableCell className="w-8 p-0">
        <button
          className={`flex items-center justify-center w-4 h-4 rounded transition-all mx-auto ${
            selected
              ? "bg-primary border border-primary text-primary-foreground opacity-100"
              : "opacity-0 border border-foreground/50 group-hover/row:opacity-60"
          }`}
          data-action="select"
          data-id={model.id}
        >
          {selected && (
            <svg width="12" height="12" viewBox="0 0 16 16" fill="none">
              <path d="M3.5 8L7 11.5L12.5 5.5" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
            </svg>
          )}
        </button>
      </TableCell>
      <TableCell className="mono font-medium">{model.id}</TableCell>
      <TableCell className="text-[0.78rem]">{primary ? <Badge variant="outline" className={`text-[0.68rem] font-normal whitespace-nowrap ${primary.enabled === false ? "opacity-40 line-through" : ""}`}>{primary.upstream_name} / {primary.upstream_model_id}</Badge> : <span className="text-muted-foreground">—</span>}</TableCell>
      <TableCell className="text-[0.78rem] align-top">{fallbacks.length > 0 ? <div className="flex flex-wrap gap-1 max-h-[52px] overflow-y-auto scrollbar-thin">{fallbacks.map((r) => <Badge key={r.upstream_name + r.upstream_model_id} variant="outline" className={`text-[0.68rem] font-normal shrink-0 whitespace-nowrap ${r.enabled === false ? "opacity-40 line-through" : ""}`}>{r.upstream_name} / {r.upstream_model_id}</Badge>)}</div> : <span className="text-muted-foreground">—</span>}</TableCell>
      <TableCell>{fmtCompact(model.max_input_tokens)}</TableCell>
      <TableCell>{fmtCompact(model.max_output_tokens)}</TableCell>
      <TableCell>{model.input_per_1m != null ? "$" + model.input_per_1m : ""}</TableCell>
      <TableCell>{model.cached_input_per_1m != null ? "$" + model.cached_input_per_1m : ""}</TableCell>
      <TableCell>{model.output_per_1m != null ? "$" + model.output_per_1m : ""}</TableCell>
      <TableCell><Badge variant={model.enabled ? "default" : "secondary"}>{model.enabled ? "on" : "off"}</Badge></TableCell>
      <TableCell>
        <div className="inline-flex border border-border rounded-md overflow-hidden">
          <Button variant="ghost" size="xs" data-action="edit" data-id={model.id}>edit</Button>
        </div>
      </TableCell>
    </TableRow>
  );
}

function ModelsTable({ models, onEdit, onAdd, onImport, onBulkDelete }) {
  const [selectedIds, setSelectedIds] = useState(new Set());
  const hasSelection = selectedIds.size > 0;
  const allSelected = models.length > 0 && selectedIds.size === models.length;
  const someSelected = hasSelection && !allSelected;

  const toggleSelect = (id) => {
    setSelectedIds(prev => {
      const next = new Set(prev);
      if (next.has(id)) next.delete(id); else next.add(id);
      return next;
    });
  };

  const handleAction = (e) => {
    const el = e.target.closest("[data-action]");
    if (!el) return;
    const { action, id } = el.dataset;
    if (action === "select") toggleSelect(id);
    else if (action === "edit") {
      const model = models.find(m => m.id === id);
      if (model) onEdit(model);
    } else if (action === "select-all") {
      setSelectedIds(prev => {
        if (models.length > 0 && prev.size === models.length) return new Set();
        return new Set(models.map(m => m.id));
      });
    }
  };

  return (
    <Card className="md:col-span-2 xl:col-span-3">
      <CardHeader className="flex flex-row items-center justify-between flex-wrap gap-2">
        {hasSelection ? (
          <>
            <CardTitle>{selectedIds.size} selected</CardTitle>
            <div className="flex gap-2">
              <Button variant="outline" size="sm" onClick={() => setSelectedIds(new Set())}>Cancel</Button>
              <Button variant="destructive" size="sm" onClick={() => onBulkDelete([...selectedIds])}>Delete Selected</Button>
            </div>
          </>
        ) : (
          <>
            <CardTitle>Models</CardTitle>
            <div className="flex gap-2">
              <Button variant="outline" size="sm" onClick={onImport}>Import</Button>
              <Button size="sm" onClick={onAdd}>Add</Button>
            </div>
          </>
        )}
      </CardHeader>
      <CardContent>
        <Table className="group/table" onClick={handleAction} containerClassName="rounded">
          <TableHeader><TableRow>
            <TableHead className="w-8">
              <button
                className={`flex items-center justify-center w-4 h-4 rounded transition-all mx-auto ${
                  allSelected || someSelected
                    ? "bg-primary border border-primary text-primary-foreground opacity-100"
                    : "opacity-0 border border-foreground/50 group-hover/table:opacity-60"
                }`}
                data-action="select-all"
              >
                {allSelected && (
                  <svg width="12" height="12" viewBox="0 0 16 16" fill="none">
                    <path d="M3.5 8L7 11.5L12.5 5.5" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
                  </svg>
                )}
                {someSelected && (
                  <svg width="12" height="12" viewBox="0 0 16 16" fill="none">
                    <path d="M3 8H13" stroke="currentColor" strokeWidth="2" strokeLinecap="round" />
                  </svg>
                )}
              </button>
            </TableHead>
            <TableHead>Proxen Model ID</TableHead><TableHead>Primary</TableHead><TableHead>Fallbacks</TableHead><TableHead>Max In</TableHead><TableHead>Max Out</TableHead>
            <TableHead>Input/1M</TableHead><TableHead>Cached/1M</TableHead><TableHead>Output/1M</TableHead><TableHead>On</TableHead><TableHead />
          </TableRow></TableHeader>
          <TableBody>
            {models.length === 0
              ? <TableRow><TableCell colSpan={11} className="text-muted-foreground text-center py-5">No models yet. Add one or import from a provider.</TableCell></TableRow>
              : models.map((m) => <ModelRow key={m.id} model={m} selected={selectedIds.has(m.id)} />)}
          </TableBody>
        </Table>
      </CardContent>
    </Card>
  );
}

// ─── GatePanel ───────────────────────────────────────────────────────

function GatePanel() {
  const [gateLimits, setGateLimits] = useState({ max_inflight: 5, max_waiting: 50 });
  useEffect(() => {
    (async () => {
      try { const g = await api("GET", "/api/management/gate"); setGateLimits(g); }
      catch (e) { toast.error(e.message); }
    })();
  }, []);
  const saveGate = async () => {
    try { const data = await api("PATCH", "/api/management/gate", gateLimits); setGateLimits(data); toast.success("limits updated"); }
    catch (e) { toast.error(e.message); }
  };
  return (
    <Card>
      <CardHeader><CardTitle>Global Concurrency</CardTitle></CardHeader>
      <CardContent className="flex flex-col gap-1">
        <div className="flex justify-between items-center py-1.5 border-b border-border text-[0.83rem]"><span className="text-muted-foreground">Max active</span><strong className="tabular-nums">{gateLimits.max_inflight}</strong></div>
        <div className="flex justify-between items-center py-1.5 border-b border-border text-[0.83rem]"><span className="text-muted-foreground">Max waiting</span><strong className="tabular-nums">{gateLimits.max_waiting}</strong></div>
        <div className="grid grid-cols-2 gap-2.5 mt-3">
          <UIField><FieldLabel>Max active</FieldLabel><Input type="number" min="1" value={gateLimits.max_inflight} onChange={(e) => setGateLimits({ ...gateLimits, max_inflight: parseInt(e.target.value) || 1 })} /></UIField>
          <UIField><FieldLabel>Max waiting</FieldLabel><Input type="number" min="0" value={gateLimits.max_waiting} onChange={(e) => setGateLimits({ ...gateLimits, max_waiting: parseInt(e.target.value) || 0 })} /></UIField>
        </div>
        <Button className="mt-2.5 self-end" onClick={saveGate}>Save Limits</Button>
      </CardContent>
    </Card>
  );
}

// ─── Manage ──────────────────────────────────────────────────────────

function ManageImpl() {
  const [adminKeyVal, setAdminKeyVal] = useState(localStorage.getItem("proxen-admin-key") || "");
  const [unlocked, setUnlocked] = useState(false);
  const [checking, setChecking] = useState(true);
  const [disabled, setDisabled] = useState(false);
  const [upstreams, setUpstreams] = useState([]);
  const [keys, setKeys] = useState([]);
  const [models, setModels] = useState([]);
  const [modal, setModal] = useState(null);
  const modelIds = models.map(m => m.id);

  const loadAll = async () => {
    try {
      const [u, k, m] = await Promise.all([
        api("GET", "/api/management/upstreams"),
        api("GET", "/api/management/keys"),
        api("GET", "/api/management/models"),
      ]);
      setUpstreams(u.data || []);
      setKeys(k.data || []);
      setModels(m.data || []);
    } catch (e) { toast.error(e.message); }
  };

  const tryUnlock = async (key) => {
    try {
      const status = await api("GET", "/api/management/status");
      if (!status.enabled) { setDisabled(true); return; }
      if (!key) return;
      setUnlocked(true);
      await loadAll();
    } catch (_e) {
      setUnlocked(false);
    } finally {
      setChecking(false);
    }
  };

  useEffect(() => { tryUnlock(adminKeyVal); }, []);

  const saveAdminKey = () => {
    localStorage.setItem("proxen-admin-key", adminKeyVal);
    tryUnlock(adminKeyVal);
  };

  const refreshProviders = async () => {
    const u = await api("GET", "/api/management/upstreams");
    setUpstreams(u.data || []);
  };
  const refreshKeys = async () => { const k = await api("GET", "/api/management/keys"); setKeys(k.data || []); };
  const refreshModels = async () => { const m = await api("GET", "/api/management/models"); setModels(m.data || []); };

  const saveProvider = async (data, existingName) => {
    try {
      if (existingName) {
        await api("PUT", `/api/management/upstreams/${encodeURIComponent(existingName)}`, data);
      } else {
        try { await api("PUT", `/api/management/upstreams/${encodeURIComponent(data.name)}`, data); }
        catch (e) { if (String(e.message).includes("not found")) await api("POST", "/api/management/upstreams", data); else throw e; }
      }
      toast.success("provider saved"); setModal(null); await refreshProviders();
    } catch (e) { toast.error(e.message); }
  };

  const deleteProvider = async (name) => {
    if (!confirm(`Delete provider '${name}'?`)) return;
    try { await api("DELETE", `/api/management/upstreams/${encodeURIComponent(name)}`); toast.success("provider deleted"); await refreshProviders(); }
    catch (e) { toast.error(e.message); }
  };

  const saveKey = async (keyVal, label, limits) => {
    const resp = await api("POST", "/api/management/keys", { key: keyVal, label });
    if (limits && (limits.max_inflight != null || limits.max_requests != null || limits.max_tokens != null) && resp.id) {
      await api("PUT", `/api/management/keys/${resp.id}/limits`, limits);
    }
    await refreshKeys();
    return resp;
  };

  const toggleKey = async (id, active) => {
    try { await api("PATCH", `/api/management/keys/${id}`, { active }); toast.success(active ? "key enabled" : "key disabled"); await refreshKeys(); }
    catch (e) { toast.error(e.message); }
  };

  const deleteKey = async (id) => {
    if (!confirm("Delete this key?")) return;
    try { await api("DELETE", `/api/management/keys/${id}`); toast.success("key deleted"); await refreshKeys(); }
    catch (e) { toast.error(e.message); }
  };

  const saveKeyEdit = async (id, label, limits) => {
    try { await Promise.all([api("PATCH", `/api/management/keys/${id}`, { label }), api("PUT", `/api/management/keys/${id}/limits`, limits)]); toast.success("key updated"); setModal(null); await refreshKeys(); }
    catch (e) { toast.error(e.message); }
  };

  const clearKeyLimits = async (id) => {
    try { await api("DELETE", `/api/management/keys/${id}/limits`); toast.success("limits cleared"); await refreshKeys(); }
    catch (e) { toast.error(e.message); }
  };

  const saveModel = async (modelId, body, isEdit) => {
    try {
      if (isEdit) {
        await api("PUT", `/api/management/models/${encodeURIComponent(modelId)}`, body);
      } else {
        await api("POST", "/api/management/models", body);
      }
      toast.success("model saved"); setModal(null); await refreshModels();
    } catch (e) { toast.error(e.message); }
  };

  const deleteModel = async (modelId) => {
    if (!confirm(`Delete model '${modelId}'?`)) return;
    try { await api("DELETE", `/api/management/models/${encodeURIComponent(modelId)}`); toast.success("model deleted"); setModal(null); await refreshModels(); }
    catch (e) { toast.error(e.message); }
  };

  const closeModal = () => setModal(null);
  const closeAndRefreshModels = () => { setModal(null); refreshModels(); };
  const handleEditModel = (model) => setModal({ type: "model", edit: model });
  const handleAddModel = () => setModal({ type: "model" });
  const handleImportModels = () => setModal({ type: "import" });
  const handleBulkDelete = async (ids) => {
    if (!confirm(`Delete ${ids.length} model(s)?`)) return;
    try { await api("POST", "/api/management/models/bulk-delete", { ids }); toast.success(`${ids.length} models deleted`); await refreshModels(); }
    catch (e) { toast.error(e.message); }
  };

  // ── render branches ──

  if (checking) return null;

  if (disabled) {
    return (
      <div className="px-6 py-5 max-w-[1400px] mx-auto">
        <Card className="max-w-md"><CardContent>
          <p className="text-muted-foreground text-[0.83rem]">Management is disabled on the server. Set <code>admin_api_keys</code> in config to enable.</p>
        </CardContent></Card>
      </div>
    );
  }

  if (!unlocked) {
    return (
      <div className="px-6 py-5 max-w-[1400px] mx-auto">
        <Card className="max-w-md">
          <CardContent className="flex flex-col gap-3">
            <div className="text-muted-foreground text-[0.82rem] font-semibold uppercase tracking-wide">Admin Access</div>
            <p className="text-muted-foreground text-[0.83rem]">Enter an admin API key to manage providers, keys, and pricing.</p>
            <div className="flex gap-2">
              <Input type="password" placeholder="admin API key" value={adminKeyVal}
                onChange={(e) => setAdminKeyVal(e.target.value)}
                onKeyDown={(e) => { if (e.key === "Enter") saveAdminKey(); }} />
              <Button onClick={saveAdminKey}>Unlock</Button>
            </div>
          </CardContent>
        </Card>
      </div>
    );
  }

  return (
    <div className="px-6 py-5 max-w-[1400px] mx-auto">
      <div className="grid gap-4 grid-cols-1 md:grid-cols-2 xl:grid-cols-3">
        {/* Providers */}
        <Card className="max-h-[33vh]">
          <CardHeader className="flex flex-row items-center justify-between">
            <CardTitle>Providers</CardTitle>
            <Button size="sm" onClick={() => setModal({ type: "provider" })}>Add</Button>
          </CardHeader>
          <CardContent className="flex flex-col gap-2 min-h-0 overflow-y-auto scrollbar-thin">
            {upstreams.length === 0
               ? <p className="text-muted-foreground text-[0.83rem]">No providers configured.</p>
               : upstreams.map((u) => (
                 <div key={u.name} className="border border-border rounded-lg p-3">
                   <div className="flex justify-between items-center">
                     <div className="flex items-center gap-1.5">
                       <strong className="mono text-sm">{u.name}</strong>
                       <Badge variant={u.enabled ? "default" : "secondary"}>{u.enabled ? "on" : "off"}</Badge>
                       {u.max_inflight != null && <span className="text-muted-foreground text-[0.76rem] ml-1">max {u.max_inflight}</span>}
                     </div>
                     <div className="inline-flex border border-border rounded-md overflow-hidden shrink-0">
                       <Button variant="ghost" size="xs" onClick={() => setModal({ type: "provider", edit: u })}>edit</Button>
                       <Button variant="ghost" size="xs" className="text-destructive" onClick={() => deleteProvider(u.name)}>delete</Button>
                     </div>
                   </div>
                   <div className="text-muted-foreground mono text-[0.78rem] mt-1 break-all">{u.base_url}</div>
                 </div>
               ))}
           </CardContent>
        </Card>

        {/* Keys */}
        <Card className="max-h-[33vh]">
          <CardHeader className="flex flex-row items-center justify-between">
            <CardTitle>Proxen User Keys</CardTitle>
            <Button size="sm" onClick={() => setModal({ type: "key" })}>Add</Button>
          </CardHeader>
          <CardContent className="flex flex-col gap-2 min-h-0 overflow-y-auto scrollbar-thin">
            {keys.length === 0
               ? <p className="text-muted-foreground text-[0.83rem]">No proxen keys. Clients will bypass auth (dev mode).</p>
               : keys.map((k) => {
                 const l = k.limits;
                 const winH = (s) => (s ? s / 3600 + "h" : "?");
                 const parts = [];
                 if (l) {
                   if (l.max_inflight != null) parts.push(l.max_inflight + " inflight");
                   if (l.max_requests != null) parts.push(l.max_requests + " req/" + winH(l.max_requests_window_s));
                   if (l.max_tokens != null) parts.push(l.max_tokens + " tok/" + winH(l.max_tokens_window_s));
                 }
                 const limitsStr = parts.length ? parts.join(" · ") : null;
                  return (
                    <div key={k.id} className="border border-border rounded-lg p-3">
                      <div className="flex justify-between items-center gap-2">
                        <div className="flex items-center gap-1.5">
                          <strong className="mono text-sm">{k.label || "(no label)"}</strong>
                          <Badge variant={k.active ? "default" : "secondary"}>{k.active ? "active" : "disabled"}</Badge>
                        </div>
                        <div className="inline-flex border border-border rounded-md overflow-hidden shrink-0">
                          <Button variant="ghost" size="xs" onClick={() => setModal({ type: "keyEdit", key: k })}>edit</Button>
                          <Button variant="ghost" size="xs" onClick={() => toggleKey(k.id, !k.active)}>{k.active ? "disable" : "enable"}</Button>
                          <Button variant="ghost" size="xs" className="text-destructive" onClick={() => deleteKey(k.id)}>delete</Button>
                        </div>
                      </div>
                       <div className="text-muted-foreground text-[0.72rem] mt-1">last used: {fmtAgo(k.last_used_at)} · {limitsStr || "unlimited"}</div>
                    </div>
                 );
               })}
           </CardContent>
        </Card>

        <GatePanel />

        <ModelsTable models={models} onEdit={handleEditModel} onAdd={handleAddModel} onImport={handleImportModels} onBulkDelete={handleBulkDelete} />
      </div>

      {modal?.type === "provider" && <ProviderModal edit={modal.edit} onSave={saveProvider} onClose={closeModal} />}
      {modal?.type === "key" && <KeyModal onSave={saveKey} onClose={closeModal} />}
      {modal?.type === "keyEdit" && <KeyEditModal keyData={modal.key} onSave={saveKeyEdit} onClear={clearKeyLimits} onClose={closeModal} />}
      {modal?.type === "model" && <ModelModal edit={modal.edit} allModels={models} upstreams={upstreams} onSave={saveModel} onDelete={deleteModel} onClose={closeModal} />}
      {modal?.type === "import" && <ImportModelsModal upstreams={upstreams} existingModelIds={modelIds} onClose={closeModal} onDone={closeAndRefreshModels} />}
    </div>
  );
}

export const Manage = ManageImpl;

// ─── Modals ──────────────────────────────────────────────────────────

function ProviderModal({ edit, onSave, onClose }) {
  const [name, setName] = useState(edit?.name || "");
  const [baseUrl, setBaseUrl] = useState(edit?.base_url || "https://api.openai.com/v1");
  const [apiKey, setApiKey] = useState(edit?.api_key || "");
  const [apiKeyDirty, setApiKeyDirty] = useState(false);
  const [maxInflight, setMaxInflight] = useState(edit?.max_inflight ?? "");
  const [enabled, setEnabled] = useState(edit?.enabled ?? true);
  const [errors, setErrors] = useState({});

  const save = () => {
    const e = {};
    if (!name) e.name = "Name is required";
    if (!edit && !apiKey) e.apiKey = "API key is required";
    if (edit && apiKeyDirty && !apiKey) e.apiKey = "API key is required";
    if (Object.keys(e).length) { setErrors(e); return; }
    const payload = {
      name, base_url: baseUrl, enabled,
      max_inflight: maxInflight ? parseInt(maxInflight) : null,
    };
    if (!edit || apiKeyDirty) payload.api_key = apiKey;
    onSave(payload, edit?.name || null);
  };

  return (
    <FormDialog title={edit ? "Edit Provider" : "Add Provider"} onClose={onClose}
      footer={<><Button variant="outline" onClick={onClose}>Cancel</Button><Button onClick={save}>Save</Button></>}>
      <UIField>
        <FieldLabel>Name</FieldLabel>
        <Input type="text" placeholder="openai" aria-invalid={!!errors.name} value={name} onChange={(e) => { setName(e.target.value); if (errors.name) setErrors((p) => ({ ...p, name: undefined })); }} />
        {errors.name && <FieldError>{errors.name}</FieldError>}
      </UIField>
      <UIField>
        <FieldLabel>Base URL</FieldLabel>
        <Input type="text" value={baseUrl} onChange={(e) => setBaseUrl(e.target.value)} />
      </UIField>
      <UIField>
        <FieldLabel>API Key</FieldLabel>
        <Input type="password" aria-invalid={!!errors.apiKey} value={apiKey} onChange={(e) => { setApiKey(e.target.value); setApiKeyDirty(true); if (errors.apiKey) setErrors((p) => ({ ...p, apiKey: undefined })); }} />
        {errors.apiKey && <FieldError>{errors.apiKey}</FieldError>}
      </UIField>
      <UIField>
        <FieldLabel>Max Inflight</FieldLabel>
        <Input type="number" placeholder="unlimited" value={maxInflight} onChange={(e) => setMaxInflight(e.target.value)} />
      </UIField>
      <Check checked={enabled} onChange={setEnabled}>enabled</Check>
    </FormDialog>
  );
}

function KeyModal({ onSave, onClose }) {
  const [keyVal, setKeyVal] = useState("");
  const [label, setLabel] = useState("");
  const [limits, setLimits] = useState(parseLimits(null));
  const [createdKey, setCreatedKey] = useState(null);
  const [copied, setCopied] = useState(false);

  const save = async () => {
    const k = keyVal || randomKey();
    try {
      const resp = await onSave(k, label, buildLimits(limits));
      setCreatedKey(resp.key || k);
      toast.success("key added");
    } catch (e) { toast.error(e.message); }
  };

  const copyKey = () => {
    navigator.clipboard.writeText(createdKey).then(() => { setCopied(true); setTimeout(() => setCopied(false), 2000); });
  };

  if (createdKey) {
    return (
      <FormDialog title="Key Created" onClose={onClose}
        footer={<Button onClick={onClose} className="w-full">Done</Button>}>
        <div className="flex gap-2">
          <Input type="text" readOnly value={createdKey} className="mono flex-1" />
          <Button variant="outline" size="icon" onClick={copyKey}>{copied ? <CheckIcon className="size-4" /> : <ClipboardCopyIcon className="size-4" />}</Button>
        </div>
      </FormDialog>
    );
  }

  return (
    <FormDialog title="Add Key" onClose={onClose}
      footer={<><Button variant="outline" onClick={onClose}>Cancel</Button><Button variant="secondary" onClick={() => setKeyVal(randomKey())}>Generate</Button><Button onClick={save}>Add</Button></>}>
      <UIField><FieldLabel>Key</FieldLabel><Input type="text" placeholder="leave blank to generate" value={keyVal} onChange={(e) => setKeyVal(e.target.value)} /></UIField>
      <UIField><FieldLabel>Label</FieldLabel><Input type="text" placeholder="e.g. alice-ci" value={label} onChange={(e) => setLabel(e.target.value)} /></UIField>
      <RateLimitsFields limits={limits} onChange={setLimits} />
    </FormDialog>
  );
}

function KeyEditModal({ keyData, onSave, onClear, onClose }) {
  const [label, setLabel] = useState(keyData?.label || "");
  const [limits, setLimits] = useState(parseLimits(keyData?.limits));
  const [copied, setCopied] = useState(false);

  const copyKey = () => {
    navigator.clipboard.writeText(keyData.key).then(() => { setCopied(true); setTimeout(() => setCopied(false), 2000); });
  };

  const save = () => {
    onSave(keyData.id, label, buildLimits(limits));
  };

  return (
    <FormDialog title="Edit Key" onClose={onClose}
      footer={<><Button variant="outline" onClick={() => onClear(keyData.id)}>Clear All</Button><Button variant="outline" onClick={onClose}>Cancel</Button><Button onClick={save}>Save</Button></>}>
      <UIField>
        <FieldLabel>Key</FieldLabel>
        <div className="flex gap-2">
          <Input type="text" readOnly value={keyData.key} className="mono flex-1" />
          <Button variant="outline" size="icon" onClick={copyKey}>{copied ? <CheckIcon className="size-4" /> : <ClipboardCopyIcon className="size-4" />}</Button>
        </div>
      </UIField>
      <UIField><FieldLabel>Label</FieldLabel><Input type="text" value={label} onChange={(e) => setLabel(e.target.value)} /></UIField>
      <RateLimitsFields limits={limits} onChange={setLimits} />
    </FormDialog>
  );
}

// ─── Model Modal (Add / Edit) ───────────────────────────────────────

function ModelModal({ edit, allModels, upstreams, onSave, onDelete, onClose }) {
  const isEdit = !!edit;
  const [id, setId] = useState(edit?.id || "");
  const [enabled, setEnabled] = useState(edit?.enabled ?? true);
  const [inputPer, setInputPer] = useState(edit?.input_per_1m ?? 0);
  const [cachedPer, setCachedPer] = useState(edit?.cached_input_per_1m ?? 0);
  const [outputPer, setOutputPer] = useState(edit?.output_per_1m ?? 0);
  const [maxIn, setMaxIn] = useState(edit?.max_input_tokens ?? "");
  const [maxOut, setMaxOut] = useState(edit?.max_output_tokens ?? "");
  const [extraBody, setExtraBody] = useState(edit?.extra_body ? JSON.stringify(edit.extra_body, null, 2) : "");
  const [errors, setErrors] = useState({});
  const [routes, setRoutes] = useState(
    edit?.routes?.length
      ? edit.routes.map((r) => ({ upstream_name: r.upstream_name, upstream_model_id: r.upstream_model_id, rid: crypto.randomUUID(), enabled: r.enabled !== false }))
      : [{ upstream_name: "", upstream_model_id: "", rid: crypto.randomUUID(), enabled: true }]
  );
  const [availableModels, setAvailableModels] = useState({});
  const [fetchingProvider, setFetchingProvider] = useState(null);
  const [dragIndex, setDragIndex] = useState(null);
  const containerRef = useRef(null);
  const positionsRef = useRef({});

  const fetchAvailable = async (upstreamName) => {
    if (availableModels[upstreamName]) return;
    try {
      const models = await getAvailableModels(upstreamName);
      setAvailableModels((a) => ({ ...a, [upstreamName]: models }));
    } catch { /* ignore */ }
  };

  const fetchFromProvider = async (upstreamName) => {
    if (!upstreamName || fetchingProvider) return;
    setFetchingProvider(upstreamName);
    try {
      const models = await fetchProviderModels(upstreamName);
      setAvailableModels((a) => ({ ...a, [upstreamName]: models }));
      toast.success(`Fetched ${models.length} model(s)`);
    } catch (e) { toast.error(e.message); }
    setFetchingProvider(null);
  };

  useEffect(() => {
    for (const r of routes) {
      if (r.upstream_name) fetchAvailable(r.upstream_name);
    }
  }, []);

  const capturePositions = () => {
    const container = containerRef.current;
    if (!container) return;
    const store = {};
    container.querySelectorAll("[data-route-key]").forEach((el) => {
      store[el.dataset.routeKey] = el.getBoundingClientRect().top;
    });
    positionsRef.current = store;
  };

  useLayoutEffect(() => {
    const container = containerRef.current;
    if (!container) return;
    const cards = container.querySelectorAll("[data-route-key]");
    cards.forEach((el) => {
      const first = positionsRef.current[el.dataset.routeKey];
      if (first == null) return;
      const last = el.getBoundingClientRect().top;
      const delta = first - last;
      if (!delta) return;
      el.style.transition = "none";
      el.style.transform = `translateY(${delta}px)`;
    });
    void container.offsetHeight;
    cards.forEach((el) => {
      el.style.transition = "";
      el.style.transform = "";
    });
    positionsRef.current = {};
  }, [routes]);

  const onProviderChange = (index, providerName) => {
    const newRoutes = [...routes];
    newRoutes[index] = { upstream_name: providerName, upstream_model_id: "" };
    setRoutes(newRoutes);
    fetchAvailable(providerName);
  };

  const onModelChange = (index, modelId) => {
    const newRoutes = [...routes];
    newRoutes[index] = { ...newRoutes[index], upstream_model_id: modelId };
    setRoutes(newRoutes);
  };

  const addFallback = () => {
    setRoutes([...routes, { upstream_name: "", upstream_model_id: "", rid: crypto.randomUUID(), enabled: true }]);
  };

  const removeRoute = (index) => {
    setRoutes(routes.filter((_, i) => i !== index));
  };

  const toggleRoute = (index) => {
    setRoutes((prev) => prev.map((r, i) => (i === index ? { ...r, enabled: !r.enabled } : r)));
  };

  const handleDragStart = (e, i) => {
    e.dataTransfer.effectAllowed = "move";
    e.dataTransfer.setData("text/plain", "");
    setDragIndex(i);
  };
  const handleDragOver = (e, i) => {
    e.preventDefault();
    e.dataTransfer.dropEffect = "move";
    if (dragIndex === null || dragIndex === i) return;
    capturePositions();
    setRoutes((prev) => {
      const next = [...prev];
      const [moved] = next.splice(dragIndex, 1);
      next.splice(i, 0, moved);
      return next;
    });
    setDragIndex(i);
  };
  const handleDragEnd = () => setDragIndex(null);

  const save = () => {
    const e = {};
    if (!id) e.id = "Model ID is required";
    if (!routes.some((r) => r.upstream_name && r.upstream_model_id)) e.routes = "At least one route is required";
    let extraBodyParsed = null;
    if (extraBody.trim()) {
      try { extraBodyParsed = JSON.parse(extraBody); } catch { e.extraBody = "Invalid JSON"; }
    }
    if (Object.keys(e).length) { setErrors(e); return; }
    const body = buildModelBody({
      id,
      enabled,
      inputPer,
      cachedPer,
      outputPer,
      maxIn,
      maxOut,
      extraBody: extraBodyParsed,
      routes: routes.filter((r) => r.upstream_name && r.upstream_model_id),
    });
    onSave(edit?.id || id, body, isEdit);
  };

  const onCopyFrom = (target) => {
    const src = allModels.find((x) => x.id === target);
    if (src) {
      setInputPer(src.input_per_1m ?? 0);
      setCachedPer(src.cached_input_per_1m ?? 0);
      setOutputPer(src.output_per_1m ?? 0);
      setMaxIn(src.max_input_tokens ?? "");
      setMaxOut(src.max_output_tokens ?? "");
      setExtraBody(src.extra_body ? JSON.stringify(src.extra_body, null, 2) : "");
    }
  };

  const otherModels = allModels.filter((x) => x.id !== id);

  return (
    <FormDialog title={isEdit ? "Edit Model" : "Add Model"} onClose={onClose} className="sm:max-w-lg"
      footer={
        <div className="flex w-full justify-between">
          {isEdit ? <Button variant="destructive" onClick={() => onDelete(edit.id)}>Delete</Button> : <div />}
          <div className="flex gap-2">
            <Button variant="outline" onClick={onClose}>Cancel</Button>
            <Button onClick={save}>Save</Button>
          </div>
        </div>
      }>
      <UIField>
        <FieldLabel>Proxen Model ID</FieldLabel>
        <Input type="text" placeholder="e.g. glm-5.1" aria-invalid={!!errors.id} value={id} onChange={(e) => { setId(e.target.value); if (errors.id) setErrors((p) => ({ ...p, id: undefined })); }} />
        {errors.id && <FieldError>{errors.id}</FieldError>}
      </UIField>

      <div className="text-muted-foreground text-[0.82rem] font-semibold uppercase tracking-wide mt-3">Upstream Routes</div>
      <div ref={containerRef}>
        {routes.map((r, i) => (
          <RouteEditor
            key={r.rid}
            route={r}
            index={i}
            upstreams={upstreams}
            availableModels={availableModels}
            onProviderChange={onProviderChange}
            onModelChange={onModelChange}
            onRemove={() => removeRoute(i)}
            onToggle={() => toggleRoute(i)}
            onFetch={fetchFromProvider}
            fetching={fetchingProvider === r.upstream_name}
            dragIndex={dragIndex}
            onDragStart={(e) => handleDragStart(e, i)}
            onDragOver={(e) => handleDragOver(e, i)}
            onDragEnd={handleDragEnd}
          />
        ))}
      </div>
      <Button variant="outline" size="sm" onClick={addFallback} className="w-full">+ Add Fallback</Button>
      {errors.routes && <FieldError>{errors.routes}</FieldError>}

      <div className="flex items-center gap-0 mt-3">
        <span className="text-muted-foreground text-[0.82rem] font-semibold uppercase tracking-wide">Settings</span>
        {otherModels.length > 0 && (
          <DropdownMenu>
            <DropdownMenuTrigger asChild>
              <Button variant="ghost" size="xs" className="text-muted-foreground hover:text-foreground" title="Fill from another model">
                <Wand2 className="size-3.5" />
              </Button>
            </DropdownMenuTrigger>
            <DropdownMenuContent align="start">
              <DropdownMenuLabel>Copy settings from:</DropdownMenuLabel>
              {otherModels.map((x) => (
                <DropdownMenuItem key={x.id} onClick={() => onCopyFrom(x.id)}>{x.id}</DropdownMenuItem>
              ))}
            </DropdownMenuContent>
          </DropdownMenu>
        )}
      </div>
      <div className="grid grid-cols-3 gap-2.5">
        <UIField><FieldLabel>Input / 1M</FieldLabel><Input type="number" step="0.01" value={inputPer} onChange={(e) => setInputPer(e.target.value)} /></UIField>
        <UIField><FieldLabel>Cached / 1M</FieldLabel><Input type="number" step="0.01" value={cachedPer} onChange={(e) => setCachedPer(e.target.value)} /></UIField>
        <UIField><FieldLabel>Output / 1M</FieldLabel><Input type="number" step="0.01" value={outputPer} onChange={(e) => setOutputPer(e.target.value)} /></UIField>
      </div>
      <div className="grid grid-cols-2 gap-2.5">
        <UIField><FieldLabel>Max Input Tokens</FieldLabel><Input type="number" placeholder="—" value={maxIn} onChange={(e) => setMaxIn(e.target.value)} /></UIField>
        <UIField><FieldLabel>Max Output Tokens</FieldLabel><Input type="number" placeholder="—" value={maxOut} onChange={(e) => setMaxOut(e.target.value)} /></UIField>
      </div>
      <UIField>
        <FieldLabel>Extra Body</FieldLabel>
        <textarea
          className="flex min-h-[72px] w-full resize-y rounded-lg border border-input bg-transparent px-2.5 py-1 text-sm font-mono outline-none transition-colors placeholder:text-muted-foreground focus-visible:border-ring focus-visible:ring-3 focus-visible:ring-ring/50 aria-invalid:border-destructive aria-invalid:ring-3 aria-invalid:ring-destructive/20 dark:bg-input/30"
          placeholder='{"reasoning_effort": "high"}'
          aria-invalid={!!errors.extraBody}
          value={extraBody}
          onChange={(e) => { setExtraBody(e.target.value); if (errors.extraBody) setErrors((p) => ({ ...p, extraBody: undefined })); }}
        />
        {errors.extraBody && <FieldError>{errors.extraBody}</FieldError>}
      </UIField>
      <Check checked={enabled} onChange={setEnabled}>enabled</Check>
    </FormDialog>
  );
}

// ─── Import Models Modal ─────────────────────────────────────────────

function ImportModelsModal({ upstreams, existingModelIds, onClose, onDone }) {
  const [activeTab, setActiveTab] = useState(upstreams[0]?.name || "");
  const [cache, setCache] = useState({});
  const [fetching, setFetching] = useState(false);
  const [checked, setChecked] = useState(new Set());
  const [proxenIds, setProxenIds] = useState({});
  const [importing, setImporting] = useState(false);

  const applyModels = (name, models) => {
    setCache((c) => ({ ...c, [name]: models }));
  };

  const loadFromCache = async (name) => {
    if (cache[name]) return;
    try {
      const models = await getAvailableModels(name);
      applyModels(name, models);
    } catch { /* no cache yet */ }
  };

  const fetchProvider = async (name) => {
    setFetching(true);
    try {
      const models = await fetchProviderModels(name);
      applyModels(name, models);
    } catch (e) { toast.error(e.message); }
    setFetching(false);
  };

  useEffect(() => {
    for (const u of upstreams) loadFromCache(u.name);
  }, []);

  const models = cache[activeTab] || [];
  const getProxenId = (upstreamId) => proxenIds[upstreamId] || upstreamId;
  const isExisting = (upstreamId) => existingModelIds.includes(getProxenId(upstreamId));

  const toggle = (upstreamId) => {
    if (isExisting(upstreamId)) return;
    setChecked((prev) => {
      const next = new Set(prev);
      if (next.has(upstreamId)) next.delete(upstreamId);
      else next.add(upstreamId);
      return next;
    });
  };

  const importable = models.filter((m) => !isExisting(m.id));
  const allChecked = importable.length > 0 && importable.every((m) => checked.has(m.id));
  const toggleAll = () => {
    setChecked((prev) => {
      const next = new Set(prev);
      importable.forEach((m) => (allChecked ? next.delete(m.id) : next.add(m.id)));
      return next;
    });
  };

  const updateProxenId = (upstreamId, value) => {
    setProxenIds((prev) => ({ ...prev, [upstreamId]: value }));
    if (existingModelIds.includes(value || upstreamId)) {
      setChecked((prev) => {
        const next = new Set(prev);
        next.delete(upstreamId);
        return next;
      });
    }
  };

  const handleImport = async () => {
    if (checked.size === 0) return;
    await doImport([...checked]);
  };

  const doImport = async (modelIds) => {
    setImporting(true);
    const importModels = [];
    const overrides = {};
    for (const upstreamId of modelIds) {
      if (isExisting(upstreamId)) continue;
      importModels.push(upstreamId);
      const proxenId = getProxenId(upstreamId);
      if (proxenId !== upstreamId) overrides[upstreamId] = proxenId;
    }
    if (importModels.length === 0) {
      setImporting(false);
      return;
    }
    try {
      const resp = await api("POST", `/api/management/upstreams/${encodeURIComponent(activeTab)}/import-models`, { models: importModels, overrides, overwrite: [] });
      toast.success(`Imported ${resp.imported.length} model(s)`);
      onDone();
    } catch (e) {
      toast.error(e.message);
    }
    setImporting(false);
  };

  // ── list view ──
  return (
    <FormDialog title="Import Models" onClose={onClose} className="max-w-2xl"
      footer={<><Button variant="outline" onClick={onClose}>Close</Button><Button onClick={handleImport} disabled={checked.size === 0 || importing}>{importing ? "Importing..." : `Import Selected (${checked.size})`}</Button></>}>
      <div className="flex gap-1 border-b border-border pb-2 mb-2 overflow-x-auto">
        {upstreams.map((u) => (
          <button
            key={u.name}
            onClick={() => setActiveTab(u.name)}
            className={`px-3 py-1.5 rounded-md text-[0.83rem] font-medium whitespace-nowrap transition-colors ${
              activeTab === u.name ? "bg-primary text-primary-foreground" : "text-muted-foreground hover:bg-muted"
            }`}
          >
            {u.name}
          </button>
        ))}
      </div>

      <div className="flex items-center justify-between mb-2">
        <span className="text-muted-foreground text-[0.76rem]">
          {models.length} models available
        </span>
        <div className="flex gap-1.5">
          <Button variant="outline" size="xs" onClick={toggleAll} disabled={models.length === 0}>
            {allChecked ? "Deselect All" : "Select All"}
          </Button>
          <Button variant="outline" size="xs" onClick={() => fetchProvider(activeTab)} disabled={fetching}>
            {fetching ? "Fetching..." : "Fetch"}
          </Button>
        </div>
      </div>

      <div className="h-[400px] overflow-auto space-y-1.5">
        {models.length === 0
          ? <p className="text-muted-foreground text-center py-5 text-[0.83rem]">No models. Click Fetch to sync from provider.</p>
          : models.map((m) => {
            const upstreamId = m.id;
            const proxenId = getProxenId(upstreamId);
            const exists = isExisting(upstreamId);
            const isChecked = checked.has(upstreamId);

            return (
              <div key={upstreamId} className={`border rounded-lg p-2.5 transition-colors ${isChecked ? "border-primary bg-primary/5" : "border-border"}`}>
                <div className="flex items-start gap-2.5">
                  {(() => {
                    const checkboxBtn = (
                      <button
                        className={`mt-0.5 flex items-center justify-center w-5 h-5 rounded border transition-all shrink-0 ${
                          isChecked ? "bg-primary border-primary text-primary-foreground"
                          : exists ? "border-muted-foreground/20 opacity-40 cursor-not-allowed"
                          : "border-muted-foreground/30 hover:border-muted-foreground"
                        }`}
                        onClick={() => toggle(upstreamId)}
                        disabled={exists}
                      >
                        {isChecked && (
                          <svg width="12" height="12" viewBox="0 0 16 16" fill="none">
                            <path d="M3.5 8L7 11.5L12.5 5.5" stroke="currentColor" strokeWidth="2" strokeLinecap="round" strokeLinejoin="round" />
                          </svg>
                        )}
                      </button>
                    );
                    if (!exists) return checkboxBtn;
                    return (
                      <TooltipProvider delayDuration={300}>
                        <Tooltip>
                          <TooltipTrigger asChild>
                            <span className="inline-flex">{checkboxBtn}</span>
                          </TooltipTrigger>
                          <TooltipContent side="top" collisionPadding={8}>
                            This model ID already exists. Rename the Proxen ID to import it.
                          </TooltipContent>
                        </Tooltip>
                      </TooltipProvider>
                    );
                  })()}
                  <div className="flex-1 min-w-0">
                    <div className="flex items-center gap-2 mb-1">
                      <span className="mono text-[0.83rem] font-medium">{upstreamId}</span>
                      {exists && <Badge className="text-[0.65rem] bg-warning text-warning-foreground">exists</Badge>}
                    </div>
                    <div className="flex items-center gap-2">
                      <span className="text-muted-foreground text-[0.7rem]">Proxen ID:</span>
                      <Input
                        type="text"
                        value={proxenId}
                        onChange={(e) => updateProxenId(upstreamId, e.target.value)}
                        className="h-6 text-[0.78rem] py-0 max-w-[200px]"
                      />
                    </div>
                  </div>
                </div>
              </div>
            );
          })}
      </div>
    </FormDialog>
  );
}
