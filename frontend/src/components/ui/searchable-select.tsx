"use client"

import * as React from "react"
import { Popover as PopoverPrimitive } from "radix-ui"
import { CheckIcon, ChevronDownIcon } from "lucide-react"
import { RemoveScroll } from "react-remove-scroll"

import { cn } from "@/lib/utils"
import { Input } from "./input"

type Option = { value: string; label: React.ReactNode }

interface SearchableSelectProps {
  value?: string
  onValueChange: (value: string) => void
  options: Option[]
  placeholder?: string
  disabled?: boolean
  emptyText?: string
  className?: string
}

export function SearchableSelect({
  value,
  onValueChange,
  options,
  placeholder,
  disabled,
  emptyText = "No results",
  className,
}: SearchableSelectProps) {
  const [open, setOpen] = React.useState(false)
  const [query, setQuery] = React.useState("")
  const selected = options.find((o) => o.value === value)
  const q = query.toLowerCase()
  const filtered = q ? options.filter((o) => String(o.label).toLowerCase().includes(q)) : options

  return (
    <PopoverPrimitive.Root
      open={open}
      onOpenChange={(o) => {
        setOpen(o)
        if (!o) setQuery("")
      }}
    >
      <PopoverPrimitive.Trigger asChild>
        <button
          type="button"
          disabled={disabled}
          className={cn(
            "flex w-full items-center justify-between gap-1.5 rounded-lg border border-input bg-transparent h-8 px-2.5 text-sm whitespace-nowrap transition-colors outline-none select-none focus-visible:border-ring focus-visible:ring-3 focus-visible:ring-ring/50 disabled:cursor-not-allowed disabled:opacity-50 data-placeholder:text-muted-foreground dark:bg-input/30",
            className
          )}
        >
          <span className={cn("truncate", !selected && "text-muted-foreground")}>
            {selected ? selected.label : placeholder}
          </span>
          <ChevronDownIcon className="pointer-events-none size-4 text-muted-foreground" />
        </button>
      </PopoverPrimitive.Trigger>
      <PopoverPrimitive.Portal>
        {/* RemoveScroll with noIsolation creates a new scroll lock (last in the
            stack) whose shouldPrevent processes wheel events for this Popover.
            onWheelCapture pushes to shouldPreventQueue, then shouldPrevent calls
            shouldCancelEvent which traverses from the event target up to the lock
            container — allowing the list to scroll when it can, and blocking scroll
            chaining when it can't. noIsolation prevents blocking events elsewhere. */}
        <RemoveScroll noIsolation removeScrollBar={false}>
          <PopoverPrimitive.Content
            align="start"
            sideOffset={4}
            style={{ minWidth: "var(--radix-popper-anchor-width)" }}
            className="z-50 min-w-36 max-w-80 overflow-hidden rounded-lg bg-popover p-1.5 text-popover-foreground shadow-md ring-1 ring-foreground/30 data-[state=open]:animate-in data-[state=open]:fade-in-0 data-[state=open]:zoom-in-95"
          >
            <Input
              autoFocus
              placeholder="Search…"
              value={query}
              onChange={(e) => setQuery(e.target.value)}
              className="h-7 mb-1 text-[0.82rem]"
            />
            <div className="max-h-60 overflow-y-auto">
              {filtered.length === 0 ? (
                <div className="px-1.5 py-1.5 text-[0.75rem] italic text-muted-foreground/80">{emptyText}</div>
              ) : (
                filtered.map((o) => (
                  <button
                    type="button"
                    key={o.value}
                    onClick={() => {
                      onValueChange(o.value)
                      setOpen(false)
                    }}
                    className="relative flex w-full cursor-default items-center gap-1.5 overflow-hidden rounded-md py-1 pr-8 pl-1.5 text-sm outline-none select-none hover:bg-accent hover:text-accent-foreground text-left"
                  >
                    {o.value === value && (
                      <span className="pointer-events-none absolute right-2 flex size-4 items-center justify-center">
                        <CheckIcon className="size-3.5" />
                      </span>
                    )}
                    <span className="min-w-0 truncate">{o.label}</span>
                  </button>
                ))
              )}
            </div>
          </PopoverPrimitive.Content>
        </RemoveScroll>
      </PopoverPrimitive.Portal>
    </PopoverPrimitive.Root>
  )
}
