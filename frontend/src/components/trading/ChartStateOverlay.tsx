/**
 * The card a pane shows over its chart while the chart is not simply showing
 * bars: dots for a slow load, a no data card, or a failed load with Try again
 * and Dismiss.
 *
 * It never covers the chart with a scrim. A failed load leaves the previous
 * chart where it was, readable and usable around the card, and Dismiss takes
 * the card away. Only the card itself takes the pointer, and the loading
 * dots take nothing.
 */
import { type ChartStateView, chartStateCopy } from '@/lib/trading/chartState'
import { cn } from '@/lib/utils'

interface Props {
  view: ChartStateView | null
  onRetry(): void
  onDismiss(): void
}

const BUTTON =
  'rounded border border-border px-2 py-1 text-xs hover:bg-accent focus-visible:outline-none focus-visible:ring-1 focus-visible:ring-ring'

export function ChartStateOverlay({ view, onRetry, onDismiss }: Props) {
  if (!view || view.kind === 'ready') return null
  const copy = chartStateCopy(view)

  // Loading: subtle bouncing dots in center
  if (view.kind === 'loading') {
    return (
      <div
        className="pointer-events-none absolute inset-0 z-[15] flex items-center justify-center p-3"
        data-chart-state="loading"
      >
        <div
          role="status"
          aria-live="polite"
          className="rounded-lg border border-border bg-popover/90 px-3 py-2 text-center shadow-md backdrop-blur select-none"
        >
          <div className="flex flex-col items-center gap-1.5">
            <span className="flex gap-1" aria-hidden="true">
              {[0, 1, 2].map((i) => (
                <span
                  key={i}
                  className="h-1.5 w-1.5 animate-bounce rounded-full bg-muted-foreground motion-reduce:animate-none"
                  style={{ animationDelay: `${i * 150}ms` }}
                />
              ))}
            </span>
            <span className="text-[11px] text-muted-foreground">{copy.title}</span>
          </div>
        </div>
      </div>
    )
  }

  // Empty / No Data: TradingView-style non-intrusive watermark in center.
  // Completely pointer-events-none so it NEVER captures clicks or blocks keyboard flipping!
  if (view.kind === 'empty') {
    return (
      <div
        className="pointer-events-none absolute inset-0 z-[15] flex items-center justify-center p-3 select-none"
        data-chart-state="empty"
      >
        <div
          role="status"
          aria-live="polite"
          className="max-w-[min(24rem,90%)] rounded-lg border border-border/50 bg-background/85 px-4 py-3 text-center shadow-sm backdrop-blur pointer-events-none"
        >
          <p className="break-words text-sm font-medium text-foreground/90">{copy.title}</p>
          <p className="mt-1 break-words text-xs leading-relaxed text-muted-foreground">
            {copy.text}
          </p>
        </div>
      </div>
    )
  }

  // Error: compact top-centered notification banner (non-blocking, with small retry and close)
  return (
    <div
      className="pointer-events-none absolute top-3 inset-x-0 z-[15] flex justify-center px-4"
      data-chart-state="error"
    >
      <div
        role="alert"
        aria-live="assertive"
        className="pointer-events-auto flex items-center gap-3 rounded-lg border border-border/80 bg-popover/95 px-3 py-2 text-xs shadow-md backdrop-blur max-w-[min(28rem,90%)]"
      >
        <div className="flex-1 min-w-0">
          <p className="font-medium text-foreground truncate">{copy.title}</p>
          <p className="text-[11px] text-muted-foreground truncate">{copy.text}</p>
        </div>
        <button
          type="button"
          className={cn(BUTTON, 'shrink-0')}
          onClick={onRetry}
        >
          Try again
        </button>
        <button
          type="button"
          className="shrink-0 text-muted-foreground hover:text-foreground text-xs px-1"
          onClick={onDismiss}
          aria-label="Dismiss"
        >
          ✕
        </button>
      </div>
    </div>
  )
}
