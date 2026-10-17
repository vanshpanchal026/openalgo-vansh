import { useState, useEffect, useRef, useCallback } from 'react'
import {
  Dialog,
  DialogContent,
  DialogDescription,
  DialogFooter,
  DialogHeader,
  DialogTitle,
} from '@/components/ui/dialog'
import { Button } from '@/components/ui/button'
import { AlertCircle, AlertTriangle, FileCode, FileUp, Loader2, Sparkles, Upload } from 'lucide-react'
import { convertPineScript, fileNameFor, nameProblem } from '@/lib/trading/openscriptFiles'

export interface ImportPineDialogProps {
  open: boolean
  onOpenChange: (open: boolean) => void
  currentEditorSource?: string
  onImportSuccess: (file: string, source: string) => Promise<void> | void
}

export function ImportPineDialog({
  open,
  onOpenChange,
  currentEditorSource,
  onImportSuccess,
}: ImportPineDialogProps) {
  const [name, setName] = useState('')
  const [nameTouched, setNameTouched] = useState(false)
  const [pineCode, setPineCode] = useState('')
  const [converting, setConverting] = useState(false)
  const [error, setError] = useState<string | null>(null)
  const [findings, setFindings] = useState<Array<{ code: string; message: string; severity?: string }>>([])
  const fileInputRef = useRef<HTMLInputElement>(null)

  // When dialog opens, initialize code
  useEffect(() => {
    if (open) {
      setError(null)
      setFindings([])
      setNameTouched(false)
      // If current editor text looks like Pine Script and user hasn't typed anything yet,
      // pre-fill it for maximum convenience!
      if (
        currentEditorSource &&
        (currentEditorSource.includes('@version') ||
          currentEditorSource.includes('indicator(') ||
          currentEditorSource.includes('strategy('))
      ) {
        setPineCode(currentEditorSource)
        // Try auto-extracting name
        const match = currentEditorSource.match(/(?:indicator|strategy)\s*\(\s*['"]([^'"]+)['"]/i)
        if (match?.[1]) {
          const slug = match[1]
            .trim()
            .toLowerCase()
            .replace(/[^a-z0-9_-]+/g, '-')
            .replace(/^-+|-+$/g, '')
            .slice(0, 48)
          setName(slug)
        }
      } else if (!pineCode) {
        setPineCode('')
        setName('')
      }
    }
  }, [open, currentEditorSource])

  // Auto-detect title from Pine Script when pasted
  const handlePineCodeChange = useCallback(
    (newCode: string) => {
      setPineCode(newCode)
      setError(null)
      if (!nameTouched) {
        const match = newCode.match(/(?:indicator|strategy)\s*\(\s*['"]([^'"]+)['"]/i)
        if (match?.[1]) {
          const slug = match[1]
            .trim()
            .toLowerCase()
            .replace(/[^a-z0-9_-]+/g, '-')
            .replace(/^-+|-+$/g, '')
            .slice(0, 48)
          if (slug) setName(slug)
        }
      }
    },
    [nameTouched]
  )

  const handleFileUpload = (e: React.ChangeEvent<HTMLInputElement>) => {
    const file = e.target.files?.[0]
    if (!file) return
    const reader = new FileReader()
    reader.onload = (event) => {
      const content = event.target?.result
      if (typeof content === 'string') {
        handlePineCodeChange(content)
      }
    }
    reader.readAsText(file)
  }

  const handleConvert = async () => {
    if (!pineCode.trim()) {
      setError('Please paste your Pine Script code.')
      return
    }

    const problem = name ? nameProblem(name) : null
    if (problem) {
      setNameTouched(true)
      setError(problem)
      return
    }

    setConverting(true)
    setError(null)

    try {
      const res = await convertPineScript(pineCode, name || undefined)
      if (!res.source || res.source.trim().length === 0) {
        throw new Error(
          'Failed to convert Pine Script. Please check that the script is valid TradingView Pine Script v5 or v6.'
        )
      }

      setFindings(res.findings)

      const targetStem = name || res.suggestedName || 'imported-pine'
      const finalFileName = fileNameFor(targetStem)

      await onImportSuccess(finalFileName, res.source)
      onOpenChange(false)
    } catch (err) {
      setError(err instanceof Error ? err.message : String(err))
    } finally {
      setConverting(false)
    }
  }

  const nameError = nameTouched ? nameProblem(name) : null

  return (
    <Dialog open={open} onOpenChange={onOpenChange}>
      <DialogContent className="max-w-2xl sm:max-w-2xl">
        <DialogHeader>
          <div className="flex items-center gap-2">
            <FileCode className="h-5 w-5 text-primary" />
            <DialogTitle>Import TradingView Pine Script</DialogTitle>
          </div>
          <DialogDescription>
            Paste your Pine Script (v5 or v6) below. It will automatically be converted to OpenScript and loaded into the editor.
          </DialogDescription>
        </DialogHeader>

        <div className="space-y-4 py-2">
          {/* Script Name */}
          <div className="space-y-1.5">
            <label className="text-xs font-medium text-foreground" htmlFor="pine-import-name">
              Script Name
            </label>
            <input
              id="pine-import-name"
              type="text"
              value={name}
              onChange={(e) => {
                setName(e.target.value)
                setNameTouched(true)
              }}
              placeholder="e.g. volume-tracker"
              className="h-8 w-full rounded-md border border-input bg-background px-2.5 text-xs outline-none placeholder:text-muted-foreground focus-visible:ring-1 focus-visible:ring-ring"
            />
            {nameError && <p className="text-[11px] text-destructive">{nameError}</p>}
          </div>

          {/* Pine Script Code */}
          <div className="space-y-1.5">
            <div className="flex items-center justify-between">
              <label className="text-xs font-medium text-foreground" htmlFor="pine-import-code">
                Pine Script Source Code
              </label>
              <div className="flex items-center gap-2">
                {currentEditorSource && (
                  <button
                    type="button"
                    onClick={() => handlePineCodeChange(currentEditorSource)}
                    className="inline-flex items-center gap-1 text-[11px] text-muted-foreground hover:text-foreground"
                  >
                    <Sparkles className="h-3 w-3" />
                    Load current editor text
                  </button>
                )}
                <button
                  type="button"
                  onClick={() => fileInputRef.current?.click()}
                  className="inline-flex items-center gap-1 text-[11px] text-muted-foreground hover:text-foreground"
                >
                  <Upload className="h-3 w-3" />
                  Upload file (.pine, .txt)
                </button>
                <input
                  ref={fileInputRef}
                  type="file"
                  accept=".pine,.txt,.text"
                  className="hidden"
                  onChange={handleFileUpload}
                />
              </div>
            </div>

            <textarea
              id="pine-import-code"
              rows={11}
              value={pineCode}
              onChange={(e) => handlePineCodeChange(e.target.value)}
              placeholder="//@version=5&#10;indicator('Volume Tracker', overlay=false)&#10;...&#10;plot(volume, 'Volume')"
              className="w-full resize-none rounded-md border border-input bg-background p-2.5 font-mono text-xs leading-relaxed outline-none placeholder:text-muted-foreground focus-visible:ring-1 focus-visible:ring-ring"
              spellCheck={false}
            />
          </div>

          {/* Error Message */}
          {error && (
            <div className="flex items-start gap-2 rounded-md border border-destructive/30 bg-destructive/10 p-2.5 text-xs text-destructive">
              <AlertCircle className="mt-0.5 h-4 w-4 shrink-0" />
              <span>{error}</span>
            </div>
          )}

          {/* Warnings / Findings */}
          {findings.length > 0 && (
            <div className="max-h-28 overflow-y-auto rounded-md border border-amber-500/30 bg-amber-500/10 p-2.5 text-xs text-amber-600 dark:text-amber-400">
              <div className="flex items-center gap-1.5 font-medium">
                <AlertTriangle className="h-3.5 w-3.5 shrink-0" />
                <span>Converter Warnings ({findings.length})</span>
              </div>
              <ul className="mt-1 space-y-1 pl-4 list-disc text-[11px]">
                {findings.map((f, i) => (
                  <li key={i}>
                    <strong>[{f.code}]</strong> {f.message}
                  </li>
                ))}
              </ul>
            </div>
          )}
        </div>

        <DialogFooter className="gap-2 sm:gap-0">
          <Button type="button" variant="outline" size="sm" onClick={() => onOpenChange(false)}>
            Cancel
          </Button>
          <Button
            type="button"
            size="sm"
            onClick={handleConvert}
            disabled={converting || !pineCode.trim()}
            className="gap-1.5"
          >
            {converting ? (
              <>
                <Loader2 className="h-3.5 w-3.5 animate-spin" />
                Converting...
              </>
            ) : (
              <>
                <FileUp className="h-3.5 w-3.5" />
                Convert & Load in Editor
              </>
            )}
          </Button>
        </DialogFooter>
      </DialogContent>
    </Dialog>
  )
}
