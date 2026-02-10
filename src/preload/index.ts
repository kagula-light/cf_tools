import { contextBridge, ipcRenderer } from 'electron'
import type {
  AggregationOutputOptions,
  AggregationRunResult,
  ValidationResult
} from '../main/services/csv-service'

type UpdaterStatus =
  | { stage: 'idle'; message: string }
  | { stage: 'checking'; message: string }
  | { stage: 'available'; message: string; version: string }
  | { stage: 'not-available'; message: string }
  | { stage: 'downloading'; message: string; percent: number }
  | { stage: 'downloaded'; message: string; version: string }
  | { stage: 'error'; message: string }

const api = {
  pickCsv: async (): Promise<{ canceled: boolean; filePath?: string }> => {
    return await ipcRenderer.invoke('file:pickCsv')
  },
  previewAndValidate: async (filePath: string): Promise<ValidationResult> => {
    return await ipcRenderer.invoke('csv:previewAndValidate', filePath)
  },
  runAggregation: async (
    filePath: string,
    options?: AggregationOutputOptions
  ): Promise<AggregationRunResult> => {
    return await ipcRenderer.invoke('csv:runAggregation', { filePath, options })
  },
  openDir: async (targetPath: string): Promise<{ success: boolean; message?: string }> => {
    return await ipcRenderer.invoke('file:openDir', targetPath)
  },
  updater: {
    checkNow: async (): Promise<{ success: boolean; message?: string }> => {
      return await ipcRenderer.invoke('updater:checkNow')
    },
    downloadNow: async (): Promise<{ success: boolean; message?: string }> => {
      return await ipcRenderer.invoke('updater:downloadNow')
    },
    quitAndInstall: async (): Promise<{ success: boolean; message?: string }> => {
      return await ipcRenderer.invoke('updater:quitAndInstall')
    },
    onStatus: (callback: (status: UpdaterStatus) => void): (() => void) => {
      const listener = (_event: Electron.IpcRendererEvent, status: UpdaterStatus): void => {
        callback(status)
      }
      ipcRenderer.on('updater:status', listener)
      return () => {
        ipcRenderer.removeListener('updater:status', listener)
      }
    }
  }
}

contextBridge.exposeInMainWorld('electronAPI', api)

declare global {
  interface Window {
    electronAPI: typeof api
  }
}
