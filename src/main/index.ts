import electron from 'electron'
import { join } from 'node:path'
import { existsSync } from 'node:fs'
import electronUpdater from 'electron-updater'
import {
  previewAndValidateCsv,
  runCsvAggregation,
  type AggregationOutputOptions,
  type AggregationRunResult,
  type ValidationResult
} from './services/csv-service'

let mainWindow: Electron.BrowserWindow | null = null

const { app, BrowserWindow, dialog, ipcMain, shell } = electron
const { autoUpdater } = electronUpdater

type UpdaterStatus =
  | { stage: 'idle'; message: string }
  | { stage: 'checking'; message: string }
  | { stage: 'available'; message: string; version: string }
  | { stage: 'not-available'; message: string }
  | { stage: 'downloading'; message: string; percent: number }
  | { stage: 'downloaded'; message: string; version: string }
  | { stage: 'error'; message: string }

const sendUpdaterStatus = (status: UpdaterStatus): void => {
  if (mainWindow && !mainWindow.isDestroyed()) {
    mainWindow.webContents.send('updater:status', status)
  }
}

const createWindow = async (): Promise<void> => {
  mainWindow = new BrowserWindow({
    width: 1200,
    height: 820,
    minWidth: 1040,
    minHeight: 720,
    title: '5G CSV Analyzer',
    webPreferences: {
      preload: join(__dirname, '../preload/index.mjs'),
      contextIsolation: true,
      nodeIntegration: false,
      sandbox: false
    }
  })

  mainWindow.on('closed', () => {
    mainWindow = null
  })

  if (process.env.ELECTRON_RENDERER_URL) {
    await mainWindow.loadURL(process.env.ELECTRON_RENDERER_URL)
    mainWindow.webContents.openDevTools({ mode: 'detach' })
  } else {
    await mainWindow.loadFile(join(__dirname, '../../dist/renderer/index.html'))
  }
}

const registerUpdater = (): void => {
  if (process.env.NODE_ENV !== 'production') {
    sendUpdaterStatus({
      stage: 'idle',
      message: '开发环境不检查自动更新（仅生产安装包启用）。'
    })
    return
  }

  autoUpdater.autoDownload = false
  autoUpdater.autoInstallOnAppQuit = true

  autoUpdater.on('checking-for-update', () => {
    sendUpdaterStatus({ stage: 'checking', message: '正在检查更新...' })
  })

  autoUpdater.on('update-available', (info) => {
    sendUpdaterStatus({
      stage: 'available',
      message: `发现新版本 ${info.version}`,
      version: info.version
    })
  })

  autoUpdater.on('update-not-available', () => {
    sendUpdaterStatus({ stage: 'not-available', message: '当前已是最新版本。' })
  })

  autoUpdater.on('download-progress', (progress) => {
    sendUpdaterStatus({
      stage: 'downloading',
      message: `更新下载中 ${progress.percent.toFixed(1)}%`,
      percent: progress.percent
    })
  })

  autoUpdater.on('update-downloaded', (info) => {
    sendUpdaterStatus({
      stage: 'downloaded',
      message: `版本 ${info.version} 下载完成，可重启安装。`,
      version: info.version
    })
  })

  autoUpdater.on('error', (error) => {
    sendUpdaterStatus({
      stage: 'error',
      message: `更新失败: ${error.message}`
    })
  })
}

const registerIpc = (): void => {
  ipcMain.handle('file:pickCsv', async () => {
    const result = await dialog.showOpenDialog({
      title: '选择 CSV 文件',
      properties: ['openFile'],
      filters: [{ name: 'CSV Files', extensions: ['csv'] }]
    })

    if (result.canceled || result.filePaths.length === 0) {
      return { canceled: true as const }
    }

    return {
      canceled: false as const,
      filePath: result.filePaths[0]
    }
  })

  ipcMain.handle('csv:previewAndValidate', async (_, filePath: string): Promise<ValidationResult> => {
    return await previewAndValidateCsv(filePath)
  })

  ipcMain.handle(
    'csv:runAggregation',
    async (
      _,
      payload: { filePath: string; options?: AggregationOutputOptions }
    ): Promise<AggregationRunResult> => {
      return await runCsvAggregation(payload.filePath, payload.options)
    }
  )

  ipcMain.handle('file:openDir', async (_, targetPath: string) => {
    if (!existsSync(targetPath)) {
      return { success: false, message: '目标路径不存在。' }
    }
    await shell.showItemInFolder(targetPath)
    return { success: true }
  })

  ipcMain.handle('updater:checkNow', async () => {
    try {
      await autoUpdater.checkForUpdates()
      return { success: true }
    } catch (error) {
      const message = error instanceof Error ? error.message : '检查更新失败'
      sendUpdaterStatus({ stage: 'error', message })
      return { success: false, message }
    }
  })

  ipcMain.handle('updater:downloadNow', async () => {
    try {
      await autoUpdater.downloadUpdate()
      return { success: true }
    } catch (error) {
      const message = error instanceof Error ? error.message : '下载更新失败'
      sendUpdaterStatus({ stage: 'error', message })
      return { success: false, message }
    }
  })

  ipcMain.handle('updater:quitAndInstall', () => {
    try {
      autoUpdater.quitAndInstall()
      return { success: true }
    } catch (error) {
      const message = error instanceof Error ? error.message : '安装更新失败'
      sendUpdaterStatus({ stage: 'error', message })
      return { success: false, message }
    }
  })
}

app.whenReady().then(async () => {
  registerUpdater()
  registerIpc()
  await createWindow()

  sendUpdaterStatus({ stage: 'idle', message: `当前版本 ${app.getVersion()}` })

  if (process.env.NODE_ENV === 'production') {
    setTimeout(() => {
      void autoUpdater.checkForUpdates().catch((error: unknown) => {
        sendUpdaterStatus({
          stage: 'error',
          message: error instanceof Error ? error.message : '自动检查更新失败'
        })
      })
    }, 2500)
  }

  app.on('activate', () => {
    if (BrowserWindow.getAllWindows().length === 0) {
      void createWindow()
    }
  })
})

app.on('window-all-closed', () => {
  if (process.platform !== 'darwin') {
    app.quit()
  }
})
