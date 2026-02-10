import { useMemo, useState, useEffect } from 'react'
import type {
  AggregationOutputOptions,
  AggregationRunResult,
  UpdaterStatus,
  ValidationResult
} from './types'

const DELIMITER_NAME: Record<string, string> = {
  ',': '逗号(,)',
  ';': '分号(;)',
  '\t': '制表符(\t)',
  '|': '竖线(|)'
}

const StatusTag = ({ ok, text }: { ok: boolean; text: string }): JSX.Element => {
  return <span className={`tag ${ok ? 'ok' : 'error'}`}>{text}</span>
}

const hasElectronAPI = (): boolean => {
  return typeof window !== 'undefined' && typeof window.electronAPI !== 'undefined'
}

const ResultPreviewTable = ({ rows }: { rows: string[][] }): JSX.Element | null => {
  if (!rows.length) {
    return null
  }

  const header = rows[0] ?? []
  const body = rows.slice(1)

  return (
    <div className="table-wrap result-wrap">
      <table className="auto-fit-table">
        <thead>
          <tr>
            {header.map((cell, index) => (
              <th key={`result-head-${index}`}>{cell}</th>
            ))}
          </tr>
        </thead>
        <tbody>
          {body.map((row, rowIndex) => (
            <tr key={`result-row-${rowIndex}`}>
              {header.map((_, colIndex) => (
                <td key={`result-cell-${rowIndex}-${colIndex}`}>{row[colIndex] ?? ''}</td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}

export function App(): JSX.Element {
  const [filePath, setFilePath] = useState<string>('')
  const [validation, setValidation] = useState<ValidationResult | null>(null)
  const [aggregationResult, setAggregationResult] = useState<AggregationRunResult | null>(null)
  const [busy, setBusy] = useState(false)
  const [errorText, setErrorText] = useState<string>('')
  const [includeSheetStyleRows, setIncludeSheetStyleRows] = useState(true)
  const [updaterStatus, setUpdaterStatus] = useState<UpdaterStatus>({
    stage: 'idle',
    message: '等待更新状态...'
  })

  useEffect(() => {
    if (!hasElectronAPI()) {
      setErrorText('未检测到桌面桥接API，请确认通过 Electron 启动应用。')
      return
    }

    const off = window.electronAPI.updater.onStatus((status) => {
      setUpdaterStatus(status)
    })
    return () => {
      off()
    }
  }, [])

  const canRun = Boolean(filePath && validation?.requiredColumnsFound && !busy)

  const header = validation?.previewRows[0] ?? []
  const bodyRows = (validation?.previewRows ?? []).slice(1)

  const missingText = useMemo(() => {
    if (!validation || validation.missingColumns.length === 0) {
      return '无'
    }
    return validation.missingColumns.join('、')
  }, [validation])

  const onPickFile = async (): Promise<void> => {
    if (!hasElectronAPI()) {
      setErrorText('桌面桥接API不可用，无法选择文件。')
      return
    }

    setErrorText('')
    setAggregationResult(null)
    const picked = await window.electronAPI.pickCsv()
    if (picked.canceled || !picked.filePath) {
      return
    }

    setBusy(true)
    setFilePath(picked.filePath)

    try {
      const preview = await window.electronAPI.previewAndValidate(picked.filePath)
      setValidation(preview)
    } catch (error) {
      setValidation(null)
      setErrorText(error instanceof Error ? error.message : '预览失败')
    } finally {
      setBusy(false)
    }
  }

  const onRunAggregation = async (): Promise<void> => {
    if (!hasElectronAPI()) {
      setErrorText('桌面桥接API不可用，无法执行统计。')
      return
    }

    if (!filePath || !canRun) {
      return
    }

    setBusy(true)
    setErrorText('')
    setAggregationResult(null)

    try {
      const options: AggregationOutputOptions = includeSheetStyleRows
        ? {
            includeDailySubtotalRows: true,
            includeGrandTotalRow: true,
            blankDateForDetailRowsWhenSubtotalEnabled: true
          }
        : {
            includeDailySubtotalRows: false,
            includeGrandTotalRow: false
          }

      const result = await window.electronAPI.runAggregation(filePath, options)
      setAggregationResult(result)
      if (!result.success) {
        setErrorText(result.message ?? '计算失败')
      }
    } catch (error) {
      setErrorText(error instanceof Error ? error.message : '计算失败')
    } finally {
      setBusy(false)
    }
  }

  const onOpenOutputDir = async (): Promise<void> => {
    if (!hasElectronAPI()) {
      setErrorText('桌面桥接API不可用，无法打开目录。')
      return
    }

    const outputPath = aggregationResult?.outputPath
    if (!outputPath) {
      return
    }
    await window.electronAPI.openDir(outputPath)
  }

  const onCheckUpdate = async (): Promise<void> => {
    if (!hasElectronAPI()) {
      setErrorText('桌面桥接API不可用，无法检查更新。')
      return
    }

    const result = await window.electronAPI.updater.checkNow()
    if (!result.success && result.message) {
      setErrorText(result.message)
    }
  }

  const onDownloadUpdate = async (): Promise<void> => {
    if (!hasElectronAPI()) {
      setErrorText('桌面桥接API不可用，无法下载更新。')
      return
    }

    const result = await window.electronAPI.updater.downloadNow()
    if (!result.success && result.message) {
      setErrorText(result.message)
    }
  }

  const onInstallUpdate = async (): Promise<void> => {
    if (!hasElectronAPI()) {
      setErrorText('桌面桥接API不可用，无法安装更新。')
      return
    }

    const result = await window.electronAPI.updater.quitAndInstall()
    if (!result.success && result.message) {
      setErrorText(result.message)
    }
  }

  return (
    <div className="app">
      <header className="hero">
        <h1>5G CSV 日汇总分析器</h1>
        <p>选择文件后自动校验并生成统计文件，支持结果预览与目录打开。</p>
      </header>

      <section className="card">
        <div className="row between">
          <div>
            <h2>1) 文件选择</h2>
            <p className="hint">支持 CSV/XLSX，自动识别编码与分隔符。</p>
          </div>
          <button className="btn primary" onClick={onPickFile} disabled={busy}>
            {busy ? '处理中...' : '选择文件'}
          </button>
        </div>

        <div className="grid two">
          <div className="field">
            <label>文件路径</label>
            <div className="value mono">{filePath || '未选择文件'}</div>
          </div>
          <div className="field">
            <label>检测信息</label>
            <div className="value">
              编码: {validation?.encoding ?? '-'} / 分隔符:{' '}
              {validation ? DELIMITER_NAME[validation.delimiter] ?? validation.delimiter : '-'}
            </div>
          </div>
        </div>
      </section>

      <section className="card">
        <h2>2) 标题校验</h2>
        <div className="row">
          <StatusTag
            ok={Boolean(validation?.requiredColumnsFound)}
            text={
              validation
                ? validation.requiredColumnsFound
                  ? '14个关键列已齐全'
                  : '关键列缺失，已停止计算'
                : '等待校验'
            }
          />
        </div>
        <p className="hint">缺失列: {missingText}</p>
      </section>

      <section className="card">
        <div className="row between">
          <div>
            <h2>3) 源文件预览</h2>
            <p className="hint">首行作为标题，以下展示前100行数据。</p>
            <label className="hint checkbox-line">
              <input
                type="checkbox"
                checked={includeSheetStyleRows}
                onChange={(event) => setIncludeSheetStyleRows(event.target.checked)}
                disabled={busy}
              />
              生成 Sheet1 样式（按天小计 + 明细 + 总计）
            </label>
          </div>
          <button className="btn success" onClick={onRunAggregation} disabled={!canRun}>
            {busy ? '计算中...' : '开始统计并生成文件'}
          </button>
        </div>

        <div className="table-wrap">
          <table className="auto-fit-table">
            <thead>
              <tr>
                {header.map((cell, index) => (
                  <th key={`${cell}-${index}`}>{cell}</th>
                ))}
              </tr>
            </thead>
            <tbody>
              {bodyRows.slice(0, 100).map((row, rowIndex) => (
                <tr key={`row-${rowIndex}`}>
                  {header.map((_, colIndex) => (
                    <td key={`cell-${rowIndex}-${colIndex}`}>{row[colIndex] ?? ''}</td>
                  ))}
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      </section>

      <section className="card">
        <h2>4) 执行结果</h2>
        {errorText ? <p className="error-text">{errorText}</p> : <p className="hint">暂无错误</p>}

        <div className="grid two">
          <div className="field">
            <label>处理行数</label>
            <div className="value">{aggregationResult?.processedRows ?? '-'}</div>
          </div>
          <div className="field">
            <label>跳过行数</label>
            <div className="value">{aggregationResult?.skippedRows ?? '-'}</div>
          </div>
        </div>

        <div className="grid two">
          <div className="field">
            <label>输出文件名</label>
            <div className="value mono">{aggregationResult?.outputFileName ?? '尚未生成'}</div>
          </div>
          <div className="field">
            <label>输出路径</label>
            <div className="value mono">{aggregationResult?.outputPath ?? '尚未生成'}</div>
          </div>
        </div>

        <div className="row">
          <button
            className="btn"
            onClick={onOpenOutputDir}
            disabled={!aggregationResult?.success || !aggregationResult?.outputPath}
          >
            打开输出目录
          </button>
        </div>

        {aggregationResult?.invalidFieldStats ? (
          <div className="stats">
            {Object.entries(aggregationResult.invalidFieldStats).map(([key, value]) => (
              <div className="stat-item" key={key}>
                <span>{key}</span>
                <strong>{value}</strong>
              </div>
            ))}
          </div>
        ) : null}

        {aggregationResult?.resultRows?.length ? (
          <div className="result-section">
            <h3>结果预览</h3>
            <p className="hint">展示生成结果前200行，便于核对统计逻辑。</p>
            <ResultPreviewTable rows={aggregationResult.resultRows} />
          </div>
        ) : null}
      </section>

      <section className="card">
        <h2>5) 应用更新</h2>
        <p className="hint">{updaterStatus.message}</p>
        <div className="row">
          <button className="btn" onClick={onCheckUpdate}>
            检查更新
          </button>
          <button className="btn" onClick={onDownloadUpdate} disabled={updaterStatus.stage !== 'available'}>
            下载更新
          </button>
          <button className="btn danger" onClick={onInstallUpdate} disabled={updaterStatus.stage !== 'downloaded'}>
            重启并安装
          </button>
        </div>
      </section>
    </div>
  )
}

