export type RequiredColumn =
  | '时间'
  | 'CI'
  | '网络'
  | '小区名称'
  | '5G总流量(GB)'
  | '5G最大用户数'
  | '5G上行PRB利用率(%)'
  | '5G下行PRB利用率(%)'
  | '5G上行体验速率(Mbps)'
  | '5G下行体验速率(Mbps)'
  | '5G无线接通率(%)'
  | '5G无线掉线率(%)'
  | '5G切换成功率(%)'
  | '5G上行平均干扰(dBm)'

export interface ValidationResult {
  encoding: string
  delimiter: string
  previewRows: string[][]
  requiredColumnsFound: boolean
  missingColumns: RequiredColumn[]
}

export interface AggregationRunResult {
  success: boolean
  processedRows: number
  skippedRows: number
  invalidFieldStats: Record<string, number>
  outputPath?: string
  outputFileName?: string
  resultRows?: string[][]
  errorCode?:
    | 'FILE_NOT_FOUND'
    | 'MISSING_COLUMNS'
    | 'PARSE_FAILED'
    | 'WRITE_FAILED'
    | 'UNKNOWN'
  message?: string
  missingColumns?: RequiredColumn[]
}

export interface AggregationOutputOptions {
  includeDailySubtotalRows?: boolean
  includeGrandTotalRow?: boolean
  blankDateForDetailRowsWhenSubtotalEnabled?: boolean
}

export type UpdaterStatus =
  | { stage: 'idle'; message: string }
  | { stage: 'checking'; message: string }
  | { stage: 'available'; message: string; version: string }
  | { stage: 'not-available'; message: string }
  | { stage: 'downloading'; message: string; percent: number }
  | { stage: 'downloaded'; message: string; version: string }
  | { stage: 'error'; message: string }
