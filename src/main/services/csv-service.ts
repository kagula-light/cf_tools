import { createReadStream, existsSync } from 'node:fs'
import { createWriteStream } from 'node:fs'
import { dirname, extname, basename, join } from 'node:path'
import { Transform } from 'node:stream'
import { parse } from 'csv-parse'
import { stringify } from 'csv-stringify'
import iconv from 'iconv-lite'
import jschardet from 'jschardet'
import dayjs from 'dayjs'
import customParseFormat from 'dayjs/plugin/customParseFormat.js'
import ExcelJS from 'exceljs'

dayjs.extend(customParseFormat)

export const REQUIRED_COLUMNS = [
  '时间',
  'CI',
  '网络',
  '小区名称',
  '5G总流量(GB)',
  '5G最大用户数',
  '5G上行PRB利用率(%)',
  '5G下行PRB利用率(%)',
  '5G上行体验速率(Mbps)',
  '5G下行体验速率(Mbps)',
  '5G无线接通率(%)',
  '5G无线掉线率(%)',
  '5G切换成功率(%)',
  '5G上行平均干扰(dBm)'
] as const

export const OUTPUT_COLUMNS = [
  '时间',
  '网络',
  '5G总流量(GB)',
  '5G最大用户数',
  '5G上行PRB利用率(%)',
  '5G下行PRB利用率(%)',
  '5G上行体验速率(Mbps)',
  '5G下行体验速率(Mbps)',
  '5G无线接通率(%)',
  '5G无线掉线率(%)',
  '5G切换成功率(%)',
  '5G上行平均干扰(dBm)'
] as const

export type RequiredColumn = (typeof REQUIRED_COLUMNS)[number]
export type OutputColumn = (typeof OUTPUT_COLUMNS)[number]

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

interface DetectionResult {
  encoding: string
  delimiter: string
  previewRows: string[][]
}

export interface AggBucket {
  date: string
  network: string
  flowSum: number
  maxUserSum: number
  ulPrbSum: number
  ulPrbCount: number
  dlPrbSum: number
  dlPrbCount: number
  ulRateSum: number
  ulRateCount: number
  dlRateSum: number
  dlRateCount: number
  accessRateSum: number
  accessRateCount: number
  dropRateSum: number
  dropRateCount: number
  handoverRateSum: number
  handoverRateCount: number
  interferenceSum: number
  interferenceCount: number
}

type OutputRow = Record<string, string>

const DATE_PATTERNS = [
  'YYYY-MM-DD HH:mm:ss',
  'YYYY/MM/DD HH:mm:ss',
  'YYYY-MM-DD HH:mm',
  'YYYY/MM/DD HH:mm',
  'YYYY-MM-DD',
  'YYYY/MM/DD',
  'YYYYMMDDHHmmss',
  'YYYYMMDD'
]

const DEFAULT_DELIMITER = ','
const GRAND_TOTAL_LABEL = '\u603B\u8BA1'

const normalizeHeader = (value: string): string => {
  return value
    .replace(/^\uFEFF/, '')
    .replace(/（/g, '(')
    .replace(/）/g, ')')
    .replace(/\s+/g, ' ')
    .trim()
}

const parseDateToDay = (value: string): string | null => {
  const raw = value.trim()
  if (!raw) {
    return null
  }

  for (const pattern of DATE_PATTERNS) {
    const parsed = dayjs(raw, pattern, true)
    if (parsed.isValid()) {
      return parsed.format('YYYY-MM-DD')
    }
  }

  const fallback = dayjs(raw)
  if (fallback.isValid()) {
    return fallback.format('YYYY-MM-DD')
  }

  return null
}

const parseNumeric = (value: string | undefined): number | null => {
  if (value === undefined) {
    return null
  }

  const cleaned = value
    .trim()
    .replace(/,/g, '')
    .replace(/%/g, '')

  if (!cleaned) {
    return null
  }

  const num = Number(cleaned)
  if (!Number.isFinite(num)) {
    return null
  }

  return num
}

const formatNumber = (value: number): string => {
  if (!Number.isFinite(value)) {
    return ''
  }
  return Number(value.toFixed(6)).toString()
}

const formatAverage = (sum: number, count: number): string => {
  if (count <= 0) {
    return ''
  }
  return formatNumber(sum / count)
}

const detectDelimiter = (sample: string): string => {
  const lines = sample
    .split(/\r?\n/)
    .map((line) => line.trim())
    .filter((line) => line.length > 0)
    .slice(0, 20)

  const candidates = [',', ';', '\t', '|']
  const scores = new Map<string, number>()

  for (const candidate of candidates) {
    const counts = lines.map((line) => line.split(candidate).length)
    if (counts.length === 0) {
      scores.set(candidate, 0)
      continue
    }
    const valid = counts.filter((count) => count > 1).length
    const consistency = counts.every((count) => count === counts[0]) ? 2 : 1
    scores.set(candidate, valid * consistency)
  }

  const [best] = [...scores.entries()].sort((a, b) => b[1] - a[1])[0] ?? [DEFAULT_DELIMITER]
  return best || DEFAULT_DELIMITER
}

const readFileHeadBuffer = async (filePath: string, maxBytes = 2 * 1024 * 1024): Promise<Buffer> => {
  return await new Promise((resolve, reject) => {
    const chunks: Buffer[] = []
    let total = 0

    const stream = createReadStream(filePath, { highWaterMark: 64 * 1024 })

    stream.on('data', (chunk: Buffer | string) => {
      if (typeof chunk === 'string') {
        chunk = Buffer.from(chunk)
      }
      if (total >= maxBytes) {
        stream.destroy()
        return
      }

      const available = maxBytes - total
      const sliced = chunk.length > available ? chunk.subarray(0, available) : chunk
      chunks.push(sliced)
      total += sliced.length

      const merged = Buffer.concat(chunks)
      const lineCount = merged.toString('binary').split(/\r?\n/).length
      if (lineCount >= 101 || total >= maxBytes) {
        stream.destroy()
      }
    })

    stream.on('close', () => {
      resolve(Buffer.concat(chunks))
    })

    stream.on('error', reject)
  })
}

const detectCsv = async (filePath: string): Promise<DetectionResult> => {
  const headBuffer = await readFileHeadBuffer(filePath)
  const detect = jschardet.detect(headBuffer)

  const encoding = detect.encoding ? detect.encoding.toLowerCase() : 'utf-8'
  const normalizedEncoding =
    encoding === 'ascii'
      ? 'utf-8'
      : encoding === 'gb2312'
        ? 'gb18030'
        : encoding === 'windows-1252'
          ? 'utf-8'
          : encoding

  const text = iconv.decode(headBuffer, normalizedEncoding)
  const delimiter = detectDelimiter(text)

  const previewRows = await parseCsvText(text, delimiter, 100)

  return {
    encoding: normalizedEncoding,
    delimiter,
    previewRows
  }
}

const parseCsvText = async (text: string, delimiter: string, maxRows: number): Promise<string[][]> => {
  return await new Promise((resolve, reject) => {
    const rows: string[][] = []

    const parser = parse({
      delimiter,
      relax_quotes: true,
      relax_column_count: true,
      skip_empty_lines: true,
      trim: false
    })

    parser.on('readable', () => {
      let row: string[] | null
      while ((row = parser.read()) !== null) {
        rows.push(row)
        if (rows.length >= maxRows + 1) {
          parser.destroy()
          break
        }
      }
    })

    parser.on('end', () => resolve(rows.slice(0, maxRows + 1)))
    parser.on('close', () => resolve(rows.slice(0, maxRows + 1)))
    parser.on('error', reject)

    parser.write(text)
    parser.end()
  })
}

const getMissingColumns = (header: string[]): RequiredColumn[] => {
  const normalizedHeader = header.map(normalizeHeader)
  const set = new Set(normalizedHeader)

  return REQUIRED_COLUMNS.filter((column) => !set.has(normalizeHeader(column)))
}

const getOutputBasePath = (inputPath: string): { dir: string; ext: string; fileName: string } => {
  const dir = dirname(inputPath)
  const ext = extname(inputPath) || '.csv'
  const fileName = basename(inputPath, extname(inputPath))
  return { dir, ext, fileName }
}

const getOutputPath = (inputPath: string): string => {
  const { dir, ext, fileName } = getOutputBasePath(inputPath)
  return join(dir, `${fileName}-统计${ext}`)
}

const getNextAvailableOutputPath = (inputPath: string): string => {
  const { dir, ext, fileName } = getOutputBasePath(inputPath)
  const firstPath = join(dir, `${fileName}-统计${ext}`)
  if (!existsSync(firstPath)) {
    return firstPath
  }

  let index = 1
  while (index < 10000) {
    const candidate = join(dir, `${fileName}-统计(${index})${ext}`)
    if (!existsSync(candidate)) {
      return candidate
    }
    index += 1
  }

  return join(dir, `${fileName}-统计(${Date.now()})${ext}`)
}

const isXlsxPath = (filePath: string): boolean => {
  return extname(filePath).toLowerCase() === '.xlsx'
}

const isDateLikeValue = (value: string): boolean => {
  const day = parseDateToDay(value)
  return Boolean(day)
}

const toPreviewRows = (rows: OutputRow[], maxRows = 200): string[][] => {
  const header = [...OUTPUT_COLUMNS]
  const body = rows.slice(0, maxRows).map((row) =>
    OUTPUT_COLUMNS.map((column) => row[column] ?? '')
  )
  return [header, ...body]
}

export const previewAndValidateCsv = async (filePath: string): Promise<ValidationResult> => {
  if (!existsSync(filePath)) {
    return {
      encoding: 'utf-8',
      delimiter: DEFAULT_DELIMITER,
      previewRows: [],
      requiredColumnsFound: false,
      missingColumns: [...REQUIRED_COLUMNS]
    }
  }

  const detected = await detectCsv(filePath)
  const header = detected.previewRows[0] ?? []
  const missingColumns = getMissingColumns(header)

  return {
    encoding: detected.encoding,
    delimiter: detected.delimiter,
    previewRows: detected.previewRows.slice(0, 100),
    requiredColumnsFound: missingColumns.length === 0,
    missingColumns
  }
}

const addAvgField = (
  bucket: AggBucket,
  fieldKey: keyof AggBucket,
  countKey: keyof AggBucket,
  value: number | null
): boolean => {
  if (value === null) {
    return false
  }

  const current = Number(bucket[fieldKey])
  const currentCount = Number(bucket[countKey])
  ;(bucket[fieldKey] as number) = current + value
  ;(bucket[countKey] as number) = currentCount + 1
  return true
}

const createEmptyBucket = (date: string, network: string): AggBucket => ({
  date,
  network,
  flowSum: 0,
  maxUserSum: 0,
  ulPrbSum: 0,
  ulPrbCount: 0,
  dlPrbSum: 0,
  dlPrbCount: 0,
  ulRateSum: 0,
  ulRateCount: 0,
  dlRateSum: 0,
  dlRateCount: 0,
  accessRateSum: 0,
  accessRateCount: 0,
  dropRateSum: 0,
  dropRateCount: 0,
  handoverRateSum: 0,
  handoverRateCount: 0,
  interferenceSum: 0,
  interferenceCount: 0
})

const mergeBucket = (target: AggBucket, source: AggBucket): void => {
  target.flowSum += source.flowSum
  target.maxUserSum += source.maxUserSum
  target.ulPrbSum += source.ulPrbSum
  target.ulPrbCount += source.ulPrbCount
  target.dlPrbSum += source.dlPrbSum
  target.dlPrbCount += source.dlPrbCount
  target.ulRateSum += source.ulRateSum
  target.ulRateCount += source.ulRateCount
  target.dlRateSum += source.dlRateSum
  target.dlRateCount += source.dlRateCount
  target.accessRateSum += source.accessRateSum
  target.accessRateCount += source.accessRateCount
  target.dropRateSum += source.dropRateSum
  target.dropRateCount += source.dropRateCount
  target.handoverRateSum += source.handoverRateSum
  target.handoverRateCount += source.handoverRateCount
  target.interferenceSum += source.interferenceSum
  target.interferenceCount += source.interferenceCount
}

const toOutputRow = (item: AggBucket, dateValue?: string, networkValue?: string): OutputRow => {
  return {
    时间: dateValue ?? item.date,
    网络: networkValue ?? item.network,
    '5G总流量(GB)': formatNumber(item.flowSum),
    '5G最大用户数': formatNumber(item.maxUserSum),
    '5G上行PRB利用率(%)': formatAverage(item.ulPrbSum, item.ulPrbCount),
    '5G下行PRB利用率(%)': formatAverage(item.dlPrbSum, item.dlPrbCount),
    '5G上行体验速率(Mbps)': formatAverage(item.ulRateSum, item.ulRateCount),
    '5G下行体验速率(Mbps)': formatAverage(item.dlRateSum, item.dlRateCount),
    '5G无线接通率(%)': formatAverage(item.accessRateSum, item.accessRateCount),
    '5G无线掉线率(%)': formatAverage(item.dropRateSum, item.dropRateCount),
    '5G切换成功率(%)': formatAverage(item.handoverRateSum, item.handoverRateCount),
    '5G上行平均干扰(dBm)': formatAverage(item.interferenceSum, item.interferenceCount)
  }
}

const applySheetLikeStyle = (worksheet: ExcelJS.Worksheet): void => {
  worksheet.views = [{ state: 'frozen', ySplit: 1 }]

  worksheet.columns = OUTPUT_COLUMNS.map((column, index) => {
    if (index === 0) {
      return { key: column, width: 13 }
    }
    if (index === 1) {
      return { key: column, width: 10 }
    }
    if (index === 2) {
      return { key: column, width: 33 }
    }
    return { key: column, width: 14 }
  })

  worksheet.eachRow((row, rowNumber) => {
    row.alignment = { vertical: 'middle' }
    row.font = { name: '宋体', size: 11 }
    if (rowNumber === 1) {
      row.font = { name: '宋体', size: 11, bold: true }
      row.alignment = { vertical: 'middle', horizontal: 'left' }
    }
  })
}

const writeCsvFile = async (
  outputPath: string,
  outputRows: OutputRow[],
  delimiter: string,
  encoding: string
): Promise<void> => {
  await new Promise<void>((resolve, reject) => {
    const writer = createWriteStream(outputPath)
    const stringifier = stringify({
      header: true,
      columns: [...OUTPUT_COLUMNS],
      delimiter
    })

    const targetEncoding = encoding.toLowerCase()
    const shouldTranscode = targetEncoding !== 'utf-8' && targetEncoding !== 'utf8'
    const transcoder = new Transform({
      transform(chunk, _encoding, callback) {
        try {
          const textChunk = typeof chunk === 'string' ? chunk : Buffer.from(chunk).toString('utf8')
          callback(null, iconv.encode(textChunk, targetEncoding))
        } catch (error) {
          callback(error as Error)
        }
      }
    })

    writer.on('finish', () => resolve())
    writer.on('error', reject)
    stringifier.on('error', reject)
    transcoder.on('error', reject)

    if (shouldTranscode) {
      stringifier.pipe(transcoder).pipe(writer)
    } else {
      stringifier.pipe(writer)
    }

    for (const row of outputRows) {
      stringifier.write(row)
    }

    stringifier.end()
  })
}

const writeXlsxFile = async (outputPath: string, outputRows: OutputRow[]): Promise<void> => {
  const workbook = new ExcelJS.Workbook()
  const worksheet = workbook.addWorksheet('Sheet1')

  worksheet.addRow([...OUTPUT_COLUMNS])
  for (const row of outputRows) {
    const values = OUTPUT_COLUMNS.map((column) => {
      const value = row[column] ?? ''
      if (!value) {
        return ''
      }
      if (column === '时间') {
        if (value === GRAND_TOTAL_LABEL) {
          return value
        }
        if (isDateLikeValue(value)) {
          return value
        }
      }
      const asNumber = Number(value)
      return Number.isFinite(asNumber) && value.trim() !== '' ? asNumber : value
    })
    worksheet.addRow(values)
  }

  applySheetLikeStyle(worksheet)

  const dateColumnIndex = OUTPUT_COLUMNS.indexOf('时间') + 1
  worksheet.eachRow((row, rowNumber) => {
    if (rowNumber === 1) {
      return
    }

    const timeCell = row.getCell(dateColumnIndex)
    const raw = String(timeCell.value ?? '')
    if (raw && raw !== GRAND_TOTAL_LABEL && isDateLikeValue(raw)) {
      const parsed = dayjs(raw)
      if (parsed.isValid()) {
        timeCell.value = parsed.toDate()
        timeCell.numFmt = 'mm-dd-yy'
      }
    }
  })

  await workbook.xlsx.writeFile(outputPath)
}

const writeOutputFile = async (
  sourcePath: string,
  outputPath: string,
  outputRows: OutputRow[],
  delimiter: string,
  encoding: string
): Promise<void> => {
  if (isXlsxPath(sourcePath)) {
    await writeXlsxFile(outputPath, outputRows)
    return
  }

  await writeCsvFile(outputPath, outputRows, delimiter, encoding)
}

const parseDataRows = async (
  filePath: string,
  preview: ValidationResult
): Promise<{
  buckets: Map<string, AggBucket>
  processedRows: number
  skippedRows: number
  invalidFieldStats: Record<string, number>
}> => {
  const indexMap = new Map<string, number>()
  const normalizedHeader = (preview.previewRows[0] ?? []).map(normalizeHeader)
  normalizedHeader.forEach((col, idx) => indexMap.set(col, idx))

  const colIndex = {
    time: indexMap.get('时间') ?? -1,
    network: indexMap.get('网络') ?? -1,
    flow: indexMap.get('5G总流量(GB)') ?? -1,
    maxUser: indexMap.get('5G最大用户数') ?? -1,
    ulPrb: indexMap.get('5G上行PRB利用率(%)') ?? -1,
    dlPrb: indexMap.get('5G下行PRB利用率(%)') ?? -1,
    ulRate: indexMap.get('5G上行体验速率(Mbps)') ?? -1,
    dlRate: indexMap.get('5G下行体验速率(Mbps)') ?? -1,
    accessRate: indexMap.get('5G无线接通率(%)') ?? -1,
    dropRate: indexMap.get('5G无线掉线率(%)') ?? -1,
    handoverRate: indexMap.get('5G切换成功率(%)') ?? -1,
    interference: indexMap.get('5G上行平均干扰(dBm)') ?? -1
  }

  const buckets = new Map<string, AggBucket>()
  const invalidFieldStats: Record<string, number> = {
    时间: 0,
    网络: 0,
    '5G总流量(GB)': 0,
    '5G最大用户数': 0,
    '5G上行PRB利用率(%)': 0,
    '5G下行PRB利用率(%)': 0,
    '5G上行体验速率(Mbps)': 0,
    '5G下行体验速率(Mbps)': 0,
    '5G无线接通率(%)': 0,
    '5G无线掉线率(%)': 0,
    '5G切换成功率(%)': 0,
    '5G上行平均干扰(dBm)': 0
  }

  let processedRows = 0
  let skippedRows = 0
  let isHeader = true

  await new Promise<void>((resolve, reject) => {
    const fileStream = createReadStream(filePath)
    const decoder = iconv.decodeStream(preview.encoding)
    const parser = parse({
      delimiter: preview.delimiter,
      relax_quotes: true,
      relax_column_count: true,
      skip_empty_lines: true,
      trim: false
    })

    parser.on('readable', () => {
      let row: string[] | null
      while ((row = parser.read()) !== null) {
        if (isHeader) {
          isHeader = false
          continue
        }

        processedRows += 1

        const timeRaw = row[colIndex.time] ?? ''
        const networkRaw = (row[colIndex.network] ?? '').trim()
        const day = parseDateToDay(timeRaw)

        if (!day) {
          invalidFieldStats['时间'] += 1
          skippedRows += 1
          continue
        }

        if (!networkRaw) {
          invalidFieldStats['网络'] += 1
          skippedRows += 1
          continue
        }

        const key = `${day}__${networkRaw}`
        const bucket = buckets.get(key) ?? createEmptyBucket(day, networkRaw)

        const flow = parseNumeric(row[colIndex.flow])
        if (flow === null) {
          invalidFieldStats['5G总流量(GB)'] += 1
        } else {
          bucket.flowSum += flow
        }

        const maxUser = parseNumeric(row[colIndex.maxUser])
        if (maxUser === null) {
          invalidFieldStats['5G最大用户数'] += 1
        } else {
          bucket.maxUserSum += maxUser
        }

        if (!addAvgField(bucket, 'ulPrbSum', 'ulPrbCount', parseNumeric(row[colIndex.ulPrb]))) {
          invalidFieldStats['5G上行PRB利用率(%)'] += 1
        }
        if (!addAvgField(bucket, 'dlPrbSum', 'dlPrbCount', parseNumeric(row[colIndex.dlPrb]))) {
          invalidFieldStats['5G下行PRB利用率(%)'] += 1
        }
        if (!addAvgField(bucket, 'ulRateSum', 'ulRateCount', parseNumeric(row[colIndex.ulRate]))) {
          invalidFieldStats['5G上行体验速率(Mbps)'] += 1
        }
        if (!addAvgField(bucket, 'dlRateSum', 'dlRateCount', parseNumeric(row[colIndex.dlRate]))) {
          invalidFieldStats['5G下行体验速率(Mbps)'] += 1
        }
        if (!addAvgField(bucket, 'accessRateSum', 'accessRateCount', parseNumeric(row[colIndex.accessRate]))) {
          invalidFieldStats['5G无线接通率(%)'] += 1
        }
        if (!addAvgField(bucket, 'dropRateSum', 'dropRateCount', parseNumeric(row[colIndex.dropRate]))) {
          invalidFieldStats['5G无线掉线率(%)'] += 1
        }
        if (!addAvgField(bucket, 'handoverRateSum', 'handoverRateCount', parseNumeric(row[colIndex.handoverRate]))) {
          invalidFieldStats['5G切换成功率(%)'] += 1
        }
        if (
          !addAvgField(
            bucket,
            'interferenceSum',
            'interferenceCount',
            parseNumeric(row[colIndex.interference])
          )
        ) {
          invalidFieldStats['5G上行平均干扰(dBm)'] += 1
        }

        buckets.set(key, bucket)
      }
    })

    parser.on('end', () => resolve())
    parser.on('error', (error) => reject(error))

    fileStream.on('error', reject)
    fileStream.pipe(decoder).pipe(parser)
  })

  return { buckets, processedRows, skippedRows, invalidFieldStats }
}

const buildOutputRows = (buckets: Map<string, AggBucket>, options?: AggregationOutputOptions): OutputRow[] => {
  const sortedBuckets = [...buckets.values()].sort((a, b) => {
    if (a.date === b.date) {
      return a.network.localeCompare(b.network)
    }
    return a.date.localeCompare(b.date)
  })

  const includeDailySubtotalRows = Boolean(options?.includeDailySubtotalRows)
  const includeGrandTotalRow = Boolean(options?.includeGrandTotalRow)
  const blankDateForDetailRowsWhenSubtotalEnabled =
    options?.blankDateForDetailRowsWhenSubtotalEnabled ?? true

  const outputRows: OutputRow[] = []

  if (!includeDailySubtotalRows) {
    for (const item of sortedBuckets) {
      outputRows.push(toOutputRow(item))
    }
  } else {
    const byDate = new Map<string, AggBucket[]>()
    for (const item of sortedBuckets) {
      const list = byDate.get(item.date) ?? []
      list.push(item)
      byDate.set(item.date, list)
    }

    for (const [date, dateBuckets] of [...byDate.entries()].sort((a, b) => a[0].localeCompare(b[0]))) {
      const subtotal = createEmptyBucket(date, '')
      for (const item of dateBuckets) {
        mergeBucket(subtotal, item)
      }
      outputRows.push(toOutputRow(subtotal, date, ''))

      for (const item of dateBuckets.sort((a, b) => a.network.localeCompare(b.network))) {
        outputRows.push(
          toOutputRow(
            item,
            blankDateForDetailRowsWhenSubtotalEnabled ? '' : date,
            item.network
          )
        )
      }
    }
  }

  if (includeGrandTotalRow) {
    const grand = createEmptyBucket('', '')
    for (const item of sortedBuckets) {
      mergeBucket(grand, item)
    }
    outputRows.push(toOutputRow(grand, GRAND_TOTAL_LABEL, ''))
  }

  return outputRows
}

export const runCsvAggregation = async (
  filePath: string,
  options?: AggregationOutputOptions
): Promise<AggregationRunResult> => {
  try {
    if (!existsSync(filePath)) {
      return {
        success: false,
        processedRows: 0,
        skippedRows: 0,
        invalidFieldStats: {},
        errorCode: 'FILE_NOT_FOUND',
        message: '输入文件不存在。'
      }
    }

    const preview = await previewAndValidateCsv(filePath)
    if (!preview.requiredColumnsFound) {
      return {
        success: false,
        processedRows: 0,
        skippedRows: 0,
        invalidFieldStats: {},
        errorCode: 'MISSING_COLUMNS',
        message: 'CSV 缺少关键列，无法计算。',
        missingColumns: preview.missingColumns
      }
    }

    const { buckets, processedRows, skippedRows, invalidFieldStats } = await parseDataRows(filePath, preview)
    const outputRows = buildOutputRows(buckets, options)
    const outputPath = getNextAvailableOutputPath(filePath)

    await writeOutputFile(filePath, outputPath, outputRows, preview.delimiter, preview.encoding)

    return {
      success: true,
      outputPath,
      outputFileName: basename(outputPath),
      processedRows,
      skippedRows,
      invalidFieldStats,
      resultRows: toPreviewRows(outputRows)
    }
  } catch (error) {
    return {
      success: false,
      processedRows: 0,
      skippedRows: 0,
      invalidFieldStats: {},
      errorCode: 'UNKNOWN',
      message: error instanceof Error ? error.message : '未知错误'
    }
  }
}

export const __internal__ = {
  normalizeHeader,
  parseDateToDay,
  parseNumeric,
  detectDelimiter,
  formatNumber,
  formatAverage,
  getMissingColumns,
  getOutputPath,
  getNextAvailableOutputPath
}

