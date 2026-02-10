import { afterEach, describe, expect, it } from 'vitest'
import { mkdtempSync, rmSync, writeFileSync, readFileSync } from 'node:fs'
import { join } from 'node:path'
import { tmpdir } from 'node:os'
import iconv from 'iconv-lite'
import {
  __internal__,
  previewAndValidateCsv,
  runCsvAggregation,
  REQUIRED_COLUMNS
} from './csv-service'

const makeTempDir = (): string => mkdtempSync(join(tmpdir(), 'csv-analyzer-'))

const createCsv = (dir: string, name: string, content: string, encoding = 'utf-8'): string => {
  const filePath = join(dir, name)
  writeFileSync(filePath, iconv.encode(content, encoding))
  return filePath
}

describe('csv-service internals', () => {
  it('normalizes header with full-width bracket and spaces', () => {
    expect(__internal__.normalizeHeader(' 5G上行PRB利用率（%）  ')).toBe('5G上行PRB利用率(%)')
  })

  it('parses multi-format date to YYYY-MM-DD', () => {
    expect(__internal__.parseDateToDay('2025-01-01 12:20:11')).toBe('2025-01-01')
    expect(__internal__.parseDateToDay('2025/01/02')).toBe('2025-01-02')
    expect(__internal__.parseDateToDay('20250103')).toBe('2025-01-03')
  })

  it('parses numeric values with percent and thousands separators', () => {
    expect(__internal__.parseNumeric('1,234.56')).toBe(1234.56)
    expect(__internal__.parseNumeric('88.8%')).toBe(88.8)
    expect(__internal__.parseNumeric('')).toBeNull()
    expect(__internal__.parseNumeric('abc')).toBeNull()
  })

  it('detects delimiter from sample text', () => {
    expect(__internal__.detectDelimiter('a,b,c\n1,2,3')).toBe(',')
    expect(__internal__.detectDelimiter('a\tb\tc\n1\t2\t3')).toBe('\t')
  })

  it('finds output path with -统计 suffix', () => {
    expect(__internal__.getOutputPath('D:/x/input.csv')).toContain('input-统计.csv')
  })
})

describe('csv-service integration', () => {
  const dirs: string[] = []

  afterEach(() => {
    for (const dir of dirs.splice(0, dirs.length)) {
      rmSync(dir, { recursive: true, force: true })
    }
  })

  it('stops when required columns are missing', async () => {
    const dir = makeTempDir()
    dirs.push(dir)

    const header = REQUIRED_COLUMNS.filter((col) => col !== '5G下行PRB利用率(%)').join(',')
    const row =
      '2025-01-01 01:00:00,ci1,NSA,小区A,10,100,20,50,100,99.9,0.1,95,-110'
    const path = createCsv(dir, 'missing.csv', `${header}\n${row}`)

    const preview = await previewAndValidateCsv(path)
    expect(preview.requiredColumnsFound).toBe(false)
    expect(preview.missingColumns).toContain('5G下行PRB利用率(%)')

    const run = await runCsvAggregation(path)
    expect(run.success).toBe(false)
    expect(run.errorCode).toBe('MISSING_COLUMNS')
  })

  it('aggregates by day and network, keeps source encoding', async () => {
    const dir = makeTempDir()
    dirs.push(dir)

    const header = REQUIRED_COLUMNS.join(',')
    const lines = [
      '2025-01-01 01:00:00,ci1,NSA,小区A,10,100,20,30,40,50,99.9,0.1,95,-110',
      '2025-01-01 08:00:00,ci2,NSA,小区B,20,200,30,40,60,70,98.9,0.2,93,-108',
      '2025-01-02 01:00:00,ci3,SA,小区C,5,50,15,25,35,45,97,0.3,92,-105'
    ]
    const path = createCsv(dir, 'ok.csv', `${header}\n${lines.join('\n')}`, 'gb18030')

    const preview = await previewAndValidateCsv(path)
    expect(preview.requiredColumnsFound).toBe(true)
    expect(preview.encoding).toBe('gb18030')

    const run = await runCsvAggregation(path)
    expect(run.success).toBe(true)
    expect(run.outputPath).toBeDefined()
    expect(run.processedRows).toBe(3)
    expect(run.skippedRows).toBe(0)

    const outputContent = iconv.decode(readFileSync(run.outputPath!), 'gb18030')
    expect(outputContent).toContain('2025-01-01,NSA,30,300,25')
    expect(outputContent).toContain('2025-01-02,SA,5,50,15')
  })

  it('aborts when output file already exists', async () => {
    const dir = makeTempDir()
    dirs.push(dir)

    const header = REQUIRED_COLUMNS.join(',')
    const line = '2025-01-01 01:00:00,ci1,NSA,小区A,10,100,20,30,40,50,99.9,0.1,95,-110'
    const inputPath = createCsv(dir, 'exist.csv', `${header}\n${line}`)
    createCsv(dir, 'exist-统计.csv', 'dummy')

    const run = await runCsvAggregation(inputPath)
    expect(run.success).toBe(false)
    expect(run.errorCode).toBe('OUTPUT_EXISTS')
  })

  it('skips row when date/network invalid and counts field invalids', async () => {
    const dir = makeTempDir()
    dirs.push(dir)

    const header = REQUIRED_COLUMNS.join(',')
    const rows = [
      'not-a-date,ci1,NSA,小区A,10,100,20,30,40,50,99.9,0.1,95,-110',
      '2025-01-01 01:00:00,ci2,,小区B,11,101,21,31,41,51,98.9,0.2,94,-109',
      '2025-01-01 01:00:00,ci3,NSA,小区C,abc,xyz,xx,30,40,50,99.9,0.1,95,-110'
    ]

    const inputPath = createCsv(dir, 'dirty.csv', `${header}\n${rows.join('\n')}`)
    const run = await runCsvAggregation(inputPath)
    expect(run.success).toBe(true)
    expect(run.processedRows).toBe(3)
    expect(run.skippedRows).toBe(2)
    expect(run.invalidFieldStats['时间']).toBe(1)
    expect(run.invalidFieldStats['网络']).toBe(1)
    expect(run.invalidFieldStats['5G总流量(GB)']).toBe(1)
    expect(run.invalidFieldStats['5G最大用户数']).toBe(1)
    expect(run.invalidFieldStats['5G上行PRB利用率(%)']).toBe(1)
  })

  it('supports Sheet1 style output with daily subtotal and grand total', async () => {
    const dir = makeTempDir()
    dirs.push(dir)

    const header = REQUIRED_COLUMNS.join(',')
    const rows = [
      '2025-12-01 01:00:00,ci1,5G-2.6G,小区A,100,50,70,80,60,120,99.9,0.1,98,-110',
      '2025-12-01 02:00:00,ci2,5G-2.6G,小区B,78.12,29,72,84,64,122,99.8,0.2,97,-108',
      '2025-12-01 03:00:00,ci3,5G-700M,小区C,0.78,6,76,86,66,124,99.7,0.3,96,-107',
      '2025-12-02 01:00:00,ci4,5G-2.6G,小区D,179.12,80,73,83,63,123,99.6,0.4,95,-106',
      '2025-12-02 02:00:00,ci5,5G-700M,小区E,1.78,7,77,87,67,125,99.5,0.5,94,-105'
    ]

    const inputPath = createCsv(dir, 'sheet-style.csv', `${header}\n${rows.join('\n')}`)
    const run = await runCsvAggregation(inputPath, {
      includeDailySubtotalRows: true,
      includeGrandTotalRow: true,
      blankDateForDetailRowsWhenSubtotalEnabled: true
    })

    expect(run.success).toBe(true)
    const output = readFileSync(run.outputPath!, 'utf-8').replace(/\r\n/g, '\n')
    const lines = output
      .trim()
      .split('\n')
      .map((line) => line.replace(/^"|"$/g, '').replace(/","/g, ','))

    expect(lines[1]).toContain('2025-12-01,,178.9,85')
    expect(lines[2]).toContain(',5G-2.6G,178.12,79')
    expect(lines[3]).toContain(',5G-700M,0.78,6')
    expect(lines[4]).toContain('2025-12-02,,180.9,87')
    expect(lines[5]).toContain(',5G-2.6G,179.12,80')
    expect(lines[6]).toContain(',5G-700M,1.78,7')
    expect(lines[7]).toContain('总计,,359.8,172')
  })
})
