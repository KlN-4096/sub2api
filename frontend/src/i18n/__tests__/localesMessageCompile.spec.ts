import { describe, expect, it } from 'vitest'
import { baseCompile } from '@intlify/message-compiler'

import en from '../locales/en'
import zh from '../locales/zh'

// vue-i18n 在运行时才编译消息：文案里未转义的花括号（如内嵌 JSON 示例
// "{\"user-agent\": ...}"）会在渲染时抛 "Invalid token in placeholder"，
// 直接炸掉整个组件树，且构建期完全无感。本测试把全部文案预编译一遍，
// 将该类问题固化为显式失败。字面量花括号请用 {'{'} / {'}'} 转义，
// 或将语言中立的示例文本（如 JSON）移出 i18n。
function collectCompileErrors(node: unknown, path: string, out: string[]): void {
  if (typeof node === 'string') {
    baseCompile(node, {
      onError: (err) => {
        out.push(`${path}: ${err.message}`)
      }
    })
    return
  }
  if (Array.isArray(node)) {
    node.forEach((item, index) => collectCompileErrors(item, `${path}[${index}]`, out))
    return
  }
  if (node && typeof node === 'object') {
    for (const [key, value] of Object.entries(node as Record<string, unknown>)) {
      collectCompileErrors(value, path ? `${path}.${key}` : key, out)
    }
  }
}

// 裸 `|` 是复数分隔符：不带计数的 t() 只渲染第一段，`|` 之后的文字静默消失，而
// baseCompile 对它**不报错**（复数本来就合法）。只能按形状守：真复数的每一段都带
// 同一个数量占位（"{n} subtask | {n} subtasks"），拿 `|` 当视觉分隔的消息至少有一段
// 没有占位。字面量请写 {'|'}。若将来真要写不带占位的零值复数形式，改这里的判据。
function collectBareSeparatorErrors(node: unknown, path: string, out: string[]): void {
  if (typeof node === 'string') {
    const bare = node.replaceAll("{'|'}", '')
    if (bare.includes('|') && bare.split('|').some((segment) => !/\{[^}]+\}/.test(segment))) {
      out.push(`${path}: bare | (plural separator) — use {'|'} for a literal pipe`)
    }
    return
  }
  if (Array.isArray(node)) {
    node.forEach((item, index) => collectBareSeparatorErrors(item, `${path}[${index}]`, out))
    return
  }
  if (node && typeof node === 'object') {
    for (const [key, value] of Object.entries(node as Record<string, unknown>)) {
      collectBareSeparatorErrors(value, path ? `${path}.${key}` : key, out)
    }
  }
}

describe('locale messages compile', () => {
  it.each([
    ['zh', zh],
    ['en', en]
  ] as const)('%s messages all compile without placeholder errors', (locale, messages) => {
    const errors: string[] = []
    collectCompileErrors(messages, locale, errors)
    expect(errors).toEqual([])
  })

  it.each([
    ['zh', zh],
    ['en', en]
  ] as const)('%s messages use | only as a real plural separator', (locale, messages) => {
    const errors: string[] = []
    collectBareSeparatorErrors(messages, locale, errors)
    expect(errors).toEqual([])
  })

  it('flags a visual pipe but accepts a real plural and an escaped pipe', () => {
    const errors: string[] = []
    collectBareSeparatorErrors(
      {
        plural: '{n} subtask | {n} subtasks',
        escaped: "Sticky only {'|'} Buffer: {buffer}",
        visual: 'Sticky only | Buffer: {buffer}'
      },
      'x',
      errors
    )
    expect(errors).toHaveLength(1)
    expect(errors[0]).toContain('x.visual')
  })
})
