# autoclaude — установленные Claude Code skills

Скиллы лежат в `~/.claude/skills/` → видны во **всех** сессиях и проектах
автоматически (грузятся по описанию, контекст не раздувают пока не вызваны).
Источник — официальный `anthropics/skills`. Дубли с встроенными
(`/code-review`, `/simplify`, `/verify`) и с fablize-дисциплинами не ставились.

| Скилл | Назначение | Зависимости |
|---|---|---|
| **skill-creator** | создавать/править/мерить свои скиллы (оформить fablize/autoclaude как скиллы) | — |
| **mcp-builder** | строить MCP-серверы (Python FastMCP / TS SDK) — под Hermes/agent | — |
| **webapp-testing** | Playwright: тест/скриншот/логи локальных webapp (под verification-grounding) | playwright + chromium ✓ |
| **docx** | Word-документы | python-docx ✓ |
| **pdf** | чтение/сборка/слияние/OCR PDF | pypdf, pdfplumber ✓ |
| **pptx** | презентации | python-pptx ✓ |
| **xlsx** | таблицы/формулы/чистка данных | openpyxl ✓ |

Проверено: webapp-testing — реальный скриншот HTML через Playwright прошёл;
все Python-зависимости импортируются.

## Переустановка / обновление

```bash
git clone --depth 1 https://github.com/anthropics/skills /tmp/anthropic-skills
mkdir -p ~/.claude/skills
for s in skill-creator mcp-builder webapp-testing docx pdf pptx xlsx; do
  cp -R "/tmp/anthropic-skills/skills/$s" ~/.claude/skills/
done
python3 -m pip install openpyxl pypdf pdfplumber python-pptx python-docx
# webapp-testing (если браузеров нет): python3 -m playwright install chromium
```

Альтернатива (интерактивно, через плагин-механизм Claude Code):
`/plugin marketplace add anthropics/skills` → `/plugin install document-skills@…`.

## Замечания
- docx/pdf/pptx/xlsx — лицензия proprietary (source-available), для личного
  использования ок.
- Свои скиллы пока не писали — для этого теперь есть `skill-creator`.
