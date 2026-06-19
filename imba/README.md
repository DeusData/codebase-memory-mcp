# imba

Тулкит, который делает **Claude Code** и **Hermes Agent** дешевле и
дисциплинированнее. Один установщик настраивает всё на новой машине.

```bash
git clone <this-repo> imba && cd imba
./setup.sh            # поставить всё, что применимо к твоей системе
```

## Что внутри

| Компонент | Что делает | Куда ставится |
|---|---|---|
| **autoclaude/** | Экономия Claude Code: терсный вывод + узкое чтение (в `~/.claude/CLAUDE.md`), квота-гард хук, `.claudeignore`, 7 готовых **skills** (skill-creator, mcp-builder, webapp-testing, docx/pdf/pptx/xlsx), плюс **дисциплина памяти** (layered-memory правила, episodic-логгер + memory-guard хуки, `/memory-prune`) | `~/.claude/` |
| **hermes-economizer/** | Drop-in плагин для Hermes Agent: бюджет-леджер + гард, fablize-дисциплины | `$HERMES_HOME/plugins/` |

> **Hermes Agent сам** (полный upstream NousResearch/hermes-agent) в этот репо
> НЕ входит — это внешняя зависимость. Установи его отдельно, плагин подключится
> автоматически. См. https://github.com/NousResearch/hermes-agent

## Установка по частям

```bash
./setup.sh autoclaude      # только экономия Claude Code (CLAUDE.md + хук + .claudeignore)
./setup.sh skills          # только Claude Code skills (быстро: sparse-clone, без тяжёлых deps)
./setup.sh hermes-plugin   # только плагин Hermes (если Hermes установлен)
./setup.sh --uninstall     # убрать autoclaude (economy-блок + хук)
```

Все шаги **идемпотентны** — повторный запуск безопасен.

Скиллы по умолчанию ставятся **быстро и без зависимостей** (document/test-скиллы
доустанавливают свои библиотеки при первом вызове). Хочешь сразу всё:

```bash
SKILLS_WITH_DEPS=1 ./setup.sh skills       # + pip-зависимости
SKILLS_WITH_BROWSERS=1 ./setup.sh skills   # + Playwright Chromium (~150MB)
```

## Требования
- `python3` (autoclaude, skills, hermes-cli)
- `git` (skills)
- `hermes` в PATH — только для `hermes-plugin`
- macOS/Linux. Платформо-зависимые пути (`~/.claude`, `~/.hermes`) учтены.

## Принципы (что НЕ делаем и почему)
- **Не переписываем Claude Code / Hermes** — авто-распределение задач и
  субагенты уже встроены; мы только настраиваем и экономим.
- **Без дублей**: не ставим скиллы, повторяющие встроенные `/code-review`,
  `/simplify`, `/verify` или fablize-дисциплины.
- **На подписке экономим квоту, не доллары** — квота-гард считает токены окна.

Детали по компонентам — в их собственных README и в `autoclaude/skills.md`.
