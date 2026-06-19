# autoclaude

Слой экономии токенов поверх **Claude Code** (подписка). Ничего не переделывает:
авто-распределение задач делает сам Claude Code, дисциплины fablize уже подключены
через `~/.claude/CLAUDE.md`. autoclaude добавляет экономию и включает её
**автоматически во всех сессиях** через глобальную установку.

## Что делает

**Экономия:**
1. **Терсный вывод + узкое чтение** (`economy.md`) — правила «без воды, читай
   grep/head, одна задача на сессию, /compact, тяжёлый контекст в субагентов».
   Вшиваются в `~/.claude/CLAUDE.md` → действуют в каждой сессии и флоу.
2. **Квота-гард** (`.claude/hooks/quota_guard.py`) — PreToolUse-хук: бережёт окно
   квоты (токены, не доллары). ~80% → предупреждение; 100% → спрашивает перед
   новыми tool-calls.
3. **`.claudeignore`** (`.claudeignore.template`) — держит node_modules, логи,
   сборки, секреты вне контекста. Кладётся в проект.

**Дисциплина памяти** (brain-like layered memory; controller = Claude Code):
4. **Memory-discipline блок** (`memory.md`) — правила слоёв памяти: semantic =
   стабильные факты с `expires_at`/scope, episodic = raw-логи, procedural =
   skills; selective retention; код-факты → в `codebase-memory-mcp`, не в память.
5. **Episodic-логгер** (`.claude/hooks/episodic_logger.py`) — Stop-хук: пишет
   одну запись задачи (goal / tools / outcome) в `memory/episodes/YYYY-MM.jsonl`,
   деду́п по session_id. Недостающий слой «учиться на прошлых задачах».
6. **Memory-guard** (`.claude/hooks/memory_guard.py`) — PreToolUse-хук на
   Write/Edit: блокирует запись секретов (ключи/токены/JWT) в `memory/`.
7. **`/memory-prune`** (`skills-local/memory-prune/`) — skill: показывает
   протухшие/дублирующие записи и удаляет по подтверждению.

**Качество промптов:**
8. **Prompt-upgrade** (`.claude/hooks/prompt_upgrade.py`) — UserPromptSubmit-хук:
   на каждый *содержательный* промпт тихо инжектит директиву «переформулируй в
   спек (goal/контекст/ограничения/критерии) и выбери дисциплину, потом делай».
   Без LLM-вызова, без лишнего вывода. Тривиальные («привет», «ок», slash-команды)
   пропускает. Отключается `prompt_upgrade: false` в `.claude/autoclaude.yaml`.

9. **Установщик** (`install.sh`) — вшивает всё глобально, идемпотентно.

## Установка (глобально, все сессии)

```bash
./install.sh                 # economy + memory блоки, 3 хука, /memory-prune; .claudeignore в текущий проект
./install.sh /path/to/proj   # + .claudeignore в указанный проект
./install.sh --uninstall      # убрать всё вышеперечисленное
```
Хуки подключаются в `~/.claude/settings.json`: PreToolUse → `quota_guard.py` и
`memory_guard.py`; Stop → `episodic_logger.py`; UserPromptSubmit →
`prompt_upgrade.py`. Все привязки идемпотентны.

## Настройка квоты

`.claude/autoclaude.yaml` в проекте (опционально):
```yaml
cap_tokens: 250000   # лимит окна (ask выше)
warn_ratio: 0.8
```

## Честные границы

- Токены квота-гарда — **оценка** (символы/4 по транскрипту), не реальный usage.
- economy-блок добавляет ~300 токенов на сессию, но окупается концизностью и
  узким чтением многократно.
- Терсность задаётся инструкцией, а не жёстким фильтром — модель её соблюдает,
  но это не гарантия байт-в-байт.

## Что НЕ делаем и почему

- **CLAUDE.md не ужимаем** — твой уже ~560 токенов, экономить нечего.
- **Оркестратор не пишем** — Claude Code распределяет задачи сам.

## Сторонние инструменты (по желанию, drop-in)

- [caveman](https://github.com/juliusbrussee/caveman) — терсный вывод как скилл (~65%).
- rtk — Rust-прокси, сжимает вывод команд (60–90% на dev-командах).
