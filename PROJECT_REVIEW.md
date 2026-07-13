# MindForge / fablize-memory-mcp — итоговый review

**Дата:** 2026-07-13 · **ветка:** `fablize-integration` · **объём:** постоянная установка для Codex, procedure/brain-слои, MindForge GUI и полный локальный verification loop.

## Итог

Проект теперь полезен как постоянный слой над Codex, а не как отдельный «сайт-секретарь»:

- Memory даёт агенту структурный граф проекта и уменьшает повторное чтение кода.
- Procedure заставляет работать через план, доказательства и ограниченный closed loop.
- Brain сохраняет проверенные факты и уроки между сессиями.
- MindForge GUI управляет проектом, планом, задачами, approve/reject, терминалом и состоянием этих слоёв.

После `install-combined.sh` интеграция сохраняется после перезапуска: MCP зарегистрирован в Codex, graph-first инструкции и skill установлены глобально, новый репозиторий индексируется автоматически, а `mindforge-doctor.sh` проверяет фактическое состояние.

## Что реализовано

### Постоянная интеграция

- Идемпотентный combined-installer, режим `--check` и подробный doctor.
- Глобальный и project-local MindForge skill для Codex.
- `AGENTS.md`, portable `.fablize-disciplines/`, `.agents/` и Codex hooks.
- Безопасный auto-index с лимитом файлов; private runtime state исключён из git.
- Эволюция skill только через reviewable candidate, baseline/candidate evaluation и явное применение — модель не «переписывает себя» скрытно.

### GUI и производительность

- Runtime-строки вынесены в общий RU/EN i18n-слой; языковая полнота статического HTML сознательно не считается блокером.
- Polling теперь останавливается в скрытом окне, использует backoff 4→8→16→30 секунд и сбрасывается при активности/изменении.
- Metrics subprocess дедуплицируется и кешируется; кеш очищается при смене проекта.
- IPC contract test проверяет соответствие preload API обработчикам main-процесса.
- Реальный скрытый Electron E2E создаёт временный git-проект, создаёт план, принимает G001, отклоняет G002, проверяет merge и receipt, затем удаляет временные данные.

### Документация

- README и INTEGRATION описывают назначение, установку, Codex workflow и ограничения.
- Добавлен версионируемый `CHANGELOG.md`.

## Проверено в этой сессии

| Уровень | Результат |
|---|---|
| Knowledge graph | **14 868 узлов / 52 268 рёбер**; MCP-запросы работают после перезапуска |
| Python (`fablize` + installer) | **149/149 passed** |
| MindForge UI unit/contract | **30/30 passed** |
| MindForge UI syntax | `npm run check` — passed |
| Electron E2E | create → plan → approve → reject; merge + receipt — passed |
| Graph UI | **3/3 passed** |
| Постоянная установка | `install-combined.sh --check` — **healthy** |
| C core | ASan/UBSan test-runner — passed; production binary собран |
| Git hygiene | `git diff --check` — passed |

Первый C-прогон внутри файловой песочницы дал массовый `phase=dump`: тесты без явного `db_path` не могли писать временные SQLite-графы в системный cache. Повтор официального `scripts/test.sh` вне песочницы подтвердил, что это ограничение окружения, а не дефект кода.

## Оставшиеся риски

| Приоритет | Риск | Следующее действие |
|---|---|---|
| **P1** | macOS parent-watchdog: production child переживает смерть тестового parent. C-core оставлен без fork-патча. | Исправить upstream и затем обновить vendored core. До этого doctor/cleanup должны явно показывать живой процесс. |
| **P1** | Проверен dev Electron, но не запуск подписанного `.app`/AppImage после упаковки. | Добавить packaged artifact smoke, macOS arm64/x64 matrix, signing/notarization gate. |
| **P2** | Полная EN-локализация статического HTML не завершена; пользователь указал, что языки не критичны. | Возвращаться только при реальной потребности, не блокировать функциональный roadmap. |
| **P2** | Graph UI heavy chunk и release-size остаются крупными. | Ввести lazy-chunk и artifact size budgets после packaged smoke. |
| **P2** | Нет автоматического dependency update/audit gate. | Добавить Dependabot/Renovate и dependency-review отдельным security этапом. |
| **P3** | `main.js` и крупные view-файлы остаются hotspot для сопровождения. | После стабилизации E2E постепенно выделять IPC/project/process services, без big-bang rewrite. |

## Что делать дальше

1. Зафиксировать текущий verified batch локальным коммитом и прогнать CI без push из этой задачи.
2. Добавить packaged Electron smoke — это главный оставшийся разрыв между «работает из исходников» и «можно раздать пользователю».
3. Закрыть parent-watchdog через upstream.
4. После этого заняться signing, release matrix, dependency automation и размерами артефактов.

**Критерий следующего релиза:** чистая машина → установка → Codex видит Memory/skill/hooks → первый проект → plan → agent → verify → approve/reject → перезапуск → состояние и знания сохранены; тот же путь проходит на упакованном приложении в CI.

---

## Дополнение: независимый closed-loop прогон 2026-07-13 (вторая сессия)

Полный повторный прогон всех уровней на этой машине, с починкой найденного красного.

| Уровень | Результат |
|---|---|
| C test-runner (`make -f Makefile.cbm test`) | **5604 passed**, exit 0 |
| C shell-тесты (`tests/*.sh`) | **4/4 зелёные** после починки `smoke_guard.sh` (см. ниже) |
| `tests/test_parent_watchdog.sh` (macOS) | **3/3 прогона зелёные** — «child переживает смерть родителя» больше не воспроизводится; P1-риск выше следует считать устаревшим до появления нового репро |
| fablize unittest | **146/146 OK** |
| MindForge UI | **35/35** (unit + IPC contract + i18n) · `npm run check` OK |
| Electron E2E | create → plan → approve(G001) → reject(G002) → merge + receipt — passed |
| Graph UI | 3/3 + production build OK |

### Починено в этом прогоне

- **`tests/smoke_guard.sh` был сломан целиком** (все 5 handler-проверок падали): бинарь печатает guard-ошибку в **stderr** и корректно выходит с ненулевым кодом, а скрипт захватывал только stdout и умирал на `set -e` ещё при первом вызове. Исправление локальное в скрипте (`2>&1 || true` при захвате), `src/` не тронут; стоит предложить upstream.
- **i18n доведён до core-модулей**: `projectcore.js`, `providercore.js`, `graphcore.js` (шаги создания проекта, ошибки provider HTTP, ошибки 3D-графа) переведены на словарь — теперь 292/292 ключа en=ru. Тест полноты расширен: скан кириллицы покрывает все runtime-модули, добавлена проверка «каждый используемый ключ `t("…")`/`data-i18n` существует в словаре».
