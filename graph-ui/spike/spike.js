// W0-Spike. Wegwerf-Beweiscode, NICHT die Produktarchitektur.
//
// Ablauf: Symbol aus den Query-Parametern (?project=...&qn=...) holen, den
// Quelltext über POST /rpc (MCP tools/call get_code_snippet) beim gebauten
// C-Server abfragen, ihn read-only in Monaco rendern und genau eine
// Decoration auf die Zeile mit dem validateUser(-Aufruf setzen.
//
// Monaco kommt aus /node_modules/monaco-editor/min/vs (AMD-Loader), kein CDN.

(function () {
  'use strict';

  var MONACO_VS = '/node_modules/monaco-editor/min/vs';
  var CALL_NEEDLE = 'validateUser(';

  var statusEl = document.getElementById('status');

  function setStatus(text, failed) {
    statusEl.textContent = text;
    statusEl.className = failed ? 'failed' : '';
  }

  function fail(stage, err) {
    var message = (err && err.message) ? err.message : String(err);
    setStatus('FEHLER in ' + stage + ': ' + message, true);
    window.__spike = {
      ready: false,
      stage: stage,
      error: message,
      monacoReadOnly: false,
      decorationCount: 0,
      sourceLength: 0
    };
  }

  function params() {
    var q = new URLSearchParams(window.location.search);
    return {
      project: q.get('project') || '',
      qn: q.get('qn') || ''
    };
  }

  // POST /rpc im MCP-Format. Der Mini-Proxy vor dieser Seite reicht den
  // Aufruf serverseitig an den C-Server weiter (dessen Origin-Prüfung
  // lässt eine fremde Seite nicht direkt an /rpc).
  function callTool(name, args) {
    return fetch('/rpc', {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({
        jsonrpc: '2.0',
        id: 1,
        method: 'tools/call',
        params: { name: name, arguments: args }
      })
    }).then(function (res) {
      if (!res.ok) {
        throw new Error('/rpc antwortete mit HTTP ' + res.status);
      }
      return res.json();
    }).then(function (body) {
      if (body.error) {
        throw new Error('/rpc-Fehler: ' + JSON.stringify(body.error));
      }
      var content = body.result && body.result.content;
      if (!content || !content.length || typeof content[0].text !== 'string') {
        throw new Error('/rpc lieferte kein content[0].text');
      }
      return JSON.parse(content[0].text);
    });
  }

  // Die Call-Site suchen: die Zeile, die validateUser( aufruft, nicht der
  // Import und nicht die Deklaration selbst.
  function findCallSiteLine(source) {
    var lines = source.split('\n');
    for (var i = 0; i < lines.length; i++) {
      var line = lines[i];
      if (line.indexOf(CALL_NEEDLE) === -1) { continue; }
      var trimmed = line.trim();
      if (trimmed.indexOf('import') === 0) { continue; }
      if (/\bfunction\s+validateUser\s*\(/.test(trimmed)) { continue; }
      return i + 1; // Monaco zählt Zeilen ab 1
    }
    return 0;
  }

  function loadMonaco() {
    return new Promise(function (resolve, reject) {
      if (typeof window.require !== 'function' || !window.require.config) {
        reject(new Error('AMD-Loader aus ' + MONACO_VS + '/loader.js fehlt'));
        return;
      }
      window.require.config({ paths: { vs: MONACO_VS } });
      window.require(['vs/editor/editor.main'], function () {
        resolve(window.monaco);
      }, function (err) {
        reject(new Error('Monaco ließ sich nicht laden: ' + (err && err.message ? err.message : String(err))));
      });
    });
  }

  function render(monaco, snippet) {
    monaco.editor.defineTheme('atlas-phosphor', {
      base: 'vs-dark',
      inherit: true,
      rules: [],
      colors: {
        'editor.background': '#06090b',
        'editorGutter.background': '#0b1013',
        'editorLineNumber.foreground': '#2f4a40',
        'editorLineNumber.activeForeground': '#33ff99'
      }
    });

    var editor = monaco.editor.create(document.getElementById('editor'), {
      value: snippet.source,
      language: 'typescript',
      theme: 'atlas-phosphor',
      readOnly: true,
      domReadOnly: true,
      glyphMargin: true,
      automaticLayout: true,
      minimap: { enabled: false },
      scrollBeyondLastLine: false,
      renderLineHighlight: 'none',
      fontSize: 14,
      lineNumbers: function (n) {
        // Zeilennummern der echten Datei, nicht des Ausschnitts.
        return String((snippet.start_line || 1) + n - 1);
      }
    });

    var callLine = findCallSiteLine(snippet.source);
    var ids = [];
    if (callLine > 0) {
      ids = editor.deltaDecorations([], [{
        range: new monaco.Range(callLine, 1, callLine, 1),
        options: {
          isWholeLine: true,
          className: 'atlas-callsite-line',
          glyphMarginClassName: 'atlas-callsite-glyph',
          glyphMarginHoverMessage: { value: '1 Aufruf: validateUser' }
        }
      }]);
    }

    return { editor: editor, decorationIds: ids, callLine: callLine };
  }

  function main() {
    var p = params();
    if (!p.project || !p.qn) {
      fail('parameter', new Error('project und qn sind Pflicht-Query-Parameter'));
      return;
    }

    document.getElementById('hdr-qn').textContent = p.qn;
    setStatus('hole Quelltext über /rpc get_code_snippet ...');

    var snippet = null;

    callTool('get_code_snippet', { project: p.project, qualified_name: p.qn })
      .then(function (result) {
        snippet = result;
        if (typeof snippet.source !== 'string' || snippet.source.length === 0) {
          throw new Error('get_code_snippet lieferte kein source-Feld');
        }
        document.getElementById('hdr-file').textContent =
          (snippet.file_path || '?') + ':' + (snippet.start_line || '?');
        setStatus('lade Monaco aus ' + MONACO_VS + ' ...');
        return loadMonaco();
      })
      .then(function (monaco) {
        var rendered = render(monaco, snippet);
        var readOnly = rendered.editor.getOption(monaco.editor.EditorOption.readOnly);

        window.__spike = {
          ready: true,
          monacoReadOnly: readOnly === true,
          decorationCount: rendered.decorationIds.length,
          sourceLength: snippet.source.length,
          qualifiedName: snippet.qualified_name || p.qn,
          filePath: snippet.file_path || '',
          startLine: snippet.start_line || 0,
          endLine: snippet.end_line || 0,
          callSiteLine: rendered.callLine,
          callSiteFileLine: rendered.callLine > 0
            ? (snippet.start_line || 1) + rendered.callLine - 1
            : 0
        };

        setStatus(
          'ok: ' + snippet.source.length + ' Zeichen vom Server, read-only=' +
          (readOnly === true) + ', Decorations=' + rendered.decorationIds.length +
          ' auf Datei-Zeile ' + window.__spike.callSiteFileLine
        );
      })
      .catch(function (err) {
        fail('spike', err);
      });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', main);
  } else {
    main();
  }
})();
