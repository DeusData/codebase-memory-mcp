/** What an open file is, from its path alone. A YAML workflow that the model took for
 * a Python script got an invented flake8 linter (K12); the kind is a fact the extension
 * gives, so the prompt says it. */
const KINDS: Readonly<Record<string, string>> = {
    yml: 'YAML configuration, not program code', yaml: 'YAML configuration, not program code', json: 'JSON data, not program code',
    toml: 'TOML configuration, not program code', ini: 'INI configuration, not program code', cfg: 'INI configuration, not program code',
    md: 'Markdown text, not program code', rst: 'reStructuredText, not program code', txt: 'plain text, not program code',
    py: 'Python source file', pyi: 'Python stub file', ts: 'TypeScript source file', tsx: 'TypeScript source file', js: 'JavaScript source file',
    mjs: 'JavaScript source file', jsx: 'JavaScript source file', c: 'C source file', h: 'C header file', cc: 'C++ source file', cpp: 'C++ source file',
    hpp: 'C++ header file', go: 'Go source file', rs: 'Rust source file', java: 'Java source file', rb: 'Ruby source file', sh: 'shell script',
    html: 'HTML document', css: 'CSS stylesheet', sql: 'SQL script', xml: 'XML data, not program code',
    conf: 'configuration file, not program code', env: 'environment settings, not program code', properties: 'Java properties, not program code',
    editorconfig: 'EditorConfig settings, not program code', gitignore: 'ignore patterns, not program code', dockerignore: 'ignore patterns, not program code',
    gitattributes: 'Git attribute patterns, not program code',
    // INI files named after their tool, without an extension of their own (B6).
    flake8: 'INI configuration, not program code', coveragerc: 'INI configuration, not program code', pylintrc: 'INI configuration, not program code',
};

/** Configuration, data and text files: what they say is read from them, not guessed by a model (K12). */
export const isDataFile = (path: string): boolean => /not program code/.test(fileKind(path) ?? '');

/** A GitHub Actions workflow, by where GitHub reads it from. */
export const isGithubWorkflow = (path: string): boolean => /(?:^|\/)\.github\/workflows\/[^/]+\.ya?ml$/i.test(path);

export function fileKind(path: string): string | undefined {
    const name = path.split('/').pop() ?? path;
    const template = /^(.+)-tpl$/.exec(name);
    const extension = (template?.[1] ?? name).split('.').slice(1).pop()?.toLowerCase();
    const kind = extension ? KINDS[extension] : undefined;
    if (!kind) return undefined;
    if (isGithubWorkflow(path)) return `GitHub Actions workflow (${kind})`;
    return template ? kind.replace(/ file$/, ' template') : kind;
}
