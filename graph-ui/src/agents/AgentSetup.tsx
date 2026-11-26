import { useEffect, useState } from 'react';
import type { AtlasApi } from '../app/atlas-api';
import hookSource from '../../agents/hooks/atlas-trace.py?raw';

/** POSIX shell word quoting; paths and indexed names are data, never commands. */
export const shellWord = (value: string): string => `'${value.replaceAll("'", "'\\''")}'`;
export const agentInstallCommand = (root: string, project: string, port: number): string =>
    `python3 ~/Downloads/cbm-atlas-trace.py --install-claude --root ${shellWord(root)} --project ${shellWord(project)} --daemon-url ${shellWord(`http://127.0.0.1:${port}`)}`;

export default function AgentSetup({ api, project, port }: { api?: Pick<AtlasApi, 'repoInfo'>; project?: string; port: number }) {
    const [open, setOpen] = useState(false);
    const [reading, setReading] = useState<{ project: string; root: string; error: string }>();
    const [copied, setCopied] = useState('');
    useEffect(() => {
        if (!open || !api || !project) return;
        let disposed = false;
        setReading(undefined); setCopied('');
        void api.repoInfo(project).then(info => {
            if (!disposed) setReading({ project, root: info.rootPath, error: info.rootPath ? '' : 'The daemon did not report this repository root.' });
        }).catch(error => {
            if (!disposed) setReading({ project, root: '', error: error instanceof Error ? error.message : String(error) });
        });
        return () => { disposed = true; };
    }, [open, api, project]);
    const current = reading?.project === project ? reading : undefined;
    const command = current?.root && project ? agentInstallCommand(current.root, project, port) : '';
    const download = () => {
        const url = URL.createObjectURL(new Blob([hookSource], { type: 'text/x-python;charset=utf-8' }));
        const anchor = document.createElement('a'); anchor.href = url; anchor.download = 'cbm-atlas-trace.py';
        anchor.click(); setTimeout(() => URL.revokeObjectURL(url), 1000);
    };
    return <details className="cbm-agent-setup" onToggle={event => setOpen(event.currentTarget.open)}>
        <summary>Connect your agent</summary>
        <p>Claude Code on macOS/Linux · Python 3 required. This installs a project-local PostToolUse hook. Connecting the browser only reads history; it does not configure your coding client.</p>
        {!project || !api ? <p>Select an indexed project to generate its setup command.</p> : <>
            <ol>
                <li><button type="button" onClick={download}>Download hook</button> Save <code>cbm-atlas-trace.py</code> in Downloads, or adjust its path in the command below.</li>
                <li>Review and run this command in your terminal. It copies the hook into <code>.claude/hooks/</code> and adds it to <code>.claude/settings.local.json</code>, preserving unrelated settings. Conflicting existing hooks are refused.
                    {current?.error ? <p role="alert">Setup unavailable: {current.error}</p> : command ? <>
                        <pre>{command}</pre><button type="button" onClick={() => { void navigator.clipboard.writeText(command).then(() => setCopied('Command copied')).catch(() => setCopied('Clipboard unavailable; select and copy the command.')); }}>Copy setup command</button> <span role="status">{copied}</span>
                    </> : <p>Reading the indexed repository root…</p>}
                </li>
                <li>Start a new Claude Code session in that repository and perform a tool call. Then choose <strong>Load recorded activity</strong> here. A new recorded event is the evidence that the hook reached this daemon.</li>
            </ol>
            <p>Destination: <code>http://127.0.0.1:{port}/api/agent-events</code> · Project: <code>{project}</code>. No bridge or extra listener. Pending events retry on the next tool call.</p>
        </>}
        <p>The hook records tool names, paths, timestamps, optional line spans and up to 180 characters of a command or search pattern. It records no file contents or tool results. Metadata stays in the local daemon; commands and paths can still contain sensitive information.</p>
        <p>To remove it, delete only the entry referencing <code>cbm-atlas-trace.py</code> from this project's <code>PostToolUse</code> hooks. Other coding clients need their own event adapter; their activity is not inferred.</p>
    </details>;
}
