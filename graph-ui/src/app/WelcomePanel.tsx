import { useEffect, useRef } from 'react';
import type { JSX } from 'react';
import type { Workspace, Guidance } from './workspace-strings';
import { workspaceStrings as s } from './workspace-strings';

interface Props {
    workspace: Workspace; guidance: Guidance;
    onWorkspace: (workspace: Workspace) => void; onGuidance: (guidance: Guidance) => void;
    onContinue: () => void; onLocalAi: () => void;
}
export default function WelcomePanel(props: Props): JSX.Element {
    const panelRef = useRef<HTMLElement>(null);
    useEffect(() => {
        const previous = document.activeElement as HTMLElement | null;
        panelRef.current?.querySelector<HTMLButtonElement>('.cbm-primary')?.focus();
        return () => previous?.focus();
    }, []);
    return <div className="cbm-welcome-backdrop">
        <section ref={panelRef} className="cbm-welcome" role="dialog" aria-modal="true" aria-labelledby="cbm-welcome-title"
            onKeyDown={(event) => {
                if (event.key === 'Escape') { event.stopPropagation(); props.onContinue(); }
                if (event.key !== 'Tab') return;
                const controls = Array.from(event.currentTarget.querySelectorAll<HTMLElement>('button, select'));
                const first = controls[0], last = controls[controls.length - 1];
                if (event.shiftKey && document.activeElement === first) { event.preventDefault(); last?.focus(); }
                if (!event.shiftKey && document.activeElement === last) { event.preventDefault(); first?.focus(); }
            }}>
            <p className="cbm-welcome-eyebrow">{s.welcomeEyebrow}</p>
            <h2 id="cbm-welcome-title">{s.welcomeTitle}</h2><p>{s.welcomeDescription}</p>
            <div className="cbm-welcome-choices" role="group" aria-label={s.navigation}>
                {s.workspaces.map((item) => <button type="button" key={item.id}
                    aria-pressed={props.workspace === item.id} onClick={() => props.onWorkspace(item.id)}>
                    <strong>{item.label}</strong><span>{s.descriptions[item.id]}</span>
                </button>)}
            </div>
            <label className="cbm-welcome-guidance">{s.guidance}
                <select value={props.guidance} onChange={(event) => props.onGuidance(event.target.value as Guidance)}>
                    <option value="brief">{s.brief}</option><option value="explained">{s.explained}</option>
                </select>
            </label>
            <p className="cbm-welcome-note">{s.changeLater}</p>
            <footer><button type="button" onClick={props.onLocalAi}>{s.localAi}</button>
                <button type="button" className="cbm-primary" autoFocus onClick={props.onContinue}>{s.start}</button></footer>
            <p className="cbm-welcome-note">{s.aiOptional}</p>
        </section>
    </div>;
}
