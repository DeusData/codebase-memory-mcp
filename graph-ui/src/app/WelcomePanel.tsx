import { useEffect, useRef } from 'react';
import type { JSX } from 'react';
import type { Workspace } from './workspace-strings';
import { availableWorkspaces, workspaceStrings as s } from './workspace-strings';

interface Props {
    workspace: Workspace;
    experimentalAgents?: boolean;
    onWorkspace: (workspace: Workspace) => void;
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
                {availableWorkspaces(props.experimentalAgents).map((item) => <button type="button" key={item.id}
                    aria-pressed={props.workspace === item.id} onClick={() => props.onWorkspace(item.id)}>
                    <strong>{item.label}</strong><span>{s.descriptions[item.id]}</span>
                </button>)}
            </div>
            <p className="cbm-welcome-note">{s.changeLater}</p>
            <footer><button type="button" onClick={props.onLocalAi}>{s.localAi}</button>
                <button type="button" className="cbm-primary" autoFocus onClick={props.onContinue}>{s.start}</button></footer>
            <p className="cbm-welcome-note">{s.aiOptional}</p>
        </section>
    </div>;
}
