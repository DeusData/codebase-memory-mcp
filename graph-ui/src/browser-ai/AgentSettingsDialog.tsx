import { useEffect, useRef, type ReactNode } from 'react';
import { createPortal } from 'react-dom';
import './agent-settings.css';

export default function AgentSettingsDialog({ children, onClose }: { children: ReactNode; onClose: () => void }) {
    const dialog = useRef<HTMLDialogElement>(null);
    useEffect(() => {
        const previous = document.activeElement;
        const element = dialog.current;
        if (element && !element.open) {
            if (typeof element.showModal === 'function') element.showModal();
            else element.setAttribute('open', '');
        }
        return () => { if (previous instanceof HTMLElement && previous.isConnected) previous.focus(); };
    }, []);
    return createPortal(<dialog ref={dialog} className="cbm-agent-settings" aria-labelledby="cbm-agent-settings-title"
        onKeyDown={event => event.stopPropagation()} onCancel={event => { event.preventDefault(); onClose(); }} onClick={event => {
            if (event.target !== event.currentTarget) return;
            const bounds = event.currentTarget.getBoundingClientRect();
            if (event.clientX < bounds.left || event.clientX > bounds.right || event.clientY < bounds.top || event.clientY > bounds.bottom) onClose();
        }}>
        <header><div><h2 id="cbm-agent-settings-title">Agent configuration</h2><p>Runs locally in this browser.</p></div>
            <button type="button" aria-label="Close agent configuration" onClick={onClose}>×</button></header>
        <div className="cbm-agent-settings-body">{children}</div>
    </dialog>, document.body);
}
