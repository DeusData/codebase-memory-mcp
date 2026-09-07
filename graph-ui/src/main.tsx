import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import App from './App';
import { installUiLog } from './app/ui-log-install';
// Erst die Tokens, dann das Chrome, das sie benutzt. Die Reihenfolge ist keine
// Kosmetik: eine Regel, die eine noch nicht definierte Variable liest, faellt
// still auf ihren Initialwert zurueck.
import './styles/tokens.css';
import './styles/terminal.css';

// Before anything else can fail: the console keeps printing, and from here
// on a copy of every line, every uncaught error and every failed request
// reaches the server's file (app/ui-log.ts, GET /api/ui-log).
installUiLog();

const container = document.getElementById('root');
if (container === null) {
  throw new Error('#root fehlt in index.html');
}

createRoot(container).render(
  <StrictMode>
    <App />
  </StrictMode>,
);
