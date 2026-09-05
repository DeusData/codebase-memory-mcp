import { StrictMode } from 'react';
import { createRoot } from 'react-dom/client';
import App from './App';
// Erst die Tokens, dann das Chrome, das sie benutzt. Die Reihenfolge ist keine
// Kosmetik: eine Regel, die eine noch nicht definierte Variable liest, faellt
// still auf ihren Initialwert zurueck.
import './styles/tokens.css';
import './styles/terminal.css';

const container = document.getElementById('root');
if (container === null) {
  throw new Error('#root fehlt in index.html');
}

createRoot(container).render(
  <StrictMode>
    <App />
  </StrictMode>,
);
