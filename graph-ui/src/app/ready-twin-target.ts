import type { SymbolRef } from '../core/focus-protocol';
import type { SemanticIR } from '../core/semantic-ir';
import type { TwinStatus } from '../twin/TwinPanel';

/**
 * Darf ein Follow den sichtbaren, fertigen Twin stehen lassen?
 *
 * Ein Name reicht nicht: ueberladene oder gleich benannte Symbole waeren sonst
 * dieselbe Behauptung. Nur der qualifizierte Name bindet Ziel, sichtbares
 * Subjekt und die bereits gebaute IR eindeutig zusammen.
 */
export function keepsReadyTwinForTarget(
    target: SymbolRef,
    shown: SymbolRef | undefined,
    shownIr: SemanticIR | undefined,
    status: TwinStatus,
): boolean {
    const qualifiedName = target.qualifiedName;
    return status === 'ready'
        && qualifiedName !== undefined
        && qualifiedName.length > 0
        && shown?.qualifiedName === qualifiedName
        && shownIr?.symbol.qualifiedName === qualifiedName;
}
