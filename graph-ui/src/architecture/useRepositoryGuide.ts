import { useEffect, useState } from 'react';
import { sourceQuote, type SourceQuote, type SourceReader } from './repository-guide';

/** Small, sequential batches keep source reads bounded and owned by this project snapshot. */
export function useRepositoryGuide(paths: string[], readSource?: SourceReader) {
    const [reading, setReading] = useState<{ key?: string; reader?: SourceReader; quotes: Record<string, SourceQuote>; finished: boolean; failures: number }>({ quotes: {}, finished: false, failures: 0 });
    const key = paths.join('\n');
    useEffect(() => {
        let active = true;
        const list = key.split('\n').filter(Boolean).slice(0, 29);
        setReading({ key, reader: readSource, quotes: {}, finished: !readSource || list.length === 0, failures: 0 });
        if (!readSource || list.length === 0) return;
        void (async () => {
            const quotes: Record<string, SourceQuote> = {}; let failures = 0;
            for (let index = 0; index < list.length && active; index += 3) {
                const batch = list.slice(index, index + 3);
                const results = await Promise.allSettled(batch.map(path => readSource(path, 1, 65)));
                results.forEach((result, i) => {
                    if (result.status === 'fulfilled') { const quote = sourceQuote(batch[i], result.value.source, result.value.start_line); if (quote) quotes[batch[i]] = quote; }
                    else failures++;
                });
                if (active) setReading({ key, reader: readSource, quotes: { ...quotes }, finished: index + 3 >= list.length, failures });
            }
        })();
        return () => { active = false; };
    }, [key, readSource]);
    return reading.reader === readSource && reading.key === key ? reading : { quotes: {}, finished: false, failures: 0 };
}
