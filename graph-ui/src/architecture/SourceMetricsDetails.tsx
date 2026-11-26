import type { SourceCatalog, SourceMeasure } from './source-metrics';

export default function SourceMetricsDetails({ measure, catalog }: { measure: SourceMeasure; catalog: SourceCatalog }) {
    const share = measure.lines !== undefined && catalog.total.lines ? Math.min(100, measure.lines / catalog.total.lines * 100) : undefined;
    const denominator = measure.lines || measure.files || 1;
    return <div className="spatial-source-metrics" aria-label="Source size and languages">
        <div className="spatial-source-total"><strong>{measure.lines === undefined ? 'Unknown' : `${measure.measuredFiles < measure.files ? '≥ ' : ''}${measure.lines.toLocaleString()}`}</strong><span>indexed source lines</span></div>
        <small>Includes comments and blank lines. Each measured file counts once.</small>
        {share !== undefined && <div className="spatial-source-share"><span>{share.toFixed(1)}% of measured project lines</span><div role="meter" aria-label="Share of measured project lines" aria-valuenow={share} aria-valuemin={0} aria-valuemax={100}><i style={{ width: `${share}%` }} /></div></div>}
        <small>{measure.measuredFiles} / {measure.files} files measured{measure.measuredFiles < measure.files ? ' · remaining sizes unknown' : ''}</small>
        <h4>Language mix <span>· {measure.lines ? 'measured lines' : 'files'}</span></h4>
        <div className="spatial-language-bar" aria-hidden="true">{measure.languages.map(language => <i key={language.name} style={{ background: language.color, flex: measure.lines ? language.lines : language.files }} />)}</div>
        <ul className="spatial-language-breakdown">{measure.languages.map(language => <li key={language.name}><i style={{ background: language.color }} /><span>{language.name}</span><span>{((measure.lines ? language.lines : language.files) / denominator * 100).toFixed(1)}%</span></li>)}</ul>
        <small>Language inferred from file type. A mixed file may contain other languages.</small>
    </div>;
}
