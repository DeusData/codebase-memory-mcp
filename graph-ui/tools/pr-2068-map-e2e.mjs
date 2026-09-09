#!/usr/bin/env node
// Real repository, real embedded daemon. No fixture graph or mocked API.
import assert from 'node:assert/strict';
import { writeFile, mkdir } from 'node:fs/promises';
import { chromium } from 'playwright';
const origin='http://127.0.0.1:9749';
const out='verification/pr-2068';
await mkdir(out,{recursive:true});
const report={origin,project:'cbm-pr2068',startedAt:new Date().toISOString(),screenshots:[],checks:{},pageErrors:[],externalRequests:[],rpc:[]};
const browser=await chromium.launch({headless:true});
const context=await browser.newContext({viewport:{width:1600,height:1060}});
await context.addInitScript(()=>{localStorage.setItem('cbm.workspace.setup','done');localStorage.setItem('cbm.workspace','architecture');});
const page=await context.newPage();
page.on('pageerror',error=>report.pageErrors.push(error.message));
page.on('request',request=>{
 const url=request.url(); if(/^https?:/.test(url)&&!url.startsWith(origin+'/')) report.externalRequests.push(url);
 if(url===origin+'/rpc') {try{const body=request.postDataJSON();report.rpc.push({method:body.method,tool:body.params?.name,arguments:body.params?.arguments});}catch{}}
});
const capture=async(name,locator)=>{
 if(locator) await locator.scrollIntoViewIfNeeded();
 await page.screenshot({path:`${out}/${name}`,fullPage:false});report.screenshots.push({file:name,at:new Date().toISOString()});
};
try {
 await page.goto(origin+'/?project=cbm-pr2068');
 await page.locator('.repo-map-area').first().waitFor({timeout:60000});
 await capture('after-architecture.png');
 report.checks.realMap=await page.getByTestId('repository-map').innerText();
 assert.ok(report.checks.realMap.includes('src/daemon'));
 assert.ok(await page.getByRole('button',{name:'Read project README',exact:true}).count());
 await page.getByRole('button',{name:'Read project README',exact:true}).click();
 await page.locator('[data-workspace-tab="explore"][aria-selected="true"]').waitFor();
 await page.waitForTimeout(1200);
 report.checks.readme=await page.getByTestId('selected-code-panel').innerText();
 assert.ok(report.checks.readme.toLowerCase().includes('readme.md'));
 await capture('after-project-readme.png');
 await page.locator('[data-workspace-tab="architecture"]').click();
 await page.locator('.repo-map-area').first().waitFor();
 await page.locator('.repo-map-area').filter({hasText:'src/daemon'}).click();
 await page.locator('.repo-map-connections').waitFor();
 report.checks.area=await page.getByTestId('repository-map').innerText();
 assert.ok(report.checks.area.includes('Used by'));
 await page.locator('.repo-map-connections button').filter({hasText:'edges · inspect'}).first().click();
 const evidence=page.locator('.repo-map-evidence').first();
 await capture('after-architecture-relationship.png',evidence);
 report.checks.relationship=await evidence.innerText();
 assert.match(report.checks.relationship,/CALLS|IMPORTS|USAGE/);
 assert.match(report.checks.relationship,/Edge #\d+/);
 await evidence.locator('.repo-map-nodes button').first().click();
 // Assess a genuinely modified production file, not just an arbitrary symbol.
 await page.locator('.repo-map-files button').filter({hasText:'src/daemon/application.c'}).click();
 const symbols=page.getByRole('heading',{name:'Symbols in src/daemon/application.c',exact:true}).locator('..');
 await symbols.locator('.repo-map-nodes button').first().click();
 const selected=page.locator('.repo-map-selection');
 await selected.waitFor();
 await selected.getByTestId('selection-impact').locator('[data-level]').waitFor({timeout:60000});
 report.checks.selected=await selected.innerText();
 assert.ok(report.checks.selected.toLowerCase().includes('graph evidence'));
 assert.ok(report.checks.selected.toLowerCase().includes('observed agent activity'));
 assert.ok(report.checks.selected.includes('Historical evidence'));
 assert.ok(report.checks.selected.includes('Git HEAD'));
 assert.ok(report.checks.selected.includes('uncommitted changes'));
 report.checks.realUncommittedChange='src/daemon/application.c';
 await capture('after-selection-context.png',selected.locator('.selection-context'));
 await capture('after-context-and-impact.png',selected.locator('.selection-impact'));
 const subject=(await selected.locator('.repo-map-selection-heading h2').textContent()).trim();
 const source=await selected.locator('.repo-map-selection-heading button').innerText();
 report.checks.sourceTarget={subject,source};
 await selected.locator('.repo-map-selection-heading button').click();
 await page.locator('[data-workspace-tab="explore"][aria-selected="true"]').waitFor();
 await page.getByTestId('selected-code-panel').filter({visible:true}).waitFor();
 await page.waitForTimeout(1500);
 report.checks.reader=await page.getByTestId('selected-code-panel').innerText();
 assert.ok(report.checks.reader.includes(subject));
 assert.ok(report.rpc.some(call=>call.tool==='get_code_snippet'));
 await capture('after-source-evidence.png');
 // Selection stays available when returning to the map.
 await page.locator('[data-workspace-tab="architecture"]').click();
 await page.locator('.repo-map-selection').waitFor();
 assert.ok((await page.locator('.repo-map-selection-heading h2').textContent()).trim()===subject);
 report.checks.selectionSurvivesWorkspaceNavigation=true;
 assert.deepEqual(report.pageErrors,[]);
 assert.deepEqual(report.externalRequests,[]);
 report.success=true;
} catch(error) {
 report.success=false;report.error=String(error);await capture('map-e2e-failure.png');
 await writeFile(`${out}/map-e2e-failure-dom.txt`,await page.locator('body').innerText());
 throw error;
} finally {
 await writeFile(`${out}/map-e2e.json`,JSON.stringify(report,null,2)+'\n');
 await browser.close();
}
