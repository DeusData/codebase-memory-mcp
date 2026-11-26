// Read-only browser audit: real baseline and current frontend, same current index.
import {chromium} from 'playwright';
import {writeFile,mkdir} from 'node:fs/promises';
const out='verification/pr-2068';await mkdir(out,{recursive:true});
const report={startedAt:new Date().toISOString(),project:'cbm-pr2068',viewport:{width:1440,height:1000},comparability:'Different frontend revisions; same current daemon/index. No storage seeding or fixtures.',versions:[]};
const browser=await chromium.launch({headless:true,args:['--use-angle=swiftshader']});
try{for(const [label,port] of [['before',9751],['after',9749]]){
 const c=await browser.newContext({viewport:report.viewport,reducedMotion:'reduce'});const p=await c.newPage();
 const v={label,port,actions:[],stages:[],pageErrors:[],failedRequests:[]};report.versions.push(v);
 p.on('pageerror',e=>v.pageErrors.push(e.message));p.on('requestfailed',r=>v.failedRequests.push({url:r.url(),error:r.failure()}));
 async function stage(name){
  const screenshot=`audit-${label}-${name}.png`;await p.screenshot({path:`${out}/${screenshot}`,fullPage:false});
  const state=await p.evaluate(()=>({url:location.href,workspace:document.querySelector('[data-workspace]')?.getAttribute('data-workspace'),text:document.body.innerText,headings:[...document.querySelectorAll('h1,h2,h3')].map(e=>({text:e.textContent,y:Math.round(e.getBoundingClientRect().y)})),landmarks:[...document.querySelectorAll('[data-testid="repository-map"],.repo-map-selection,.selection-context,.selection-impact,.repo-map-evidence')].map(e=>({type:e.className,y:Math.round(e.getBoundingClientRect().y),height:Math.round(e.getBoundingClientRect().height)}))}));
  v.stages.push({name,screenshot,at:new Date().toISOString(),...state});
 }
 async function click(locator,description){const bounds=await locator.boundingBox();v.actions.push({description,boundsBefore:bounds,requiresScroll:bounds ? bounds.y<0||bounds.y+bounds.height>report.viewport.height : null});await locator.click();}
 try{
 await p.goto(`http://127.0.0.1:${port}/?project=cbm-pr2068`,{waitUntil:'domcontentloaded'});await p.locator('[data-workspace-tab="architecture"]').waitFor();
 await stage('first-open');
 if(await p.getByRole('button',{name:'Start exploring',exact:true}).isVisible())await click(p.getByRole('button',{name:'Start exploring',exact:true}),'Dismiss standard welcome: Start exploring');
 await click(p.locator('[data-workspace-tab="architecture"]'),'Architecture workspace');
 await click(p.getByRole('button',{name:'Overview',exact:true}),'Architecture Overview');
 if(label==='after')await p.locator('.repo-map-area').first().waitFor({timeout:60000});else await p.locator('.atlas-arch-module').first().waitFor({timeout:60000});
 await stage('architecture-overview');
 if(label==='after'){
  v.entries=await p.locator('.repo-map-start .repo-map-nodes button').allTextContents();
  v.topAreas=await p.locator('.repo-map-area > strong').allTextContents();
  await click(p.locator('.repo-map-area').filter({hasText:'src/daemon'}),'Connected area: src/daemon');
  await stage('architecture-area');
  await click(p.locator('.repo-map-connections button').filter({hasText:'edges · inspect'}).first(),'Inspect first recorded cross-area relationship');
  await stage('relationship-immediate');
  const evidence=p.locator('.repo-map-evidence');await evidence.scrollIntoViewIfNeeded();v.actions.push({description:'Scroll to relationship evidence after click'});await stage('relationship-scrolled');
  const site=evidence.getByRole('button',{name:/^Site :/}).first();
  if(await site.count()){
   await click(site,'Open exact recorded call site');await p.locator('[data-workspace-tab="explore"][aria-selected="true"]').waitFor();await p.getByTestId('selected-code-panel').waitFor();await p.waitForTimeout(1000);await stage('source-from-relationship');
  }
  await click(p.locator('[data-workspace-tab="architecture"]'),'Return to Architecture');
  await click(p.getByRole('button',{name:'Repository',exact:true}),'Repository breadcrumb');
  await click(p.getByRole('button',{name:'Read project README',exact:true}),'Read project README');await p.locator('[data-workspace-tab="explore"][aria-selected="true"]').waitFor();await p.waitForTimeout(1000);await stage('readme-entry');
  await click(p.locator('[data-workspace-tab="architecture"]'),'Return to Architecture');
 }
 for(const tab of ['Dependencies','Entry points','Routes','Hotspots']){
  await click(p.getByRole('button',{name:tab,exact:true}),`Architecture tab: ${tab}`);await p.waitForTimeout(350);await stage(`architecture-${tab.toLowerCase().replaceAll(' ','-')}`);
 }
 await click(p.locator('[data-workspace-tab="galaxy"]'),'Galaxy workspace');await p.waitForTimeout(1300);await stage('galaxy-overview');
 v.completed=true;
 }catch(e){v.failure=String(e);await stage('failure');}
 await c.close();
 await writeFile(`${out}/audit-architecture.json`,JSON.stringify(report,null,2)+'\n');
 }}finally{report.finishedAt=new Date().toISOString();await writeFile(`${out}/audit-architecture.json`,JSON.stringify(report,null,2)+'\n');await browser.close();}
console.log(JSON.stringify(report.versions.map(v=>({label:v.label,completed:v.completed,failure:v.failure,stages:v.stages.map(s=>s.name),pageErrors:v.pageErrors,actions:v.actions})),null,2));
