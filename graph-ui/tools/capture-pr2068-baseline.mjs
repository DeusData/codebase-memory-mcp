import { chromium } from 'playwright';
import { mkdir,writeFile } from 'node:fs/promises';
const out='verification/pr-2068'; await mkdir(out,{recursive:true});
const browser=await chromium.launch({headless:true});
const page=await browser.newPage({viewport:{width:1600,height:1060}});
const consoleErrors=[]; page.on('pageerror',e=>consoleErrors.push(e.message));
await page.goto('http://127.0.0.1:9749/?project=cbm-pr2068');
await page.evaluate(()=>{localStorage.setItem('cbm.workspace.setup','done');localStorage.setItem('cbm.workspace','architecture')});
await page.reload();
await page.waitForTimeout(7000);
await page.screenshot({path:out+'/before-architecture.png',fullPage:true});
await writeFile(out+'/before-dom.txt',await page.locator('body').innerText());
for (const [view,name] of [['system','before-system'],['galaxy','before-galaxy'],['agents','before-agents']]) {
 await page.locator(`[data-workspace-tab="${view}"]`).click();
 await page.waitForTimeout(2000); await page.screenshot({path:`${out}/${name}.png`,fullPage:true});
}
await writeFile(out+'/before-browser.json',JSON.stringify({url:page.url(),consoleErrors,revision:'80f4f41f9bcb9daf60afc5f607bcbcba24a4b196',data:'Real repository indexed through POST /api/index',port:9749},null,2));
await browser.close();
