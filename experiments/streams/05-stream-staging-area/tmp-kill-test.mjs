import { newWebSocketRpcSession } from "capnweb";
const CDP="http://localhost:9444", BASE="http://localhost:4173";
const path=`/kill-${Date.now()}`, url=`${BASE}/streams${path}`;
const sleep=ms=>new Promise(r=>setTimeout(r,ms));
const FACTS=`(()=>{const f={};document.querySelectorAll(".stream-page__facts > div").forEach(d=>{const k=d.querySelector("dt")?.textContent?.trim();const v=d.querySelector("dd output, dd")?.textContent?.trim();if(k)f[k]=v;});return JSON.stringify({status:f.Status,events:f.Events,recv:globalThis.__receivedEventCount??null});})()`;
async function newTab(u){return (await fetch(`${CDP}/json/new?${encodeURIComponent(u)}`,{method:"PUT"})).json();}
function cdp(wsUrl){const ws=new WebSocket(wsUrl);let id=0;const w=new Map();const ready=new Promise((res,rej)=>{ws.addEventListener("open",res);ws.addEventListener("error",rej);});ws.addEventListener("message",m=>{const x=JSON.parse(m.data);if(w.has(x.id)){w.get(x.id)(x.result);w.delete(x.id);}});return{ready,ev:expr=>{const i=++id;ws.send(JSON.stringify({id:i,method:"Runtime.evaluate",params:{expression:expr,returnByValue:true,awaitPromise:true}}));return new Promise(r=>w.set(i,r)).then(r=>r?.result?.value);}};}
async function rpc(){const u=new URL(BASE);u.pathname=`/stream/${encodeURIComponent(path)}`;u.protocol="ws:";const ws=new WebSocket(u.toString());await new Promise((res,rej)=>{ws.addEventListener("open",res);ws.addEventListener("error",rej);});return {r:newWebSocketRpcSession(ws),ws};}

const N=8000;
const t=await newTab(url); const c=cdp(t.webSocketDebuggerUrl); await c.ready; await sleep(3000);
const {r,ws}=await rpc();
console.log(`appending ${N} events...`);
await r.appendBatch({events:Array.from({length:N},(_,i)=>({type:"events.iterate.com/debug/random-event",payload:{i}}))});
for(let i=0;i<40;i++){await sleep(1000);const f=JSON.parse(await c.ev(FACTS));if(Number(f.events)>=N+2){console.log("written:",JSON.stringify(f));break;}}
const before=JSON.parse(await c.ev(FACTS));
console.log("before kill:",JSON.stringify(before));
console.log("KILL -> measuring time until recv increases (woken after reconnect)...");
const killStart=Date.now();
await r.kill().catch(()=>{}); // DO aborts; our socket dies
let delta=null;
for(let i=0;i<60;i++){await sleep(150);const v=await c.ev(FACTS);if(v){const f=JSON.parse(v);if(f.recv!=null&&Number(f.recv)>Number(before.recv)){delta={ms:Date.now()-killStart,recvBefore:before.recv,recvAfter:f.recv,events:f.events};break;}}}
console.log("RESULT:",JSON.stringify(delta??"no recv change in 9s"));
ws.close?.();
