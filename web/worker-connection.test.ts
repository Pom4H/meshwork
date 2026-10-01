import {test,expect} from 'bun:test';
import {workerConnection} from './worker-connection.js';

function harness() {
  let time = 0, seq = 0;
  const jobs = new Map<number,{at:number,fn:()=>void,interval?:number}>();
  const sockets: FakeSocket[] = [];
  class FakeSocket extends EventTarget {
    static OPEN = 1;
    readyState = 0;
    sent: any[] = [];
    constructor(_:any) { super(); sockets.push(this); }
    send(value:string) {this.sent.push(JSON.parse(value));}
    close(code=1000,reason='') {this.readyState=3;this.dispatchEvent(Object.assign(new Event('close'),{code,reason}));}
    open() {this.readyState=1;this.dispatchEvent(new Event('open'));}
    message(value:any) {this.dispatchEvent(Object.assign(new Event('message'),{data:JSON.stringify(value)}));}
  }
  const timers = {
    setTimeout(fn:()=>void,ms:number) {const id=++seq;jobs.set(id,{at:time+ms,fn});return id;},
    clearTimeout(id:number) {jobs.delete(id);},
    setInterval(fn:()=>void,ms:number) {const id=++seq;jobs.set(id,{at:time+ms,fn,interval:ms});return id;},
    clearInterval(id:number) {jobs.delete(id);},
  };
  const events:any[]=[];
  let active = {taskId:'task-a',attemptId:'attempt-a'};
  const connection = workerConnection({url:'ws://test',Socket:FakeSocket,timers,now:()=>time,
    hello:()=>({type:'worker.hello'}),heartbeat:()=>({type:'worker.heartbeat',...active}),
    onDisconnect:(e:any)=>events.push(['close',e.code,e.reason]),
    onRetry:(delay:number)=>events.push(['retry',delay]),onFatal:()=>events.push(['fatal']),
    onMessage:(e:any)=>{if(JSON.parse(e.data).type==='worker.ping')connection.heartbeat(true);else events.push(['message']);},
  });
  const tick=(ms:number)=>{
    const end=time+ms;
    while(true){const next=[...jobs].filter(([,v])=>v.at<=end).sort((a,b)=>a[1].at-b[1].at)[0];if(!next)break;
      const [id,job]=next;time=job.at;if(job.interval)job.at+=job.interval;else jobs.delete(id);job.fn();}
    time=end;
  };
  return {connection,sockets,events,tick,setTime:(value:number)=>time=value,setActive:(value:any)=>active=value};
}

test('server ping renews the current attempt even when browser timers never run',()=>{
  const h=harness(),s=h.sockets[0]!;s.open();
  h.setTime(60000);h.setActive({taskId:'task-b',attemptId:'attempt-b'});
  s.message({type:'worker.ping'});
  expect(s.sent.at(-1)).toEqual({type:'worker.heartbeat',taskId:'task-b',attemptId:'attempt-b'});
  h.connection.stop();
});

test('reconnects with backoff and fences events from the old socket',()=>{
  const h=harness(),first=h.sockets[0]!;first.open();first.close(1012,'heartbeat timeout');
  h.tick(1000);expect(h.sockets).toHaveLength(2);
  const second=h.sockets[1]!;second.open();
  first.message({type:'task.assign'});first.close(1006);
  expect(h.events.filter(e=>e[0]==='message')).toHaveLength(0);
  expect(h.events.filter(e=>e[0]==='close')).toHaveLength(1);
  second.close(1006);expect(h.events.at(-1)).toEqual(['retry',2000]);
  h.connection.resume();expect(h.sockets).toHaveLength(3);
  h.tick(5000);expect(h.sockets).toHaveLength(3);
  h.connection.stop();h.tick(60000);expect(h.sockets).toHaveLength(3);
});

test('protocol rejection stops retries until explicitly restarted',()=>{
  const h=harness();h.sockets[0]!.open();h.sockets[0]!.close(1008,'unauthorized');
  h.tick(60000);h.connection.resume();expect(h.sockets).toHaveLength(1);
  expect(h.events.at(-1)).toEqual(['fatal']);
});

test('GPU recovery uses a close code accepted by browser clients',()=>{
  const h=harness();h.sockets[0]!.open();h.connection.reconnect();
  expect(h.events[0]).toEqual(['close',4000,'worker restarting']);
  h.tick(1000);expect(h.sockets).toHaveLength(2);h.connection.stop();
});

test('a real WebSocket reconnects after broker restart and resumes heartbeats',async()=>{
  let joins=0,closes=0;
  let complete!:()=>void;
  const recovered=new Promise<void>(resolve=>complete=resolve);
  const broker=Bun.serve({port:0,fetch(req,server){
    if(server.upgrade(req))return;return new Response('test');
  },websocket:{message(socket,raw){
    const m=JSON.parse(String(raw));
    if(m.type==='worker.hello'){
      joins++;
      if(joins===1)socket.close(1012,'broker restarting');
      else socket.send(JSON.stringify({type:'worker.ping'}));
    }else if(m.type==='worker.heartbeat'&&joins>=2)complete();
  }}});
  const connection=workerConnection({url:`ws://127.0.0.1:${broker.port}/ws`,
    hello:()=>({type:'worker.hello'}),heartbeat:()=>({type:'worker.heartbeat'}),
    onDisconnect:()=>closes++,onMessage:()=>connection.heartbeat(true),
  });
  try{
    await Promise.race([recovered,Bun.sleep(3000).then(()=>{throw Error('Reconnect timed out');})]);
    expect(joins).toBe(2);expect(closes).toBe(1);
  }finally{connection.stop();broker.stop(true);}
});
