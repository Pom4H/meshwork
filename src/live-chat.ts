import {existsSync,readFileSync} from 'node:fs';
import {mkdir} from 'node:fs/promises';
import {join} from 'node:path';
import {validateSettings,type SceneDirector,type SceneChange} from './director';
type Message={id:string;role:'viewer'|'codex'|'system';name:string;text:string;createdAt:number;startFrame?:number;revision?:number;status?:string};
type Room={messages:Message[];reactions:Record<string,number>;sequence:number;pending:number;busy:boolean;queue:Promise<void>;persist:Promise<unknown>;clients:Set<ReadableStreamDefaultController<Uint8Array>>};
export class LiveChat{
 private rooms=new Map<string,Room>();private encoder=new TextEncoder();private stopped=false;
 private heartbeat=setInterval(()=>{for(const room of this.rooms.values())for(const client of room.clients)try{client.enqueue(this.encoder.encode(': ping\n\n'));}catch{room.clients.delete(client);}},10000);
 constructor(private directory:string,private director:SceneDirector|undefined,private context:(id:string)=>unknown,private apply:(id:string,patch:any)=>SceneChange){}
 private room(id:string){
  let room=this.rooms.get(id);if(room)return room;
  const file=join(this.directory,id+'.json');let saved:any;
  try{if(existsSync(file))saved=JSON.parse(readFileSync(file,'utf8'));}catch{}
  room={messages:saved?.messages?.slice(-100)??[],reactions:saved?.reactions??{},sequence:saved?.sequence??0,pending:0,busy:false,queue:Promise.resolve(),persist:Promise.resolve(),clients:new Set()};this.rooms.set(id,room);return room;
 }
 snapshot(id:string){const r=this.room(id);return {messages:r.messages,reactions:r.reactions,sequence:r.sequence,busy:r.busy,agentEnabled:!!this.director};}
 private publish(id:string){
  const room=this.room(id);room.sequence++;const value=JSON.stringify(this.snapshot(id));
  for(const client of room.clients)try{client.enqueue(this.encoder.encode('data: '+value+'\n\n'));}catch{room.clients.delete(client);}
  room.persist=room.persist.catch(()=>{}).then(async()=>{await mkdir(this.directory,{recursive:true});await Bun.write(join(this.directory,id+'.json'),value);});
 }
 private message(id:string,value:Omit<Message,'id'|'createdAt'>){const r=this.room(id);const m={...value,id:crypto.randomUUID(),createdAt:Date.now()};r.messages.push(m);r.messages=r.messages.slice(-100);this.publish(id);return m;}
 reaction(id:string,emoji:string){if(!['❤️','🔥','👏','❄️','🐎'].includes(emoji))throw Error('Unsupported reaction');const r=this.room(id);r.reactions[emoji]=(r.reactions[emoji]??0)+1;this.publish(id);return this.snapshot(id);}
 submit(id:string,name:string,text:string){
  if(!text.trim()||text.length>1000||!name.trim()||name.length>40)throw Error('Invalid chat message');
  const r=this.room(id);if(r.pending>=8)throw Error('Codex queue is full');
  this.message(id,{role:'viewer',name,text:text.trim()});
  if(!this.director){this.message(id,{role:'system',name:'Meshwork',text:'Codex сейчас не подключён.'});return this.snapshot(id);}
  r.pending++;r.busy=true;this.publish(id);
  r.queue=r.queue.then(async()=>{
   if(this.stopped)return;
   try{
    const answer=await this.director!(text,await this.context(id));
    const patch=validateSettings(answer.patch);
    if(Object.keys(patch).length){
     const change=this.apply(id,patch);
     this.message(id,{role:'codex',name:'Codex',text:answer.reply,startFrame:change.startFrame,revision:change.revision,status:'queued'});
    }else this.message(id,{role:'codex',name:'Codex',text:answer.reply});
   }catch{this.message(id,{role:'system',name:'Meshwork',text:'Не удалось выполнить пожелание: Codex недоступен или изменение не прошло проверку. Сцена продолжает работать.'});}
   finally{r.pending--;r.busy=r.pending>0;this.publish(id);}
  });return this.snapshot(id);
 }
 events(id:string,signal:AbortSignal){
  const r=this.room(id);let client:ReadableStreamDefaultController<Uint8Array>;
  const body=new ReadableStream<Uint8Array>({start:(c:ReadableStreamDefaultController<Uint8Array>)=>{client=c;r.clients.add(c);c.enqueue(this.encoder.encode('data: '+JSON.stringify(this.snapshot(id))+'\n\n'));signal.addEventListener('abort',()=>{r.clients.delete(c);try{c.close();}catch{}},{once:true});},cancel:()=>{r.clients.delete(client);}});
  return new Response(body,{headers:{'content-type':'text/event-stream','cache-control':'no-cache','x-accel-buffering':'no'}});
 }
 close(){this.stopped=true;clearInterval(this.heartbeat);for(const r of this.rooms.values())for(const c of r.clients)try{c.close();}catch{}}
}
