import {test,expect} from 'bun:test';
import {startBroker} from './broker';
import {mkdir,rm} from 'node:fs/promises';
test('chat patches future segments without epoch resets and reactions/history persist',async()=>{
 const directory='.meshwork-test-chat-'+crypto.randomUUID();await mkdir(directory,{recursive:true});
 const snapshot:any={spec:{id:'chat-test',capability:'render.video.segment.v1',width:1280,height:720,fps:60,codec:'avc',segmentFrames:120,warmupFrames:24},currentEpoch:{streamId:'chat-test',id:0,startFrame:0,sceneHash:'a'.repeat(64),renderHash:'b'.repeat(64),createdAt:0},nextFrame:0,status:'active',createdAt:0,updatedAt:0};
 const broker=startBroker({port:0,artifactDir:directory+'/artifacts',chatDir:directory+'/chat',initialStreams:[snapshot],director:async text=>({reply:'Подготовил более прозрачный туман.',patch:{rendering:{fogDensity:.17}}})});
 const base=`http://127.0.0.1:${broker.port}/streams/chat-test`;
 try{
  const first=await fetch(base).then(r=>r.json() as Promise<any>);
  const response=await fetch(base+'/chat',{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({name:'Roman',text:'Ослабь туман',clientId:'qa'})});
  expect(response.status).toBe(202);
  for(let i=0;i<30;i++){const room=await fetch(base+'/chat').then(r=>r.json() as Promise<any>);if(!room.busy)break;await Bun.sleep(10);}
  const after=await fetch(base).then(r=>r.json() as Promise<any>);
  expect(after.currentEpoch.id).toBe(first.currentEpoch.id);
  expect(after.sceneChanges[0].values.rendering.fogDensity).toBe(.17);
  expect(after.sceneChanges[0].startFrame).toBe(first.nextFrame);
  const room=await fetch(base+'/chat').then(r=>r.json() as Promise<any>);
  expect(room.messages.at(-1).role).toBe('codex');expect(room.messages.at(-1).status).toBe('queued');
  const reaction=await fetch(base+'/reactions',{method:'POST',body:JSON.stringify({emoji:'🐎',clientId:'qa'})}).then(r=>r.json() as Promise<any>);
  expect(reaction.reactions['🐎']).toBe(1);
  const cross=await fetch(base+'/chat',{method:'POST',headers:{origin:'https://example.com'},body:'{}'});expect(cross.status).toBe(403);
  const rate=await fetch(base+'/chat',{method:'POST',body:JSON.stringify({name:'Roman',text:'ещё',clientId:'qa'})});expect(rate.status).toBe(429);
  await Bun.sleep(40);
  const saved=await Bun.file(directory+'/chat/chat-test.json').json();expect(saved.reactions['🐎']).toBe(1);
 }finally{broker.stop(true);await Bun.sleep(30);await rm(directory,{recursive:true,force:true});}
});
