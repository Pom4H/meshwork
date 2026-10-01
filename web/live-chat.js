const id=location.pathname.split('/').at(-1),base=`/streams/${id}`,video=document.querySelector('video');
const messages=document.querySelector('#messages'),agent=document.querySelector('#agent'),form=document.querySelector('#chat-form'),input=document.querySelector('#message'),name=document.querySelector('#name'),error=document.querySelector('#chat-error');
let clientId,storedName;
try{clientId=localStorage.getItem('meshwork-viewer')??crypto.randomUUID();localStorage.setItem('meshwork-viewer',clientId);storedName=localStorage.getItem('meshwork-name');}catch{clientId=crypto.randomUUID();}
name.value=storedName??'Гость';
let room,lastSequence=-1,counts={},connected=false;
const queued=new Map();
function reaction(emoji){const node=document.createElement('div');node.className='reaction';node.textContent=emoji;document.querySelector('.stage').append(node);node.addEventListener('animationend',()=>node.remove(),{once:true});}
function applied(){for(const [small,message] of queued){const fps=window.meshworkStream?.spec.fps??60,time=window.meshworkSourceTime?.()??video.currentTime;if(time>=message.startFrame/fps)small.textContent='Изменение в трансляции';else small.textContent=`В очереди · кадр ${message.startFrame}`;}}
function show(value){
 room=value;
 agent.textContent=!room.agentEnabled?'Codex не подключён':room.busy?'Codex готовит изменение…':'Codex на связи · пожелания меняют сцену';
 for(const button of document.querySelectorAll('[data-emoji]')){
  const emoji=button.dataset.emoji,count=room.reactions[emoji]??0;
  button.querySelector('span').textContent=count;
  if(lastSequence>=0&&count>(counts[emoji]??0))reaction(emoji);
 }
 counts=room.reactions;
 if(room.sequence!==lastSequence){
  const bottom=messages.scrollHeight-messages.scrollTop-messages.clientHeight<60;
  queued.clear();messages.replaceChildren();
  for(const message of room.messages){
   const li=document.createElement('li');li.className=message.role;
   const title=document.createElement('strong');title.textContent=message.name;
   const p=document.createElement('p');p.textContent=message.text;li.append(title,p);
   if(message.startFrame!==undefined){const small=document.createElement('small');li.append(small);queued.set(small,message);}
   messages.append(li);
  }
  if(bottom)messages.scrollTop=messages.scrollHeight;
 }
 lastSequence=room.sequence;applied();
}
async function post(path,body){const response=await fetch(base+'/'+path,{method:'POST',headers:{'content-type':'application/json'},body:JSON.stringify({...body,clientId})});const data=await response.json();if(!response.ok)throw Error(data.error??'Не удалось отправить');show(data);}
form.addEventListener('submit',async event=>{
 event.preventDefault();const text=input.value.trim();if(!text)return;
 const button=form.querySelector('button');button.disabled=true;error.textContent='';
 try{await post('chat',{name:name.value.trim()||'Гость',text});input.value='';try{localStorage.setItem('meshwork-name',name.value);}catch{}}
 catch(e){error.textContent=e.message;}finally{button.disabled=false;}
});
for(const button of document.querySelectorAll('[data-emoji]'))button.addEventListener('click',async()=>{try{await post('reactions',{emoji:button.dataset.emoji});error.textContent='';}catch(e){error.textContent=e.message;}});
const events=new EventSource(base+'/events');
events.onopen=()=>{connected=true;};events.onmessage=event=>{connected=true;show(JSON.parse(event.data));};events.onerror=()=>{connected=false;agent.textContent='Переподключение к чату…';};
async function refresh(){if(connected)return;try{const response=await fetch(base+'/chat');if(response.ok)show(await response.json());}catch{}}
void refresh();setInterval(refresh,3000);video.addEventListener('timeupdate',applied);
window.addEventListener('pagehide',()=>events.close(),{once:true});
