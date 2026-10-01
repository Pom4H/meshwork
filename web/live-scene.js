const mix=(a,b,u)=>typeof b==='number'?a+(b-a)*u:Array.isArray(b)?b.map((v,i)=>mix(a[i],v,u)):u>=.5?b:a;
const smooth=x=>{x=Math.max(0,Math.min(1,x));return x*x*(3-2*x);};
export function sceneAudioGain(changes,frame){let gain=1;for(const change of changes??[]){if(frame<change.startFrame)break;if(change.values.audioGain!==undefined)gain=mix(gain,change.values.audioGain,smooth((frame-change.startFrame)/change.transitionFrames));}return gain;}
export function sceneSettings(base,changes,frame,fps){
 let values={rendering:{...base.rendering},snowfall:base.environment?.snowfall??'none',wind:base.environment?.wind??[0,0,0],lightScale:1,audioGain:1,cameraSpeed:1,entities:[]},cameraTime=frame/fps;
 for(const change of changes??[]){
  if(frame<change.startFrame)break;
  const elapsed=(frame-change.startFrame)/fps,duration=change.transitionFrames/fps,u=smooth(elapsed/duration),target=change.values;
  if(target.cameraSpeed!==undefined){
   const a=values.cameraSpeed,b=target.cameraSpeed,q=Math.min(elapsed,duration)/duration;
   const ramp=duration*(q*q*q-.5*q*q*q*q);
   cameraTime+=(b-a)*(ramp+Math.max(0,elapsed-duration));
  }
  for(const key of ['lightScale','audioGain','cameraSpeed','wind','snowfall'])if(target[key]!==undefined)values[key]=mix(values[key],target[key],u);
  for(const [key,value] of Object.entries(target.rendering??{}))values.rendering[key]=mix(values.rendering[key]??value,value,u);
  if(target.entities)values.entities=target.entities;
 }
 return {...values,cameraTime};
}

const bindings=new WeakMap();
export function applyLiveScene(runtime,base,changes,frame,fps,cameraOnRail){
 if(!changes?.length)return;
 const scene=runtime.scene,settings=sceneSettings(base,changes,frame,fps);
 let binding=bindings.get(scene);
 if(!binding){binding={lights:[...scene.entities.values()].filter(e=>e.light).map(e=>[e,e.light.intensity]),glow:[...scene.entities.values()].filter(e=>e.material.emissive).map(e=>[e,e.material.emissive]),added:new Set(),lightScale:1};bindings.set(scene,binding);}
 // Uniform fog transitions do not rebuild geometry or allocate another device.
 Object.assign(scene.spec.rendering,settings.rendering);scene.appearanceRevision++;
 if(settings.lightScale!==binding.lightScale){
  for(const [entity,intensity] of binding.lights)entity.light.intensity=intensity*settings.lightScale;
  for(const [entity,emission] of binding.glow)entity.material=Object.freeze({...entity.material,emissive:emission.map(v=>v*settings.lightScale)});
  binding.lightScale=settings.lightScale;scene.renderStructureRevision++;
 }
 const rates={none:0,light:4e-6,moderate:1.1e-5,heavy:2.2e-5};
 scene.environment={...scene.environment,snowfall:settings.snowfall,snowfallRate:rates[settings.snowfall],wind:settings.wind};
 if(base.cameraRail&&cameraOnRail)scene.camera=cameraOnRail(base.camera,base.cameraRail,settings.cameraTime);
 for(const id of binding.added)if(!settings.entities.some(e=>e.id===id)){scene.remove(id);binding.added.delete(id);}
 for(const entity of settings.entities)if(!binding.added.has(entity.id)){
  scene.add(entity);binding.added.add(entity.id);
 }
}
