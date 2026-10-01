import {test,expect} from 'bun:test';
import {validateSettings} from './director';
import {sceneSettings,sceneAudioGain} from '../web/live-scene.js';
test('director accepts only finite scene data, never scripts, paths or arbitrary meshes',()=>{
 expect(validateSettings({rendering:{fogDensity:.17},snowfall:'heavy',lightScale:.8})).toEqual({rendering:{fogDensity:.17},snowfall:'heavy',lightScale:.8});
 for(const value of [{command:'rm'},{rendering:{fogDensity:NaN}},{rendering:{fogDensity:1}},{entities:[{id:'../../escape'}]},{wind:[0,0,Infinity]}])expect(()=>validateSettings(value)).toThrow();
 expect(validateSettings({entities:[{id:'chat-bench',position:[-8,.6,-3.5],mesh:{kind:'box',size:[2,.1,.5]},material:{color:[.2,.1,.06,1],roughness:.8}}]}).entities).toHaveLength(1);
});
test('absolute frame transitions agree across workers and camera speed has no position jump',()=>{
 const base={rendering:{fogDensity:.2,exposure:.96},environment:{snowfall:'light'}};
 const changes=[{revision:1,startFrame:120,transitionFrames:60,values:{rendering:{fogDensity:.1},audioGain:0,cameraSpeed:2}}];
 expect(sceneSettings(base,changes,119,60).rendering.fogDensity).toBe(.2);
 expect(sceneSettings(base,changes,120,60).cameraTime).toBe(2);
 expect(sceneSettings(base,changes,150,60).rendering.fogDensity).toBeCloseTo(.15);
 expect(sceneSettings(base,changes,180,60).cameraTime).toBeCloseTo(3.5);
 expect(sceneSettings(base,changes,240,60).cameraTime).toBeCloseTo(5.5);
 expect(sceneAudioGain(changes,150)).toBeCloseTo(.5);
 expect(sceneAudioGain(changes,180)).toBe(0);
 expect(sceneSettings(base,structuredClone(changes),150,60)).toEqual(sceneSettings(base,changes,150,60));
});
