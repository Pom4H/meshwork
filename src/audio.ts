import {spawn} from 'node:child_process';
import {pathToFileURL} from 'node:url';
import {resolve} from 'node:path';
import {Input,BufferSource,MP4,EncodedPacketSink,Output,BufferTarget,Mp4OutputFormat,EncodedVideoPacketSource,EncodedAudioPacketSource,EncodedPacket} from 'mediabunny';
import {readScene} from './scenes';
import {splitMp4} from './playback';
import type {RenderSegmentInput} from './protocol';
import {sceneAudioGain} from '../web/live-scene.js';

/** One global 1024-sample AAC grid; adjacent video segments own disjoint
 * packets. A silent priming packet must never create a new gap every 2 s. */
export function audioFrameRange(startFrame:number,frameCount:number,fps:number){
 return {first:Math.max(0,Math.ceil(startFrame*48000/(fps*1024)-1e-9)),end:Math.ceil((startFrame+frameCount)*48000/(fps*1024)-1e-9)};
}
export function adtsPackets(bytes:Uint8Array){
 const packets:Uint8Array[]=[];
 for(let offset=0;offset<bytes.length;){
  if(offset+7>bytes.length||bytes[offset]!==255||(bytes[offset+1]!&0xf6)!==0xf0)throw Error('Invalid AAC frame');
  const header=bytes[offset+1]!&1?7:9,length=(bytes[offset+3]!&3)*2048+bytes[offset+4]!*8+(bytes[offset+5]!>>5);
  if(length<header||offset+length>bytes.length||(bytes[offset+2]!>>2&15)!==3||(bytes[offset+3]!>>6|(bytes[offset+2]!&1)<<2)!==2)throw Error('Expected AAC stereo at 48 kHz');
  packets.push(bytes.subarray(offset+header,offset+length));offset+=length;
 }return packets;
}
async function encodeAac(pcm:Float32Array){
 return await new Promise<Uint8Array>((done,fail)=>{
  const child=spawn(process.env.FFMPEG_PATH??'ffmpeg',['-v','error','-f','f32le','-ar','48000','-ac','2','-i','pipe:0','-c:a','aac','-b:a','128k','-f','adts','pipe:1'],{windowsHide:true,stdio:['pipe','pipe','pipe']});
  let error='';const chunks:Buffer[]=[];
  child.stdout.on('data',data=>chunks.push(data));child.stderr.on('data',data=>{error=(error+data.toString()).slice(-2000);});
  const timer=setTimeout(()=>{child.kill();fail(Error('Soundtrack encoder timed out'));},20000);
  child.on('error',e=>{clearTimeout(timer);fail(e);});
  child.on('close',code=>{clearTimeout(timer);code===0?done(Buffer.concat(chunks)):fail(Error('Soundtrack encoder: '+error));});
  child.stdin.on('error',e=>{clearTimeout(timer);child.kill();fail(e);});child.stdin.end(Buffer.from(pcm.buffer,pcm.byteOffset,pcm.byteLength));
 });
}
/** GPU workers encode video once; the broker adds the shared deterministic
 * soundtrack without decoding or re-encoding a single video frame. */
export async function addSceneAudio(bytes:Uint8Array,input:RenderSegmentInput,sceneDir:string,_taskDir:string){
 const scene=await readScene(sceneDir,input.sceneHash);if(!scene?.audioEntry)return bytes;
 const {renderAudio}=await import(pathToFileURL(resolve(sceneDir,input.sceneHash,scene.audioEntry)).href);
 if(typeof renderAudio!=='function')throw Error('Scene audio entry must export renderAudio');
 const {first,end}=audioFrameRange(input.startFrame,input.frameCount,input.fps),sampleStart=(first-1)*1024,samples=(end-first+1)*1024;
 const pcm=new Float32Array(samples*2),skip=Math.max(0,-sampleStart);
 const rendered=renderAudio({startTime:Math.max(0,sampleStart)/48000,duration:(samples-skip)/48000,sampleRate:48000});
 if(!(rendered instanceof Float32Array)||rendered.length!==(samples-skip)*2||rendered.some(v=>!Number.isFinite(v)||Math.abs(v)>1))throw Error('Invalid scene stereo PCM');
 pcm.set(rendered,skip*2);
 if(input.sceneChanges?.length)for(let i=skip;i<samples;i++){
  const gain=sceneAudioGain(input.sceneChanges,(sampleStart+i)/48000*input.fps);pcm[i*2]!*=gain;pcm[i*2+1]!*=gain;
 }
 const audio=adtsPackets(await encodeAac(pcm)).slice(2,2+end-first);
 if(audio.length!==end-first)throw Error('AAC encoder emitted an incomplete range');
 const source=new Input({source:new BufferSource(bytes),formats:[MP4]}),track=await source.getPrimaryVideoTrack();
 if(!track)throw Error('Missing video track');
 const video=new EncodedVideoPacketSource(input.codec as 'avc'|'hevc'),sound=new EncodedAudioPacketSource('aac');
 const output=new Output({target:new BufferTarget(),format:new Mp4OutputFormat({fastStart:'fragmented',minimumFragmentDuration:input.frameCount/input.fps})});
 output.addVideoTrack(video,{frameRate:input.fps});output.addAudioTrack(sound);
 try{
  await output.start();const config=await track.getDecoderConfig();if(!config)throw Error('Missing video decoder config');
  await Promise.all([
   (async()=>{for await(const packet of new EncodedPacketSink(track).packets())await video.add(packet,{decoderConfig:config});video.close();})(),
   (async()=>{for(let i=0;i<audio.length;i++)await sound.add(new EncodedPacket(audio[i]!,'key',(first+i)*1024/48000,1024/48000),{decoderConfig:{codec:'mp4a.40.2',sampleRate:48000,numberOfChannels:2,description:new Uint8Array([0x11,0x90])}});sound.close();})(),
  ]);
  await output.finalize();const result=new Uint8Array(output.target.buffer!);splitMp4(result);
  if(result.byteLength>32*1024*1024)throw Error('Audio/video artifact exceeds 32 MiB');return result;
 }finally{source.dispose();if(output.state!=='finalized'&&output.state!=='canceled')await output.cancel();}
}
