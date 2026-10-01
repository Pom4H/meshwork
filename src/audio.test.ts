import {test,expect} from 'bun:test';
import {audioFrameRange,adtsPackets} from './audio';
test('AAC ranges have no gaps or duplicate packets at independently rendered boundaries',()=>{
 let previous=0,total=0;
 for(let frame=0;frame<36000;frame+=120){const range=audioFrameRange(frame,120,60);expect(range.first).toBe(previous);expect(range.end-range.first).toBeGreaterThanOrEqual(93);previous=range.end;total+=range.end-range.first;}
 expect(total).toBe(Math.ceil(600*48000/1024));
 expect(audioFrameRange(1080,120,60).first).toBe(Math.ceil(18*48000/1024));
 expect(()=>adtsPackets(new Uint8Array([255,0,0,0,0,0,0]))).toThrow();
});
