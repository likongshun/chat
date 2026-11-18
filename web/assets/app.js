const $=id=>document.getElementById(id);
const throughputHistory=[];
let previousSamples=null;
let busy=false;
let devices=[];
let selectedDevice="";
let selectedPoint="";
let historyRows=[];
let deviceQuery="";
let devicePage=1;
const devicePageSize=50;

function fmt(n){return Number(n||0).toLocaleString('zh-CN')}
function bytes(n){n=Number(n||0);if(n<1024)return n+' B';const u=['KB','MB','GB','TB'];let i=-1;do{n/=1024;i++}while(n>=1024&&i<u.length-1);return n.toFixed(2)+' '+u[i]}
function pct(n){return (Number(n||0)*100).toFixed(1)+'%'}
function set(id,v){const el=$(id);if(el)el.textContent=v}
function toast(s){const el=$('toast');el.textContent=s;el.classList.add('show');setTimeout(()=>el.classList.remove('show'),2600)}
function parseMetrics(src){const out={};for(const line of src.split(/\r?\n/)){if(!line||line[0]==='#')continue;const m=line.match(/^([a-zA-Z_:][a-zA-Z0-9_:]*)(?:\{([^}]*)\})?\s+([-+\d.eE]+)/);if(!m)continue;let key=m[1];if(m[2]){const sink=(m[2].match(/sink="([^"]+)"/)||[])[1];if(sink)key+='_'+sink}out[key]=Number(m[3])}return out}
function drawThroughput(){const vals=throughputHistory.length?throughputHistory:[0,0];const max=Math.max(1,...vals);const pts=vals.map((v,i)=>[i*720/Math.max(1,vals.length-1),188-(v/max)*160]);const d=pts.map((p,i)=>(i?'L':'M')+p[0].toFixed(1)+' '+p[1].toFixed(1)).join(' ');$('chart-line').setAttribute('d',d);$('chart-area').setAttribute('d',d+' L720 210 L0 210 Z')}
async function refresh(){if(busy)return;busy=true;try{const [mr,hr,rr]=await Promise.all([fetch('/api/metrics',{cache:'no-store'}),fetch('/api/healthz',{cache:'no-store'}),fetch('/api/readyz',{cache:'no-store'})]);if(!mr.ok)throw Error('指标接口 HTTP '+mr.status);const m=parseMetrics(await mr.text());const healthy=hr.ok,ready=rr.ok;set('side-status',healthy?'运行正常':'连接异常');set('collector-health',healthy?'运行中':'不可用');set('wal-health',ready?'可读写':'容量告警');set('delivery-health',m.modbus_delivery_published_total_mqtt!==undefined?'已启用':'等待指标');$('health-dot').style.background=healthy?'#27c58b':'#e35d67';set('health-title',healthy?(ready?'运行正常':'需要关注'):'连接异常');set('health-desc',healthy?'采集服务在线 · '+(ready?'WAL 就绪':'WAL 容量达到限制'):'无法连接采集服务');set('polls',fmt(m.modbus_collector_polls_total));set('samples',fmt(m.modbus_collector_samples_total));set('errors',fmt(m.modbus_collector_poll_errors_total));set('timeouts',fmt(m.modbus_collector_poll_timeouts_total));set('pending',fmt(m.modbus_wal_pending_records));set('oldest','最早 '+(m.modbus_wal_oldest_pending_age_seconds||0).toFixed(0)+' 秒');set('disk-bytes',bytes(m.modbus_wal_disk_bytes));set('disk-ratio',pct(m.modbus_wal_disk_used_ratio));set('fs-ratio',pct(m.modbus_wal_filesystem_used_ratio));set('segments',fmt(m.modbus_wal_segments));set('disk-level',['正常','预警','严重','停止'][Math.min(3,Math.max(0,Math.round(m.modbus_wal_disk_level||0)))]);$('disk-bar').style.width=Math.min(100,(m.modbus_wal_disk_used_ratio||0)*100)+'%';$('disk-bar').style.background=(m.modbus_wal_disk_level||0)>=2?'#e46a70':'';set('mqtt-published',fmt(m.modbus_delivery_published_total_mqtt));set('influx-published',fmt(m.modbus_delivery_published_total_influx));set('mqtt-errors','错误 '+fmt(m.modbus_delivery_publish_errors_total_mqtt));set('influx-errors','错误 '+fmt(m.modbus_delivery_publish_errors_total_influx));set('fsync-p99',((m.modbus_wal_fsync_p99_seconds||0)*1000).toFixed(2)+' ms');set('checkpoint',fmt(m.modbus_wal_checkpoint_sequence));const samples=m.modbus_collector_samples_total||0;if(previousSamples!==null){const rate=Math.max(0,samples-previousSamples);throughputHistory.push(rate);if(throughputHistory.length>24)throughputHistory.shift();set('current-throughput',fmt(rate)+' samples');set('poll-rate',fmt(rate)+' / 5s');drawThroughput()}previousSamples=samples;set('error-rate',m.modbus_collector_polls_total?((m.modbus_collector_poll_errors_total||0)/m.modbus_collector_polls_total*100).toFixed(2)+'% 错误率':'—');const time=new Date();set('updated','更新于 '+time.toLocaleTimeString('zh-CN'));set('footer-time',time.toLocaleString('zh-CN'));}catch(e){set('side-status','连接异常');set('health-title','无法连接');set('health-desc','请确认采集器服务及网络');set('collector-health','不可用');$('health-dot').style.background='#e35d67';toast('监控同步失败：'+e.message)}finally{busy=false}}

function cell(row,value,tag='td',className=''){const el=document.createElement(tag);el.textContent=value??'—';if(className)el.className=className;row.appendChild(el);return el}
function clearBody(id){const body=$(id);body.replaceChildren();return body}
function localTime(value){if(!value)return '暂无采集';const d=new Date(value);return Number.isNaN(d.getTime())?'—':d.toLocaleString('zh-CN')}
async function loadDevices(){try{const response=await fetch('/api/v1/devices',{cache:'no-store'});if(!response.ok)throw Error('HTTP '+response.status);devices=await response.json();renderDevices();const select=$('history-device');select.replaceChildren(new Option('选择设备',''));for(const d of devices)select.add(new Option((d.name||d.id)+' · '+d.id,d.id));if(!selectedDevice&&devices.length)await selectDevice(devices[0].id);else if(selectedDevice&&devices.some(d=>d.id===selectedDevice))await selectDevice(selectedDevice);set('device-count',devices.length+' 台设备');}catch(e){set('device-count','加载失败');const body=clearBody('devices-body');const tr=document.createElement('tr');cell(tr,'设备配置加载失败：'+e.message,'td','empty');body.appendChild(tr)}}
function renderDevices(){
 const body=clearBody('devices-body');
 const q=deviceQuery.toLocaleLowerCase();
 const filtered=devices.filter(d=>[d.id,d.name,d.model,d.host,d.protocol,d.meter_address].some(v=>String(v||'').toLocaleLowerCase().includes(q)));
 const pages=Math.max(1,Math.ceil(filtered.length/devicePageSize));
 devicePage=Math.min(devicePage,pages);
 const visible=filtered.slice((devicePage-1)*devicePageSize,devicePage*devicePageSize);
 set('device-page-info',filtered.length?((devicePage-1)*devicePageSize+1)+'-'+Math.min(devicePage*devicePageSize,filtered.length)+' / '+filtered.length:'0 / 0');
 $('device-prev').disabled=devicePage<=1;
 $('device-next').disabled=devicePage>=pages;
 if(!visible.length){const tr=document.createElement('tr');const empty=cell(tr,devices.length?'没有匹配的设备':'当前配置没有设备','td','empty');empty.colSpan=6;body.appendChild(tr);return}
 for(const d of visible){
  const tr=document.createElement('tr');
  if(d.id===selectedDevice)tr.style.background='#f2f7ff';
  const name=cell(tr);
  const b=document.createElement('button');b.className='table-action';b.textContent=(d.name||d.id)+' · '+d.id;b.addEventListener('click',()=>selectDevice(d.id));name.appendChild(b);
  cell(tr,d.protocol==='dl-t645-2007'?'DL/T645-2007':'Modbus TCP');cell(tr,d.model);cell(tr,d.protocol==='dl-t645-2007'?(d.meter_address||'待发现')+' · '+d.host+':'+d.port:d.host+':'+d.port);cell(tr,fmt(d.point_count));
  const fresh=d.last_seen&&Date.now()-new Date(d.last_seen).getTime()<Math.max(10000,(d.interval_ms||1000)*3);
  cell(tr,d.discovery_only?'待搜索':d.enabled?(fresh?'在线':'等待数据'):'已禁用','td',d.discovery_only?'':d.enabled?(fresh?'state-good':''):'state-bad');
  body.appendChild(tr);
 }
}
async function selectDevice(id){selectedDevice=id;const d=devices.find(x=>x.id===id);set('points-title',(d?.name||id)+' · 测点实时值');renderDevices();await loadPoints();$('history-device').value=id}
async function loadPoints(){const body=clearBody('points-body');if(!selectedDevice){const tr=document.createElement('tr');cell(tr,'请先选择设备','td','empty');body.appendChild(tr);return}try{const response=await fetch('/api/v1/devices/'+encodeURIComponent(selectedDevice)+'/points',{cache:'no-store'});if(!response.ok)throw Error('HTTP '+response.status);const points=await response.json();set('points-count',points.length+' 个测点');if(!points.length){const tr=document.createElement('tr');cell(tr,'该设备型号没有启用测点','td','empty');body.appendChild(tr)}for(const p of points){const tr=document.createElement('tr');const name=cell(tr);name.innerHTML='';const button=document.createElement('button');button.className='table-action';button.textContent=(p.name||p.code)+' · '+p.code;button.addEventListener('click',()=>chooseHistoryPoint(p.code));name.appendChild(button);cell(tr,devices.find(d=>d.id===selectedDevice)?.protocol==='dl-t645-2007'?'DI '+p.data_id:'FC'+p.function+' / '+p.address);cell(tr,p.latest?.value===undefined?'暂无数据':String(p.latest.value));cell(tr,p.unit||'—');const quality=p.latest?.quality||'暂无数据';cell(tr,quality,'td',quality==='good'?'state-good':quality==='暂无数据'?'':'state-bad');cell(tr,p.latest?localTime(p.latest.last_success||p.latest.timestamp):'暂无采集');body.appendChild(tr)}const select=$('history-point');select.replaceChildren(new Option('选择测点',''));for(const p of points)select.add(new Option((p.name||p.code)+' · '+p.code,p.code));if(selectedPoint&&points.some(p=>p.code===selectedPoint))select.value=selectedPoint;else if(points.length){selectedPoint=points[0].code;select.value=selectedPoint}}catch(e){const tr=document.createElement('tr');cell(tr,'测点加载失败：'+e.message,'td','empty');body.appendChild(tr);set('points-count','不可用')}}
function chooseHistoryPoint(code){selectedPoint=code;$('history-point').value=code;$('history-device').value=selectedDevice;queryHistory()}
function drawHistory(rows){const values=rows.map(x=>Number(x.value)).filter(Number.isFinite);if(values.length<2){$('history-line').setAttribute('d','');$('history-area').setAttribute('d','');return}const min=Math.min(...values),max=Math.max(...values),span=max-min||1;const pts=values.map((v,i)=>[i*900/Math.max(1,values.length-1),225-(v-min)/span*195]);const d=pts.map((p,i)=>(i?'L':'M')+p[0].toFixed(1)+' '+p[1].toFixed(1)).join(' ');$('history-line').setAttribute('d',d);$('history-area').setAttribute('d',d+' L900 250 L0 250 Z');set('history-minmax','最小值 '+min+' · 最大值 '+max)}
async function queryHistory(){const device=$('history-device').value,point=$('history-point').value;if(!device||!point){toast('请先选择设备和测点');return}selectedDevice=device;selectedPoint=point;const hours=Number($('history-range').value||24);const to=new Date(),from=new Date(to.getTime()-hours*3600000);const params=new URLSearchParams({device_id:device,point_id:point,from:from.toISOString(),to:to.toISOString(),limit:'1000'});set('history-status','正在查询…');try{const response=await fetch('/api/v1/telemetry?'+params.toString(),{cache:'no-store'});if(!response.ok){let message='HTTP '+response.status;try{message=(await response.text())||message}catch{}throw Error(message)}historyRows=await response.json();renderHistory();set('history-status','查询完成 · '+historyRows.length+' 条')}catch(e){set('history-status','查询失败');const body=clearBody('history-body');const tr=document.createElement('tr');cell(tr,'历史查询失败：'+e.message,'td','empty');body.appendChild(tr);$('history-line').setAttribute('d','');$('history-area').setAttribute('d','')}}
function renderHistory(){const body=clearBody('history-body');set('history-points-count',historyRows.length+' 个数据点');if(!historyRows.length){const tr=document.createElement('tr');cell(tr,'该时间范围内没有历史记录','td','empty');body.appendChild(tr);$('history-line').setAttribute('d','');$('history-area').setAttribute('d','');set('history-minmax','最小值 — · 最大值 —');return}for(const row of historyRows.slice().reverse()){const tr=document.createElement('tr');cell(tr,localTime(row.timestamp));cell(tr,String(row.value));cell(tr,row.quality||'—','td',row.quality==='good'?'state-good':'');body.appendChild(tr)}drawHistory(historyRows)}


async function loadDLT645Templates(){
 const query=$('dlt-template-query').value.trim();
 const body=clearBody('dlt-template-body');
 try{
  const response=await fetch('/api/v1/dlt645/templates'+(query?'?q='+encodeURIComponent(query):''),{cache:'no-store'});
  if(!response.ok)throw Error('HTTP '+response.status);
  const rows=await response.json();set('dlt-template-count',rows.length+' 个模板');
  if(!rows.length){const tr=document.createElement('tr');const td=cell(tr,'没有匹配的数据标识','td','empty');td.colSpan=8;body.appendChild(tr);return}
  for(const item of rows){const tr=document.createElement('tr');cell(tr,item.id);cell(tr,item.name);cell(tr,item.category);cell(tr,item.data_type);cell(tr,item.length+' B');cell(tr,item.scale);cell(tr,item.unit);cell(tr,item.notes);body.appendChild(tr)}
 }catch(e){const tr=document.createElement('tr');const td=cell(tr,'模板加载失败：'+e.message,'td','empty');td.colSpan=8;body.appendChild(tr);set('dlt-template-count','不可用')}
}
async function discoverSelectedMeter(){
 const device=devices.find(d=>d.id===selectedDevice);
 if(!device){toast('请先选择设备');return}
 if(device.protocol!=='dl-t645-2007'){toast('请选择 DL/T645-2007 电表设备');return}
 const button=$('meter-discover');button.disabled=true;set('meter-discover-result','正在搜索…');
 try{
  const response=await fetch('/api/v1/dlt645/discover',{method:'POST',headers:{'Content-Type':'application/json'},body:JSON.stringify({device_id:device.id})});
  const data=await response.json().catch(()=>({}));
  if(!response.ok)throw Error(data.error||'HTTP '+response.status);
  set('meter-discover-result','发现地址：'+data.address);
  toast('电表地址搜索成功：'+data.address);
 }catch(e){set('meter-discover-result','搜索失败');toast('电表搜索失败：'+e.message)}
 finally{button.disabled=false}
}
$('meter-discover').addEventListener('click',discoverSelectedMeter);
$('dlt-template-search').addEventListener('click',loadDLT645Templates);
$('dlt-template-query').addEventListener('keydown',e=>{if(e.key==='Enter')loadDLT645Templates()});
$('refresh').addEventListener('click',()=>{refresh();loadDevices();toast('正在刷新数据')});
$('devices-refresh').addEventListener('click',loadDevices);
$('device-search').addEventListener('input',e=>{deviceQuery=e.target.value.trim();devicePage=1;renderDevices()});
$('device-prev').addEventListener('click',()=>{devicePage=Math.max(1,devicePage-1);renderDevices()});
$('device-next').addEventListener('click',()=>{devicePage++;renderDevices()});
$('history-device').addEventListener('change',async e=>{await selectDevice(e.target.value);});
$('history-point').addEventListener('change',e=>{selectedPoint=e.target.value});
$('history-query').addEventListener('click',queryHistory);
refresh();loadDevices();loadDLT645Templates();setInterval(refresh,5000);setInterval(loadDevices,15000);
