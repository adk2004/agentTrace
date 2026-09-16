#!/usr/bin/env python3
"""generate_traces.py - Synthetic AI agent execution trace generator.
Streams data line-by-line; RAM < 300 MB even for 1M runs.
Uses stdlib + numpy (random sampling only, no ML).
"""
import argparse, json, os, sys, time, uuid
from datetime import datetime, timezone, timedelta
import numpy as np

TASK_TYPES=['coding','web_research','data_analysis','customer_support','summarization']
MODELS=['gpt-4o','claude-sonnet','llama-3-70b','mistral-large','gemini-pro']
STATUSES=['success','failed','timeout']
STEP_TYPES=['llm_call','tool_call','retrieval']
FINISH_REASONS=['stop','length','tool_calls']
TOOL_NAMES=['web_search','code_exec','sql_query','file_read','http_get','calculator']
ERROR_TYPES=['rate_limit','timeout','invalid_args','network_error','parse_error']
USER_TIERS=['free','pro']
REGIONS=['us-east-1','us-west-2','eu-west-1','ap-southeast-1','ap-northeast-1']
AGENT_NAMES=['ResearchAgent-v1','ResearchAgent-v2','CodingAssistant-v3',
            'SupportBot-v1','AnalyticsAgent-v2','SummarizationAgent-v1']
AGENT_VERSIONS=['1.0.0','1.2.1','2.0.0','2.1.0','2.3.4','3.0.0-beta']

TOOL_FAIL_PROB={'web_search':0.08,'code_exec':0.15,'http_get':0.12,'calculator':0.01,'sql_query':0.06,'file_read':0.04}
TASK_TOOL_FAIL_MULT={'coding':{'code_exec':1.4,'sql_query':1.2},'web_research':{'web_search':1.3,'http_get':1.2},
    'data_analysis':{'sql_query':1.5,'file_read':1.3},'customer_support':{'http_get':1.1},'summarization':{'file_read':1.2}}

MODEL_LLM_LATENCY={'gpt-4o':(7.0,0.60),'claude-sonnet':(6.8,0.60),'llama-3-70b':(7.5,0.70),'mistral-large':(7.2,0.65),'gemini-pro':(6.9,0.60)}
TOOL_LATENCY={'web_search':(8.2,0.90),'code_exec':(8.8,1.00),'sql_query':(7.0,0.70),'file_read':(5.5,0.50),'http_get':(7.8,0.85),'calculator':(4.0,0.40)}
RETRIEVAL_LATENCY=(5.8,0.50)
_EPOCH_START=datetime(2026,1,1,tzinfo=timezone.utc)
_WINDOW_SECS=30*86400

def new_uuid(): return str(uuid.uuid4())
def lognormal_ms(mu,sigma,rng): return max(0,int(rng.lognormal(mu,sigma)))
def iso_utc(dt): return dt.strftime("%Y-%m-%dT%H:%M:%S.")+"{:03d}Z".format(dt.microsecond//1000)
def prompt_tokens_for_step(base,idx,rng): return min(int(base*(1+idx*rng.uniform(0.15,0.35))),128000)
def completion_tokens(rng): return max(1,int(rng.lognormal(4.8,0.8)))

def make_step(idx,step_type,model,task_type,ts,base_prompt,rng):
    step={"step_index":idx,"type":step_type,"timestamp":iso_utc(ts),"latency_ms":0}
    if step_type in ("llm_call","retrieval"):
        mu,sigma=RETRIEVAL_LATENCY if step_type=="retrieval" else MODEL_LLM_LATENCY[model]
        step["latency_ms"]=lognormal_ms(mu,sigma,rng)
        step["llm"]={"prompt_tokens":prompt_tokens_for_step(base_prompt,idx,rng),
            "completion_tokens":completion_tokens(rng),
            "finish_reason":rng.choice(FINISH_REASONS,p=[0.55,0.15,0.30]).item()}
    else:
        tool=rng.choice(TOOL_NAMES).item()
        mu,sigma=TOOL_LATENCY[tool]
        step["latency_ms"]=lognormal_ms(mu,sigma,rng)
        base_fail=TOOL_FAIL_PROB[tool]
        mult=TASK_TOOL_FAIL_MULT.get(task_type,{}).get(tool,1.0)
        failed=rng.random()<min(base_fail*mult,0.99)
        if failed:
            t_st=rng.choice(["error","timeout"],p=[0.75,0.25]).item()
            err=rng.choice(ERROR_TYPES).item()
            retry=int(rng.integers(1,4))
        else:
            t_st,err,retry="ok",None,0
        step["tool"]={"name":tool,"status":t_st,"error_type":err,
                      "retry_count":retry,"args_size_bytes":int(rng.integers(64,2048))}
    return step,step["latency_ms"]

def _random_malformed(rng):
    k=int(rng.integers(0,5))
    if k==0: return ""
    if k==1: return "{\"trace_id\":\""+new_uuid()+"\",\"steps\":[{\"step_index\":0"
    if k==2: return "{\"trace_id\":"+str(int(rng.integers(1000,9999)))+",\"steps\":\"bad\"}"
    if k==3: return "{\"agent_name\":\"SomeBot\"}"
    return "{invalid json}"

def make_run(rng,malformed_pct):
    if rng.random()*100<malformed_pct:
        return {"__malformed__":True,"line":_random_malformed(rng)}
    task_type=rng.choice(TASK_TYPES).item()
    model=rng.choice(MODELS).item()
    agent_name=rng.choice(AGENT_NAMES).item()
    status=rng.choice(STATUSES,p=[0.78,0.14,0.08]).item()
    start_dt=_EPOCH_START+timedelta(seconds=float(rng.uniform(0,_WINDOW_SECS)))
    n_steps=max(min(int(rng.geometric(p=0.115))+1,40),2)
    base_prompt=int(rng.integers(256,4096))
    steps,cur_ts=[],start_dt
    for idx in range(n_steps):
        stype=rng.choice(STEP_TYPES,p=[0.50,0.35,0.15]).item()
        step,lat=make_step(idx,stype,model,task_type,cur_ts,base_prompt,rng)
        steps.append(step)
        cur_ts+=timedelta(milliseconds=lat+int(rng.integers(10,200)))
    end_dt=cur_ts
    if rng.random()<0.03: meta=None
    else:
        meta={"user_tier":rng.choice(USER_TIERS).item() if rng.random()>0.03 else None,
              "region":rng.choice(REGIONS).item() if rng.random()>0.03 else None,
              "agent_version":rng.choice(AGENT_VERSIONS).item() if rng.random()>0.03 else None}
    rec={"trace_id":new_uuid(),"session_id":new_uuid(),"run_id":new_uuid(),
         "agent_name":agent_name,"task_type":task_type,"model":model,
         "start_time":iso_utc(start_dt),"end_time":iso_utc(end_dt),
         "status":status,"steps":steps}
    if meta is not None: rec["metadata"]=meta
    if rng.random()<0.03:
        for s in rec["steps"]:
            if "llm" in s: del s["llm"]; break
            if "tool" in s: del s["tool"]; break
    return rec

def run_to_spans(rec):
    meta=rec.get("metadata") or {}
    spans,prev=[],None
    for step in rec.get("steps",[]):
        sid=new_uuid()
        llm=step.get("llm") or {}
        tool=step.get("tool") or {}
        span={"trace_id":rec.get("trace_id"),"session_id":rec.get("session_id"),
              "run_id":rec.get("run_id"),"span_id":sid,"parent_span_id":prev,
              "event_type":step.get("type"),"timestamp":step.get("timestamp"),
              "agent_name":rec.get("agent_name"),"task_type":rec.get("task_type"),
              "model":rec.get("model"),"run_status":rec.get("status"),
              "step_index":step.get("step_index"),"latency_ms":step.get("latency_ms"),
              "llm_prompt_tokens":llm.get("prompt_tokens"),
              "llm_completion_tokens":llm.get("completion_tokens"),
              "llm_finish_reason":llm.get("finish_reason"),
              "tool_name":tool.get("name"),"tool_status":tool.get("status"),
              "tool_error_type":tool.get("error_type"),
              "tool_retry_count":tool.get("retry_count"),
              "tool_args_size_bytes":tool.get("args_size_bytes"),
              "user_tier":meta.get("user_tier"),"region":meta.get("region"),
              "agent_version":meta.get("agent_version")}
        spans.append(span); prev=sid
    return spans

def open_writers(d,prefix,n):
    os.makedirs(d,exist_ok=True)
    return [open(os.path.join(d,"{}_part_{:05d}.jsonl".format(prefix,i)),
                 "w",buffering=65536,encoding="utf-8") for i in range(n)]

def close_writers(ws):
    for w in ws: w.close()

def _worker_task(w_id, runs, file_indices, out_dir, do_n, do_f, seed, malformed_pct, return_dict):
    rng = np.random.default_rng(seed)
    nd = os.path.join(out_dir, 'nested') if do_n else None
    fd = os.path.join(out_dir, 'flat') if do_f else None
    
    nw_map = {idx: open(os.path.join(nd, f'nested_part_{idx:05d}.jsonl'), 'w', buffering=65536, encoding='utf-8') for idx in file_indices} if do_n else {}
    fw_map = {idx: open(os.path.join(fd, f'flat_part_{idx:05d}.jsonl'), 'w', buffering=65536, encoding='utf-8') for idx in file_indices} if do_f else {}
    
    runs_cnt = bad_cnt = steps_cnt = 0
    num_sub = len(file_indices)
    
    for i in range(runs):
        rec = make_run(rng, malformed_pct)
        target_f = file_indices[i % num_sub]
        if rec.get('__malformed__'):
            bad_cnt += 1
            line = rec['line'] + chr(10)
            if nw_map: nw_map[target_f].write(line)
            if fw_map: fw_map[target_f].write(line)
            continue
        runs_cnt += 1
        ns = len(rec.get('steps', []))
        steps_cnt += ns
        if nw_map: nw_map[target_f].write(json.dumps(rec, ensure_ascii=False) + chr(10))
        if fw_map:
            for j, sp in enumerate(run_to_spans(rec)):
                sp_f = file_indices[(i + j) % num_sub]
                fw_map[sp_f].write(json.dumps(sp, ensure_ascii=False) + chr(10))
                
    for w in list(nw_map.values()) + list(fw_map.values()):
        w.close()
    return_dict[w_id] = (runs_cnt, bad_cnt, steps_cnt)

def generate(args):
    import multiprocessing as mp
    num_workers = getattr(args, 'workers', 1)
    if num_workers > 1 and args.files >= num_workers and args.runs >= 10000:
        do_n = args.format in ('nested', 'both')
        do_f = args.format in ('flat', 'both')
        nd = os.path.join(args.out, 'nested') if do_n else None
        fd = os.path.join(args.out, 'flat') if do_f else None
        if nd: os.makedirs(nd, exist_ok=True)
        if fd: os.makedirs(fd, exist_ok=True)
        
        t0 = time.time()
        manager = mp.Manager()
        ret_dict = manager.dict()
        procs = []
        files_per_worker = args.files // num_workers
        runs_per_worker = args.runs // num_workers
        remainder_runs = args.runs % num_workers
        
        for w in range(num_workers):
            f_start = w * files_per_worker
            f_end = (w + 1) * files_per_worker if w < num_workers - 1 else args.files
            w_files = list(range(f_start, f_end))
            w_runs = runs_per_worker + (remainder_runs if w == num_workers - 1 else 0)
            w_seed = args.seed + w * 10007
            p = mp.Process(target=_worker_task, args=(w, w_runs, w_files, args.out, do_n, do_f, w_seed, args.malformed_pct, ret_dict))
            p.start()
            procs.append(p)
            
        for p in procs:
            p.join()
            
        el = time.time() - t0
        runs = sum(v[0] for v in ret_dict.values())
        bad = sum(v[1] for v in ret_dict.values())
        steps = sum(v[2] for v in ret_dict.values())
        print(chr(10) + '=== Generation complete in {:.1f}s (parallel: {} workers) ==='.format(el, num_workers))
        print('  Good runs:       {:>12,}'.format(runs))
        print('  Malformed lines: {:>12,}'.format(bad))
        print('  Total steps:     {:>12,}'.format(steps))
        print('  Avg steps/run:   {:>12.2f}'.format(steps / max(runs, 1)))
        for fmt, d in [('nested', nd), ('flat', fd)]:
            if d is None: continue
            fs = sorted(os.listdir(d))
            tb = sum(os.path.getsize(os.path.join(d, fn)) for fn in fs)
            print('  {:<10} {} files  {:.3f} GB ({:,} bytes)'.format(fmt, len(fs), tb / 1073741824, tb))
        return

    rng=np.random.default_rng(args.seed)

    do_n=args.format in ("nested","both")
    do_f=args.format in ("flat","both")
    nd=os.path.join(args.out,"nested") if do_n else None
    fd=os.path.join(args.out,"flat") if do_f else None
    nw=open_writers(nd,"nested",args.files) if do_n else []
    fw=open_writers(fd,"flat",args.files) if do_f else []
    runs=bad=steps=0; t0=time.time()
    for i in range(args.runs):
        rec=make_run(rng,args.malformed_pct)
        if rec.get("__malformed__"):
            bad+=1; line=rec["line"]
            if nw: nw[i%args.files].write(line+chr(10))
            if fw: fw[i%args.files].write(line+chr(10))
            continue
        runs+=1; ns=len(rec.get("steps",[])); steps+=ns; fi=i%args.files
        if nw: nw[fi].write(json.dumps(rec,ensure_ascii=False)+chr(10))
        if fw:
            for j,sp in enumerate(run_to_spans(rec)):
                fw[(fi+j)%args.files].write(json.dumps(sp,ensure_ascii=False)+chr(10))
        if (i+1)%50000==0:
            el=time.time()-t0
            sys.stderr.write("  [{:>8,}/{:>8,}] {:,.0f} runs/s steps={:,}".format(i+1,args.runs,(i+1)/el,steps)+chr(10))
    close_writers(nw); close_writers(fw)
    el=time.time()-t0
    print(chr(10)+"=== Generation complete in {:.1f}s ===".format(el))
    print("  Good runs:       {:>12,}".format(runs))
    print("  Malformed lines: {:>12,}".format(bad))
    print("  Total steps:     {:>12,}".format(steps))
    print("  Avg steps/run:   {:>12.2f}".format(steps/max(runs,1)))
    for fmt,d in [("nested",nd),("flat",fd)]:
        if d is None: continue
        fs=sorted(os.listdir(d))
        tb=sum(os.path.getsize(os.path.join(d,fn)) for fn in fs)
        print("  {:<10} {} files  {:.3f} GB ({:,} bytes)".format(fmt,len(fs),tb/1073741824,tb))

def parse_args():
    p=argparse.ArgumentParser(description="Stream-generate synthetic AI agent traces.")
    p.add_argument("--runs",type=int,default=1000,help="Runs to generate (max 1000000)")
    p.add_argument("--format",choices=["nested","flat","both"],default="both")
    p.add_argument("--out",type=str,default="./traces",help="Output directory")
    p.add_argument("--files",type=int,default=8,help="Output files per format")
    p.add_argument("--seed",type=int,default=42,help="Random seed")
    p.add_argument("--malformed-pct",dest="malformed_pct",type=float,default=1.0,
                   help="Percent of lines to inject as malformed")
    p.add_argument("--workers",type=int,default=4,help="Number of parallel workers (default 4)")
    return p.parse_args()

if __name__=="__main__":
    args=parse_args()
    if args.runs>1000000:
        print("ERROR: --runs must not exceed 1,000,000",file=sys.stderr); sys.exit(1)
    if not 0.0<=args.malformed_pct<=100.0:
        print("ERROR: --malformed-pct must be in [0,100]",file=sys.stderr); sys.exit(1)
    generate(args)

