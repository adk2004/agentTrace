package com.aitrace.model;
import java.util.List;
public class RunRecord {
    private String trace_id;
    private String session_id;
    private String run_id;
    private String agent_name;
    private String task_type;
    private String model;
    private String start_time;
    private String end_time;
    private String status;
    private Metadata metadata;
    private List<StepRecord> steps;
    private Boolean has_negative_latency;
    public String getTrace_id() { return trace_id; }
    public void setTrace_id(String trace_id) { this.trace_id = trace_id; }
    public String getSession_id() { return session_id; }
    public void setSession_id(String session_id) { this.session_id = session_id; }
    public String getRun_id() { return run_id; }
    public void setRun_id(String run_id) { this.run_id = run_id; }
    public String getAgent_name() { return agent_name; }
    public void setAgent_name(String agent_name) { this.agent_name = agent_name; }
    public String getTask_type() { return task_type; }
    public void setTask_type(String task_type) { this.task_type = task_type; }
    public String getModel() { return model; }
    public void setModel(String model) { this.model = model; }
    public String getStart_time() { return start_time; }
    public void setStart_time(String start_time) { this.start_time = start_time; }
    public String getEnd_time() { return end_time; }
    public void setEnd_time(String end_time) { this.end_time = end_time; }
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
    public Metadata getMetadata() { return metadata; }
    public void setMetadata(Metadata metadata) { this.metadata = metadata; }
    public List<StepRecord> getSteps() { return steps; }
    public void setSteps(List<StepRecord> steps) { this.steps = steps; }
    public Boolean getHas_negative_latency() { return has_negative_latency; }
    public void setHas_negative_latency(Boolean has_negative_latency) { this.has_negative_latency = has_negative_latency; }
}
