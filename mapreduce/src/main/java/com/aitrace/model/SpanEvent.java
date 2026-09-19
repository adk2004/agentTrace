package com.aitrace.model;
public class SpanEvent {
    private String trace_id;
    private String session_id;
    private String run_id;
    private String span_id;
    private String parent_span_id;
    private String event_type;
    private String timestamp;
    private String agent_name;
    private String task_type;
    private String model;
    private String run_status;
    private Integer step_index;
    private Long latency_ms;
    private Integer llm_prompt_tokens;
    private Integer llm_completion_tokens;
    private String llm_finish_reason;
    private String tool_name;
    private String tool_status;
    private String tool_error_type;
    private Integer tool_retry_count;
    private Integer tool_args_size_bytes;
    private String user_tier;
    private String region;
    private String agent_version;
    private Boolean has_negative_latency;
    public String getTrace_id() { return trace_id; }
    public void setTrace_id(String trace_id) { this.trace_id = trace_id; }
    public String getSession_id() { return session_id; }
    public void setSession_id(String session_id) { this.session_id = session_id; }
    public String getRun_id() { return run_id; }
    public void setRun_id(String run_id) { this.run_id = run_id; }
    public String getSpan_id() { return span_id; }
    public void setSpan_id(String span_id) { this.span_id = span_id; }
    public String getParent_span_id() { return parent_span_id; }
    public void setParent_span_id(String parent_span_id) { this.parent_span_id = parent_span_id; }
    public String getEvent_type() { return event_type; }
    public void setEvent_type(String event_type) { this.event_type = event_type; }
    public String getTimestamp() { return timestamp; }
    public void setTimestamp(String timestamp) { this.timestamp = timestamp; }
    public String getAgent_name() { return agent_name; }
    public void setAgent_name(String agent_name) { this.agent_name = agent_name; }
    public String getTask_type() { return task_type; }
    public void setTask_type(String task_type) { this.task_type = task_type; }
    public String getModel() { return model; }
    public void setModel(String model) { this.model = model; }
    public String getRun_status() { return run_status; }
    public void setRun_status(String run_status) { this.run_status = run_status; }
    public Integer getStep_index() { return step_index; }
    public void setStep_index(Integer step_index) { this.step_index = step_index; }
    public Long getLatency_ms() { return latency_ms; }
    public void setLatency_ms(Long latency_ms) { this.latency_ms = latency_ms; }
    public Integer getLlm_prompt_tokens() { return llm_prompt_tokens; }
    public void setLlm_prompt_tokens(Integer llm_prompt_tokens) { this.llm_prompt_tokens = llm_prompt_tokens; }
    public Integer getLlm_completion_tokens() { return llm_completion_tokens; }
    public void setLlm_completion_tokens(Integer llm_completion_tokens) { this.llm_completion_tokens = llm_completion_tokens; }
    public String getLlm_finish_reason() { return llm_finish_reason; }
    public void setLlm_finish_reason(String llm_finish_reason) { this.llm_finish_reason = llm_finish_reason; }
    public String getTool_name() { return tool_name; }
    public void setTool_name(String tool_name) { this.tool_name = tool_name; }
    public String getTool_status() { return tool_status; }
    public void setTool_status(String tool_status) { this.tool_status = tool_status; }
    public String getTool_error_type() { return tool_error_type; }
    public void setTool_error_type(String tool_error_type) { this.tool_error_type = tool_error_type; }
    public Integer getTool_retry_count() { return tool_retry_count; }
    public void setTool_retry_count(Integer tool_retry_count) { this.tool_retry_count = tool_retry_count; }
    public Integer getTool_args_size_bytes() { return tool_args_size_bytes; }
    public void setTool_args_size_bytes(Integer tool_args_size_bytes) { this.tool_args_size_bytes = tool_args_size_bytes; }
    public String getUser_tier() { return user_tier; }
    public void setUser_tier(String user_tier) { this.user_tier = user_tier; }
    public String getRegion() { return region; }
    public void setRegion(String region) { this.region = region; }
    public String getAgent_version() { return agent_version; }
    public void setAgent_version(String agent_version) { this.agent_version = agent_version; }
    public Boolean getHas_negative_latency() { return has_negative_latency; }
    public void setHas_negative_latency(Boolean has_negative_latency) { this.has_negative_latency = has_negative_latency; }
}
