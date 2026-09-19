package com.aitrace.model;
public class StepRecord {
    private Integer step_index;
    private String type;
    private String timestamp;
    private Long latency_ms;
    private LlmInfo llm;
    private ToolInfo tool;
    public Integer getStep_index() { return step_index; }
    public void setStep_index(Integer step_index) { this.step_index = step_index; }
    public String getType() { return type; }
    public void setType(String type) { this.type = type; }
    public String getTimestamp() { return timestamp; }
    public void setTimestamp(String timestamp) { this.timestamp = timestamp; }
    public Long getLatency_ms() { return latency_ms; }
    public void setLatency_ms(Long latency_ms) { this.latency_ms = latency_ms; }
    public LlmInfo getLlm() { return llm; }
    public void setLlm(LlmInfo llm) { this.llm = llm; }
    public ToolInfo getTool() { return tool; }
    public void setTool(ToolInfo tool) { this.tool = tool; }
}
