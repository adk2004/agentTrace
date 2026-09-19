package com.aitrace.model;
public class ToolInfo {
    private String name;
    private String status;
    private String error_type;
    private Integer retry_count;
    private Integer args_size_bytes;
    public String getName() { return name; }
    public void setName(String name) { this.name = name; }
    public String getStatus() { return status; }
    public void setStatus(String status) { this.status = status; }
    public String getError_type() { return error_type; }
    public void setError_type(String error_type) { this.error_type = error_type; }
    public Integer getRetry_count() { return retry_count; }
    public void setRetry_count(Integer retry_count) { this.retry_count = retry_count; }
    public Integer getArgs_size_bytes() { return args_size_bytes; }
    public void setArgs_size_bytes(Integer args_size_bytes) { this.args_size_bytes = args_size_bytes; }
}
