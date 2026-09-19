package com.aitrace.model;
public class LlmInfo {
    private Integer prompt_tokens;
    private Integer completion_tokens;
    private String finish_reason;
    public Integer getPrompt_tokens() { return prompt_tokens; }
    public void setPrompt_tokens(Integer prompt_tokens) { this.prompt_tokens = prompt_tokens; }
    public Integer getCompletion_tokens() { return completion_tokens; }
    public void setCompletion_tokens(Integer completion_tokens) { this.completion_tokens = completion_tokens; }
    public String getFinish_reason() { return finish_reason; }
    public void setFinish_reason(String finish_reason) { this.finish_reason = finish_reason; }
}
