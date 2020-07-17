
package com.exam.eventmetrics.pojoentites;

import com.fasterxml.jackson.annotation.*;

import java.io.Serializable;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

@JsonInclude(JsonInclude.Include.NON_NULL)
@JsonPropertyOrder({
    "push_id",
    "size",
    "distinct_size",
    "ref",
    "head",
    "before",
    "commits"
})
public class Payload implements Serializable {

    @JsonProperty("push_id")
    private Long pushId;
    @JsonProperty("size")
    private Integer size;
    @JsonProperty("distinct_size")
    private Integer distinctSize;
    @JsonProperty("ref")
    private String ref;
    @JsonProperty("head")
    private String head;
    @JsonProperty("before")
    private String before;
    @JsonProperty("commits")
    private List<Commit> commits = null;
    @JsonIgnore
    private Map<String, Object> additionalProperties = new HashMap<String, Object>();

    @JsonProperty("push_id")
    public Long getPushId() {
        return pushId;
    }

    @JsonProperty("push_id")
    public void setPushId(Long pushId) {
        this.pushId = pushId;
    }

    @JsonProperty("size")
    public Integer getSize() {
        return size;
    }

    @JsonProperty("size")
    public void setSize(Integer size) {
        this.size = size;
    }

    @JsonProperty("distinct_size")
    public Integer getDistinctSize() {
        return distinctSize;
    }

    @JsonProperty("distinct_size")
    public void setDistinctSize(Integer distinctSize) {
        this.distinctSize = distinctSize;
    }

    @JsonProperty("ref")
    public String getRef() {
        return ref;
    }

    @JsonProperty("ref")
    public void setRef(String ref) {
        this.ref = ref;
    }

    @JsonProperty("head")
    public String getHead() {
        return head;
    }

    @JsonProperty("head")
    public void setHead(String head) {
        this.head = head;
    }

    @JsonProperty("before")
    public String getBefore() {
        return before;
    }

    @JsonProperty("before")
    public void setBefore(String before) {
        this.before = before;
    }

    @JsonProperty("commits")
    public List<Commit> getCommits() {
        return commits;
    }

    @JsonProperty("commits")
    public void setCommits(List<Commit> commits) {
        this.commits = commits;
    }

    @JsonAnyGetter
    public Map<String, Object> getAdditionalProperties() {
        return this.additionalProperties;
    }

    @JsonAnySetter
    public void setAdditionalProperty(String name, Object value) {
        this.additionalProperties.put(name, value);
    }

}
